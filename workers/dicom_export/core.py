"""
Shared plumbing for the DICOM export worker: Orthanc REST access, the Labit
dispatch-phone lookup, FTP upload, and WhatsApp send. Modality-specific
logic (cr.py, later ct.py) is built on top of this, not duplicated into it.
"""

import ftplib
import io
import logging
import logging.handlers
import os
import re
import sys

import requests

log = logging.getLogger("dicom_export")


def setup_logging(log_dir, level="INFO"):
    os.makedirs(log_dir, exist_ok=True)
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    log.addHandler(stream_handler)

    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "dicom_export.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    log.addHandler(file_handler)
    return log


class OrthancClient:
    def __init__(self, base_url, user, password, timeout=20):
        self.base_url = base_url.rstrip("/")
        self.auth = (user, password)
        self.timeout = timeout
        self.session = requests.Session()

    def get(self, path):
        resp = self.session.get(
            self.base_url + path, auth=self.auth, timeout=self.timeout
        )
        resp.raise_for_status()
        return resp

    def get_json(self, path):
        return self.get(path).json()

    def post_json(self, path, payload):
        resp = self.session.post(
            self.base_url + path, auth=self.auth, json=payload, timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()

    def get_metadata(self, resource_id, key, default="", resource_type="studies"):
        """resource_type is "studies" (default, matches every existing
        study-level caller -- WhatsappStatus etc.) or "instances" (used
        for CT's per-instance SelectedForReport)."""
        try:
            resp = self.session.get(
                f"{self.base_url}/{resource_type}/{resource_id}/metadata/{key}",
                auth=self.auth,
                timeout=self.timeout,
            )
            if resp.status_code == 404:
                return default
            resp.raise_for_status()
            return resp.text.strip()
        except requests.RequestException as exc:
            log.warning("get_metadata(%s/%s, %s) failed: %s", resource_type, resource_id, key, exc)
            return default

    def put_metadata(self, resource_id, key, value, dry_run=True, resource_type="studies"):
        if dry_run:
            log.info(
                "[DRY_RUN] would PUT metadata %s/%s/metadata/%s = %r",
                resource_type,
                resource_id,
                key,
                value,
            )
            return
        resp = self.session.put(
            f"{self.base_url}/{resource_type}/{resource_id}/metadata/{key}",
            auth=self.auth,
            data=str(value),
            timeout=self.timeout,
        )
        resp.raise_for_status()

    def delete_metadata(self, resource_id, key, dry_run=True, resource_type="studies"):
        if dry_run:
            log.info("[DRY_RUN] would DELETE metadata %s/%s/metadata/%s", resource_type, resource_id, key)
            return
        resp = self.session.delete(
            f"{self.base_url}/{resource_type}/{resource_id}/metadata/{key}",
            auth=self.auth,
            timeout=self.timeout,
        )
        if resp.status_code not in (200, 404):
            resp.raise_for_status()

    def get_rendered_png(self, instance_id):
        resp = self.get(f"/instances/{instance_id}/frames/0/rendered")
        return resp.content

    def get_preview_png(self, instance_id):
        """Orthanc's own downsized preview -- much cheaper than the full
        rendered frame, used for the CT selection UI's thumbnail grid
        where dozens of images may load on one page."""
        resp = self.get(f"/instances/{instance_id}/preview")
        return resp.content, resp.headers.get("Content-Type", "image/png")

    def get_simplified_tags(self, instance_id):
        return self.get_json(f"/instances/{instance_id}/simplified-tags")

    def find_studies(self, query):
        return self.post_json("/tools/find", {"Level": "Study", "Query": query})

    def get_study(self, study_id):
        return self.get_json(f"/studies/{study_id}")

    def get_series(self, series_id):
        return self.get_json(f"/series/{series_id}")


def normalize_reqno(reqno):
    """
    Labit's reqno lookups are exact-match: case-sensitive (confirmed
    2026-09-16) and whitespace-sensitive (confirmed 2026-09-17 on a real
    stuck CT study whose AccessionNumber tag was "R 202609170059" -- a
    stray embedded space from the imaging console/RIS broke every lookup
    silently, no error, just an empty result each time). Collapse/strip
    all whitespace before uppercasing so a malformed tag still resolves.
    """
    return re.sub(r"\s+", "", str(reqno)).upper()


def fetch_phone_from_labit(reqno, base_url, user, password, timeout=15):
    """
    Labit's /api/dispatch-phone/{reqno} lookup is case-sensitive on reqno
    (confirmed 2026-09-16: lowercase returns phone=null, correct uppercase
    reqno returns the real phone). Always normalize before calling.
    """
    if not reqno:
        raise ValueError("fetch_phone_from_labit: reqno is missing")
    reqno_upper = normalize_reqno(reqno)
    url = f"{base_url.rstrip('/')}/api/dispatch-phone/{reqno_upper}"
    resp = requests.get(
        url,
        auth=(user, password),
        headers={"Accept": "application/json"},
        timeout=timeout,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()
    if not data or str(data.get("reqno", "")) != reqno_upper or not data.get("phone"):
        log.info("Labit dispatch-phone: no phone for reqno=%s (response=%s)", reqno_upper, data)
        return None
    return str(data["phone"])


def push_report_link_to_labit(reqno, urls, base_url, internal_token, report_ready_at=None, notes=None, timeout=15, dry_run=True):
    """
    POST /machine-api/report-link/{reqno} -- link-only, no file upload.
    Labit Core just records each URL as its own requisition_attachment row
    (kind="radiology_images"); it never fetches or inspects the bytes.
    Accepts multiple links per call; a bad link in the batch doesn't drop
    the others. Same reqno case-sensitivity applies as dispatch-phone, so
    uppercase here too.
    """
    if not reqno:
        raise ValueError("push_report_link_to_labit: reqno is missing")
    if not urls:
        raise ValueError("push_report_link_to_labit: urls is empty")

    reqno_upper = normalize_reqno(reqno)
    payload = {"links": [{"url": u} for u in urls]}
    if report_ready_at:
        payload["report_ready_at"] = report_ready_at
    if notes:
        payload["notes"] = notes

    if dry_run:
        log.info("[DRY_RUN] would POST report-link reqno=%s: %s", reqno_upper, payload)
        return {"ok": True, "dry_run": True}

    url = f"{base_url.rstrip('/')}/machine-api/report-link/{reqno_upper}"
    resp = requests.post(
        url,
        headers={"X-Internal-Token": internal_token, "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
    )
    resp.raise_for_status()
    result = resp.json()
    log.info("REPORT_LINK_PUSHED | reqno=%s | result=%s", reqno_upper, result)
    return result


def upload_file_to_ftp(local_path, remote_folder, ftp_cfg, dry_run=True):
    """
    Ports the Mirth channel's uploadFileToFTP(): connect, cd into base dir,
    mkdir+cd into remote_folder (Orthanc study/group UUID), store the file,
    return the public URL. remote_folder matches the existing convention
    (Mirth used the Orthanc studyId as the folder name).
    """
    filename = os.path.basename(local_path)
    public_url = f"{ftp_cfg['public_base_url']}/{remote_folder}/{filename}"

    if dry_run:
        log.info(
            "[DRY_RUN] would FTP upload %s -> %s:%s/%s/%s (public url: %s)",
            local_path,
            ftp_cfg["host"],
            ftp_cfg["base_dir"],
            remote_folder,
            filename,
            public_url,
        )
        return public_url

    ftp = ftplib.FTP()
    ftp.connect(ftp_cfg["host"], ftp_cfg["port"], timeout=30)
    ftp.login(ftp_cfg["user"], ftp_cfg["password"])
    try:
        ftp.set_pasv(True)
        if not _ftp_cwd(ftp, ftp_cfg["base_dir"]):
            raise RuntimeError(f"FTP base directory does not exist: {ftp_cfg['base_dir']}")
        try:
            ftp.mkd(remote_folder)
        except ftplib.error_perm:
            pass  # already exists
        if not _ftp_cwd(ftp, remote_folder):
            raise RuntimeError(f"Could not enter FTP folder: {remote_folder}")
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {filename}", f)
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()

    return public_url


def _ftp_cwd(ftp, path):
    try:
        ftp.cwd(path)
        return True
    except ftplib.error_perm:
        return False


def send_whatsapp_document(phone, patient_name, public_url, filename, wa_cfg, dry_run=True):
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": phone,
        "type": "template",
        "template": {
            "name": wa_cfg["template_pdf"],
            "language": {"code": "en"},
            "components": [
                {
                    "type": "header",
                    "parameters": [
                        {"type": "document", "document": {"link": public_url, "filename": filename}}
                    ],
                },
                {"type": "body", "parameters": [{"type": "text", "text": patient_name}]},
            ],
        },
    }

    if dry_run:
        log.info("[DRY_RUN] would send WhatsApp document to %s: %s", phone, payload)
        return "DRY_RUN_MESSAGE_ID"

    resp = requests.post(
        wa_cfg["api_url"],
        headers={"Content-Type": "application/json", "X-API-KEY": wa_cfg["api_key"]},
        json=payload,
        timeout=30,
    )
    body = resp.text
    log.info(
        "WHATSAPP_SENT | Phone=%s | Template=%s | Response=%s",
        phone,
        wa_cfg["template_pdf"],
        body,
    )
    resp.raise_for_status()
    data = resp.json()
    messages = data.get("messages") or []
    if not messages or not messages[0].get("id"):
        raise RuntimeError(f"WhatsApp send failed: {body}")
    return messages[0]["id"]
