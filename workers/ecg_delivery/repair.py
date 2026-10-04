"""Manual Tricog ECG repair/reattach path.

The normal Tricog poller remains in Mirth.  This module is intentionally only
for an operator-selected row from the DEXA ECG console: rebuild the graph PDF,
reattach the plain report to Core, refresh FTP and ledger links, and optionally
send WhatsApp when the operator explicitly requests it.
"""

from __future__ import annotations

import ftplib
import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

log = logging.getLogger("dicom_export.ecg")


def _stage(state, name, status, detail=None):
    state[name] = {"status": status, **({"detail": str(detail)} if detail else {})}


def _download(url, destination, token=None):
    headers = {"User-Agent": "SDRC-ECG-Repair/1.0"}
    if token:
        headers["token"] = str(token)
    response = requests.get(url, headers=headers, timeout=(15, 120))
    response.raise_for_status()
    body = response.content
    if len(body) < 1000 or not body.startswith(b"%PDF"):
        raise RuntimeError("downloaded ECG attachment is not a valid PDF")
    Path(destination).write_bytes(body)


def _merge_graph(original, graph, cfg):
    ecg_cfg = cfg.get("ecg", {})
    background = ecg_cfg.get("background_path", "/var/tmp/orthanc-images/ECG_Graph_Background_v2.png")
    magick = cfg["worker"].get("magick_path", "/usr/local/bin/magick")
    if not os.path.exists(background):
        raise RuntimeError(f"ECG background is missing: {background}")
    command = [
        magick,
        "(", background, ")",
        "(", "-density", "300", f"{original}[0]", "-resize", "3508x2480>",
        "-fuzz", "5%", "-transparent", "white", ")",
        "-gravity", "center", "-composite", graph,
    ]
    subprocess.run(command, check=True, capture_output=True, text=True, timeout=120)
    if not os.path.exists(graph) or os.path.getsize(graph) < 1000:
        raise RuntimeError("ImageMagick produced an empty graph PDF")


def _core_attach(accession, pdf_path, cfg):
    labit = cfg["labit"]
    url = f"{labit['base_url'].rstrip('/')}/machine-api/ecg-attachment/{quote(str(accession), safe='')}/ECG"
    with open(pdf_path, "rb") as handle:
        response = requests.post(
            url,
            headers={"X-Internal-Token": labit["internal_token"]},
            data={"report_ready_at": datetime.now(timezone.utc).isoformat()},
            files={"file": (os.path.basename(pdf_path), handle, "application/pdf")},
            timeout=(15, 120),
        )
    if response.status_code not in (200, 201, 202, 204, 409):
        raise RuntimeError(f"Core attachment HTTP {response.status_code}: {response.text[:300]}")
    return response.status_code


def _ftp_upload(local_paths, folder, cfg):
    ftp_cfg = cfg["ftp"]
    links = []
    ftp = ftplib.FTP()
    ftp.connect(ftp_cfg["host"], int(ftp_cfg.get("port", 21)), timeout=30)
    ftp.login(ftp_cfg["user"], ftp_cfg["password"])
    try:
        ftp.set_pasv(True)
        ftp.cwd(ftp_cfg["base_dir"])
        try:
            ftp.mkd(folder)
        except ftplib.error_perm:
            pass
        ftp.cwd(folder)
        for local_path in local_paths:
            filename = os.path.basename(local_path)
            with open(local_path, "rb") as handle:
                ftp.storbinary(f"STOR {filename}", handle)
            links.append(f"{ftp_cfg['public_base_url'].rstrip('/')}/{folder}/{filename}")
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()
    return links


def _supabase_update(row, links, stages, cfg):
    raw = dict(row.get("raw_json") or {})
    raw["manualDelivery"] = {
        "at": datetime.now(timezone.utc).isoformat(),
        "stages": stages,
    }
    payload = {
        "pdf_url": links[0] if links else row.get("pdf_url"),
        "pdf_url_plain": links[1] if len(links) > 1 else row.get("pdf_url_plain"),
        "raw_json": raw,
    }
    url = f"{cfg['supabase_url'].rstrip('/')}/rest/v1/ecg_studies?tricog_ecg_id=eq.{quote(str(row['tricog_ecg_id']), safe='')}"
    response = requests.patch(
        url,
        headers={
            "apikey": cfg["supabase_service_key"],
            "Authorization": f"Bearer {cfg['supabase_service_key']}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json=payload,
        timeout=(15, 30),
    )
    response.raise_for_status()


def _phone(row, cfg):
    labit = cfg["labit"]
    url = f"{labit['base_url'].rstrip('/')}/api/dispatch-phone/{quote(str(row['accession_no']), safe='')}"
    response = requests.get(url, auth=(labit["dispatch_user"], labit["dispatch_password"]), timeout=(10, 30))
    if response.status_code == 404:
        return str(cfg["whatsapp"]["default_phone"])
    response.raise_for_status()
    raw = str((response.json() or {}).get("phone") or "").replace(" ", "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 10:
        return "91" + digits
    if len(digits) == 12 and digits.startswith("91"):
        return digits
    return str(cfg["whatsapp"]["default_phone"])


def _send_whatsapp(row, public_url, cfg):
    wa = cfg["whatsapp"]
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": _phone(row, cfg),
        "type": "template",
        "template": {
            "name": wa["template_pdf"],
            "language": {"code": "en"},
            "components": [
                {"type": "header", "parameters": [{"type": "document", "document": {"link": public_url, "filename": f"{row['accession_no']}.pdf"}}]},
                {"type": "body", "parameters": [{"type": "text", "text": row.get("patient_name") or ""}]},
            ],
        },
    }
    response = requests.post(
        wa["api_url"],
        headers={"Content-Type": "application/json", "X-API-KEY": wa["api_key"]},
        json=payload,
        timeout=(10, 60),
    )
    response.raise_for_status()
    messages = (response.json() or {}).get("messages") or []
    if not messages or not messages[0].get("id"):
        raise RuntimeError(f"WhatsApp response had no message id: {response.text[:300]}")
    return messages[0]["id"]


def manual_reattach(cfg, row, send_whatsapp=False):
    """Rebuild and reattach one ECG selected by an operator."""
    if not row.get("accession_no") or not row.get("tricog_ecg_id"):
        raise ValueError("ECG row is missing accession_no or tricog_ecg_id")
    if not row.get("diagnosis") or not str(row["diagnosis"]).strip():
        raise ValueError("ECG has no diagnosis; delivery remains gated")

    raw = row.get("raw_json") or {}
    pdf_url = raw.get("pdfUrl") or raw.get("pdf_url")
    token = raw.get("tricogToken") or raw.get("tricog_token")
    if not pdf_url or not token:
        raise ValueError("This row has no Tricog report URL/token; it cannot be rebuilt safely")

    stages = {}
    tmp_root = cfg["worker"].get("tmp_dir", "/var/tmp/orthanc-images")
    workdir = tempfile.mkdtemp(prefix=f"ecg-repair-{row['accession_no']}-", dir=tmp_root)
    original = os.path.join(workdir, f"{row['accession_no']}.pdf")
    graph = os.path.join(workdir, f"{row['accession_no']}_Graph.pdf")
    try:
        _download(pdf_url, original, token)
        _merge_graph(original, graph, cfg)

        try:
            status = _core_attach(row["accession_no"], original, cfg)
            _stage(stages, "core", "ok", f"HTTP {status}")
        except Exception as exc:
            _stage(stages, "core", "error", exc)

        links = _ftp_upload([graph, original], str(row["accession_no"]), cfg)
        _stage(stages, "ftp", "ok", f"{len(links)} files")

        if cfg.get("supabase_url") and cfg.get("supabase_service_key"):
            try:
                _supabase_update(row, links, stages, cfg)
                _stage(stages, "ledger", "ok")
            except Exception as exc:
                _stage(stages, "ledger", "error", exc)
        else:
            # The DEXA API owns the Supabase service client.
            _stage(stages, "ledger", "deferred", "DEXA API will update ledger")

        if send_whatsapp:
            quality_flag = row.get("quality_flag") or (row.get("raw_json") or {}).get("qualityFlag")
            if quality_flag:
                raise ValueError("WhatsApp is blocked for a Tricog quality-flagged ECG")
            message_id = _send_whatsapp(row, links[0], cfg)
            _stage(stages, "whatsapp", "ok", message_id)
        else:
            _stage(stages, "whatsapp", "skipped", "operator did not request resend")

        # Record final WhatsApp result if it was explicitly sent. A second
        # small ledger update avoids losing the message id in the prior patch.
        if send_whatsapp and stages["whatsapp"]["status"] == "ok" and cfg.get("supabase_url") and cfg.get("supabase_service_key"):
            raw = dict(row.get("raw_json") or {})
            raw["manualDelivery"] = {"at": datetime.now(timezone.utc).isoformat(), "links": links, "stages": stages}
            url = f"{cfg['supabase_url'].rstrip('/')}/rest/v1/ecg_studies?tricog_ecg_id=eq.{quote(str(row['tricog_ecg_id']), safe='')}"
            response = requests.patch(
                url,
                headers={"apikey": cfg["supabase_service_key"], "Authorization": f"Bearer {cfg['supabase_service_key']}", "Content-Type": "application/json", "Prefer": "return=minimal"},
                json={"whatsapp_sent_at": datetime.now(timezone.utc).isoformat(), "whatsapp_message_id": stages["whatsapp"]["detail"], "raw_json": raw},
                timeout=(15, 30),
            )
            response.raise_for_status()

        return {"ok": all(value["status"] in ("ok", "skipped", "deferred") for value in stages.values()), "accession": row["accession_no"], "links": links, "stages": stages}
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run one local ECG repair from JSON stdin")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    request = json.loads(input())
    result = manual_reattach(config, request["row"], bool(request.get("send_whatsapp")))
    print(json.dumps(result))
