"""Supabase-backed fast index for Orthanc radiology studies.

The index stores lightweight study metadata and delivery pointers only. Orthanc
remains the source of truth for images and is used to fill new/missing rows.
"""

import json
import logging
import threading
import time
from urllib.parse import quote

import requests

log = logging.getLogger("dicom_export")


def _date_iso(date_str):
    value = str(date_str or "").strip()
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def _json_list(value):
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = []
        return _json_list(parsed)
    return []


def _display_timestamp(value):
    if not value:
        return ""
    return str(value).replace("T", " ").replace("+00:00", "").replace("Z", "")


class RadiologyIndex:
    """Small REST client for public.radiology.

    The service-role key stays in the machine-local worker config. The browser
    never talks to Supabase directly.
    """

    def __init__(self, cfg):
        index_cfg = cfg.get("radiology_index") or {}
        self.enabled = bool(index_cfg.get("enabled"))
        self.base_url = str(index_cfg.get("url") or "").rstrip("/")
        self.table = str(index_cfg.get("table") or "radiology")
        self.timeout = float(index_cfg.get("timeout_seconds") or 15)
        self.max_refresh_age = float(index_cfg.get("refresh_age_seconds") or 120)
        key = str(index_cfg.get("service_role_key") or "").strip()
        self._last_sync = {}
        self._lock = threading.RLock()
        self.session = requests.Session()
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }
        if self.enabled and (not self.base_url or not key):
            log.warning("Radiology index disabled: URL or service-role key is missing")
            self.enabled = False
        if self.enabled:
            log.info("Radiology index enabled: %s.%s", self.base_url, self.table)

    @property
    def endpoint(self):
        return f"{self.base_url}/rest/v1/{quote(self.table, safe='')}"

    def _mark_sync(self, date_str):
        with self._lock:
            self._last_sync[str(date_str)] = time.monotonic()

    def needs_refresh(self, date_str):
        with self._lock:
            last = self._last_sync.get(str(date_str), 0)
        return (time.monotonic() - last) >= self.max_refresh_age

    def list_rows(self, date_str, modality=None):
        if not self.enabled:
            return None
        params = {
            "select": "orthanc_study_id,accession,patient_name,study_description,modality,series_count,instance_count,phone,delivery_status,delivery_attempts,delivery_timestamp,sent_at,pdf_urls,image_urls,orthanc_metadata,source",
            "study_date": f"eq.{_date_iso(date_str)}",
            "order": "accession.desc",
            "limit": "1000",
        }
        if modality:
            params["modality"] = f"eq.{str(modality).upper()}"
        try:
            with self._lock:
                response = self.session.get(self.endpoint, headers=self.headers, params=params, timeout=self.timeout)
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list):
                return []
            return [self._api_row(row) for row in rows]
        except (requests.RequestException, ValueError) as exc:
            log.warning("Radiology index read failed for %s: %s", date_str, exc)
            return None

    def existing_for_date(self, date_str):
        if not self.enabled:
            return {}
        params = {
            "select": "orthanc_study_id,series_count,instance_count,delivery_status,orthanc_metadata",
            "study_date": f"eq.{_date_iso(date_str)}",
            "limit": "1000",
        }
        try:
            with self._lock:
                response = self.session.get(self.endpoint, headers=self.headers, params=params, timeout=self.timeout)
            response.raise_for_status()
            rows = response.json()
            return {str(row.get("orthanc_study_id")): row for row in rows if row.get("orthanc_study_id")}
        except (requests.RequestException, ValueError) as exc:
            log.warning("Radiology index state read failed for %s: %s", date_str, exc)
            return {}

    def upsert_rows(self, rows):
        if not self.enabled or not rows:
            return False
        # orthanc_study_id is the table's logical unique key. Keep this as one
        # request so a day's initial fill is much cheaper than row-by-row REST.
        url = f"{self.endpoint}?on_conflict=orthanc_study_id"
        headers = dict(self.headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        try:
            with self._lock:
                response = self.session.post(url, headers=headers, json=rows, timeout=self.timeout)
            response.raise_for_status()
            return True
        except requests.RequestException as exc:
            log.warning("Radiology index upsert failed for %d row(s): %s", len(rows), exc)
            return False

    def record_from_api_row(self, row, source="orthanc"):
        meta = row.get("orthancMetadata") or {}
        status = str(row.get("status") or meta.get("WhatsappStatus") or "").strip() or None
        try:
            attempts = int(row.get("attempts") or meta.get("WhatsappAttempts") or 0)
        except (TypeError, ValueError):
            attempts = 0
        delivery_timestamp = str(row.get("timestamp") or meta.get("WhatsappTimestamp") or "").strip() or None
        pdf_urls = _json_list(row.get("pdfUrls"))
        image_urls = _json_list(meta.get("WhatsappImageUrls") or meta.get("WhatsappImageUrl"))
        study_date = str(row.get("studyDate") or "").strip()
        study_time = str(row.get("studyTime") or "").strip()
        if len(study_time) > 8:
            study_time = study_time[:8]
        return {
            "orthanc_study_id": row.get("studyId"),
            "study_instance_uid": row.get("studyInstanceUid") or None,
            "accession": row.get("accession") or None,
            "patient_id": row.get("patientId") or None,
            "patient_name": row.get("patientName") or None,
            "patient_sex": row.get("patientSex") or None,
            "study_date": _date_iso(study_date) or None,
            "study_time": study_time or None,
            "study_description": row.get("studyDescription") or None,
            "modality": row.get("modality") or None,
            "series_count": int(row.get("seriesCount") or 0),
            "instance_count": int(row.get("instanceCount") or 0),
            "phone": row.get("phone") or None,
            "delivery_status": status,
            "delivery_attempts": attempts,
            "delivery_timestamp": delivery_timestamp,
            "sent_at": delivery_timestamp if status == "SENT" else None,
            "pdf_urls": pdf_urls,
            "image_urls": image_urls,
            "orthanc_metadata": meta,
            "source": source or "orthanc",
        }

    def _api_row(self, row):
        pdf_urls = _json_list(row.get("pdf_urls"))
        metadata = row.get("orthanc_metadata") or {}
        source = str(row.get("source") or "")
        if source not in ("primary", "backup"):
            source = "primary"
        try:
            attempts = int(row.get("delivery_attempts") or 0)
        except (TypeError, ValueError):
            attempts = 0
        return {
            "studyId": row.get("orthanc_study_id") or "",
            "seriesCount": row.get("series_count") or 0,
            "accession": row.get("accession") or "",
            "studyDescription": row.get("study_description") or "",
            "patientName": row.get("patient_name") or "",
            "patientAge": metadata.get("PatientAge") or "",
            "phone": row.get("phone") or "",
            "status": row.get("delivery_status") or "",
            "attempts": str(attempts),
            "timestamp": _display_timestamp(row.get("delivery_timestamp") or row.get("sent_at")),
            "pdfUrl": pdf_urls[0] if pdf_urls else "",
            "pdfUrls": pdf_urls,
            "modality": row.get("modality") or "",
            "source": source,
        }
