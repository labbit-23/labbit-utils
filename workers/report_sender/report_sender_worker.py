#!/usr/bin/env python3
import argparse
import hashlib
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

INVALID_PHONE_SENTINEL = "INVALID_PHONE"
IST = timezone(timedelta(hours=5, minutes=30))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    return (dt or utc_now()).isoformat()


def parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def parse_status_dt_ist_to_utc(value: Any) -> Optional[datetime]:
    """
    Parse report-status timestamps.
    If timezone is missing, treat it as IST and convert to UTC.
    If timezone exists, normalize to UTC.
    """
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    try:
        if text.endswith("Z"):
            dt = datetime.fromisoformat(text[:-1] + "+00:00")
            return dt.astimezone(timezone.utc)
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=IST).astimezone(timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=IST).astimezone(timezone.utc)
        except Exception:
            continue
    return None


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def norm_text(value: Any) -> str:
    return str(value or "").strip()


def digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def is_valid_india_phone(value: Any) -> bool:
    d = digits_only(value)
    return len(d) == 10 or (len(d) == 12 and d.startswith("91"))


def parse_neo_datetime(value: Any) -> Optional[datetime]:
    text = norm_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=IST).astimezone(timezone.utc)
        except Exception:
            continue
    return parse_status_dt_ist_to_utc(text)


def is_not_collected_test(row: Dict[str, Any]) -> bool:
    joined = " ".join([
        norm_text(row.get("REPORT_STATUS") or row.get("report_status")),
        norm_text(row.get("APPROVEDFLG") or row.get("approvedflg")),
        norm_text(row.get("SAMPLESTATUS") or row.get("samplestatus")),
        norm_text(row.get("RESULTSTATUS") or row.get("resultstatus")),
    ]).upper()
    markers = ("NOT_COLLECTED", "SAMPLE_NOT", "COLLECTION_PENDING", "PENDING_COLLECTION", "NO_SAMPLE")
    return any(m in joined for m in markers)


def is_ready_test(row: Dict[str, Any]) -> bool:
    if is_not_collected_test(row):
        return False
    status = norm_text(row.get("REPORT_STATUS") or row.get("report_status")).upper()
    approved = norm_text(row.get("APPROVEDFLG") or row.get("approvedflg")) == "1"
    return approved or status in {"LAB_READY", "RADIOLOGY_READY"}


def is_same_day_required(row: Dict[str, Any]) -> bool:
    return norm_text(row.get("SAMEDAYREPORT") or row.get("samedayreport")) == "1"


def build_template_report_label(status: Dict[str, Any]) -> str:
    # Reuses bot-side interpretation from lib/neosoft/reportStatusMessage.js, condensed for template {{2}}.
    overall = norm_text(status.get("overall_status")).upper()
    rad_total = int(status.get("radiology_total") or 0)
    rad_ready = int(status.get("radiology_ready") or 0)

    if overall == "FULL_REPORT":
        lab_piece = "complete lab"
    elif overall == "PARTIAL_REPORT":
        lab_piece = "partial lab"
    elif overall in {"LAB_PENDING", "NO_REPORT"}:
        lab_piece = "pending lab"
    elif overall == "NO_LAB_TESTS":
        lab_piece = ""
    else:
        lab_piece = "lab"

    parts = [lab_piece] if lab_piece else []
    if rad_total > 0:
        if rad_ready >= rad_total:
            parts.append("complete radiology")
        elif rad_ready > 0:
            parts.append("partial radiology")
        else:
            parts.append("pending radiology")

    text = " and ".join(parts)
    if len(text) > 120:
        return text[:117].rstrip() + "..."
    return text


def evaluate_same_day_readiness(status: Dict[str, Any]) -> bool:
    tests = status.get("tests") if isinstance(status.get("tests"), list) else []
    required = [t for t in tests if isinstance(t, dict) and is_same_day_required(t)]
    if not required:
        return False

    for row in required:
        if not is_ready_test(row):
            return False
    return True


def has_any_ready_same_day(status: Dict[str, Any]) -> bool:
    tests = status.get("tests") if isinstance(status.get("tests"), list) else []
    required = [t for t in tests if isinstance(t, dict) and is_same_day_required(t)]
    if not required:
        return False
    return any(is_ready_test(row) for row in required)


def has_same_day_required_tests(status: Dict[str, Any]) -> bool:
    tests = status.get("tests") if isinstance(status.get("tests"), list) else []
    required = [t for t in tests if isinstance(t, dict) and is_same_day_required(t)]
    return bool(required)


def is_no_reportable_case(status: Dict[str, Any]) -> bool:
    lab_total = int(status.get("lab_total") or 0)
    rad_total = int(status.get("radiology_total") or 0)
    if lab_total == 0 and rad_total == 0:
        return True

    tests = status.get("tests") if isinstance(status.get("tests"), list) else []
    if not tests:
        return False

    reportable = [t for t in tests if isinstance(t, dict) and is_same_day_required(t)]
    if reportable:
        return False

    overall = norm_text(status.get("overall_status")).upper()
    if overall not in {"NO_REPORT", "NO_LAB_TESTS", "LAB_PENDING"}:
        return False
    return not any(is_ready_test(t) for t in tests if isinstance(t, dict))


def requisition_after_cutoff(status: Dict[str, Any], cutoff_hhmm: int) -> bool:
    if cutoff_hhmm <= 0:
        return False
    dt = parse_neo_datetime(status.get("test_date"))
    if dt is None:
        tests = status.get("tests") if isinstance(status.get("tests"), list) else []
        for row in tests:
            if not isinstance(row, dict):
                continue
            dt = parse_neo_datetime(row.get("REQDT") or row.get("reqdt"))
            if dt is not None:
                break
    if dt is None:
        return False
    local = dt.astimezone()
    hhmm = local.hour * 100 + local.minute
    return hhmm > cutoff_hhmm


def extract_phone_from_status(status: Dict[str, Any]) -> str:
    top = norm_text(status.get("phoneno") or status.get("patient_phone") or status.get("MOBILENO"))
    if top:
        return top
    tests = status.get("tests") if isinstance(status.get("tests"), list) else []
    for row in tests:
        if not isinstance(row, dict):
            continue
        p = norm_text(row.get("PHONENO") or row.get("MOBILENO") or row.get("phoneno") or row.get("mobileno"))
        if p:
            return p
    return ""


def same_day_counts_and_pending(status: Dict[str, Any]) -> Tuple[int, int, List[str]]:
    tests = status.get("tests") if isinstance(status.get("tests"), list) else []
    required = [t for t in tests if isinstance(t, dict) and is_same_day_required(t)]
    total = len(required)
    ready = 0
    pending: List[str] = []
    for row in required:
        if is_ready_test(row):
            ready += 1
        else:
            pending.append(norm_text(row.get("TESTNM") or row.get("testnm") or row.get("test_name")) or "Unnamed test")
    return total, ready, pending


def derive_group_ready_timestamps(status: Dict[str, Any]) -> Tuple[Optional[datetime], Optional[datetime]]:
    tests = status.get("tests") if isinstance(status.get("tests"), list) else []
    required = [t for t in tests if isinstance(t, dict) and is_same_day_required(t)]

    lab_latest = None
    rad_latest = None

    for row in required:
        if not is_ready_test(row):
            continue

        group = norm_text(row.get("GROUPNM") or row.get("groupnm")).upper()
        # Some payloads use SCAN/SCANS/XRAY/CT/MRI labels instead of RADIOLOGY.
        if group in {"SCAN", "SCANS", "XRAY", "X-RAY", "CT", "MRI", "USG", "ULTRASOUND"}:
            group = "RADIOLOGY"
        if not group:
            gid = norm_text(row.get("GROUPID") or row.get("groupid"))
            if gid == "GDEP0001":
                group = "LAB"
            elif gid == "GDEP0002":
                group = "RADIOLOGY"

        approved = parse_status_dt_ist_to_utc(row.get("approved_at") or row.get("APPROVED_AT"))
        if approved is None:
            approved = parse_status_dt_ist_to_utc(status.get("latest_approved_at"))
        if approved is None:
            continue

        if group == "LAB":
            if lab_latest is None or approved > lab_latest:
                lab_latest = approved
        elif group == "RADIOLOGY":
            if rad_latest is None or approved > rad_latest:
                rad_latest = approved

    return lab_latest, rad_latest


class SupabaseRest:
    def __init__(self, url: str, service_role_key: str, timeout_seconds: int = 40) -> None:
        self.base = url.rstrip("/") + "/rest/v1"
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        self.timeout = timeout_seconds
        self.session = requests.Session()

    def select_jobs(self, table: str, limit: int, now_iso: str, offset: int = 0) -> List[Dict[str, Any]]:
        statuses = "(queued,cooling_off,eligible,retrying)"
        url = f"{self.base}/{table}"
        params = {
            "select": "*",
            "status": f"in.{statuses}",
            # Only pull due-now rows.
            # Also include overdue cooling_off by scheduled_at<=now to recover from next_attempt drift.
            "or": f"(force_send_now.eq.true,next_attempt_at.is.null,next_attempt_at.lte.{now_iso},and(status.eq.cooling_off,scheduled_at.lte.{now_iso}))",
            # Priority: manual push first, then older due attempts.
            "order": "force_send_now.desc,next_attempt_at.asc.nullsfirst,updated_at.asc",
            "limit": str(limit),
            "offset": str(max(0, int(offset or 0))),
        }
        r = self.session.get(url, headers=self.headers, params=params, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

    def patch_job(self, table: str, row_id: Any, patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/{table}"
        params = {"id": f"eq.{row_id}", "limit": "1"}
        r = self.session.patch(url, headers=self.headers, params=params, data=json.dumps(patch), timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, list) and rows:
            return rows[0]
        return None

    def insert_event(self, table: str, row: Dict[str, Any]) -> None:
        url = f"{self.base}/{table}"
        r = self.session.post(url, headers=self.headers, data=json.dumps(row), timeout=self.timeout)
        r.raise_for_status()

    def get_latest_event(self, table: str, job_id: Any) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/{table}"
        params = {"select": "*", "job_id": f"eq.{job_id}", "order": "created_at.desc", "limit": "1"}
        r = self.session.get(url, headers=self.headers, params=params, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, list) and rows:
            return rows[0]
        return None

    def claim_job(self, table: str, row_id: Any, worker_token: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/{table}"
        params = {
            "id": f"eq.{row_id}",
            "or": "(status.eq.queued,status.eq.cooling_off,status.eq.eligible,status.eq.retrying)",
            "limit": "1",
        }
        patch = {
            "status": "processing",
            "updated_at": utc_iso(),
        }
        r = self.session.patch(url, headers=self.headers, params=params, data=json.dumps(patch), timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, list) and rows:
            return rows[0]
        return None

    def list_watchdog_candidates(self, table: str, limit: int = 500) -> List[Dict[str, Any]]:
        url = f"{self.base}/{table}"
        params = {
            "select": "*",
            "status": "in.(queued,cooling_off,eligible,retrying)",
            "sent_at": "is.null",
            "order": "updated_at.asc",
            "limit": str(limit),
        }
        r = self.session.get(url, headers=self.headers, params=params, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return rows if isinstance(rows, list) else []

    def list_failed_invalid_phone(self, table: str, limit: int = 100) -> List[Dict[str, Any]]:
        url = f"{self.base}/{table}"
        params = {
            "select": "*",
            "status": "eq.failed",
            "last_error": f"eq.{INVALID_PHONE_SENTINEL}",
            "order": "updated_at.desc",
            "limit": str(limit),
        }
        r = self.session.get(url, headers=self.headers, params=params, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return rows if isinstance(rows, list) else []

    def get_latest_sent_job_for_reqno(self, table: str, reqno: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/{table}"
        params = {
            "select": "id,status,report_label,last_status_snapshot,sent_at,updated_at",
            "reqno": f"eq.{reqno}",
            "status": "eq.sent",
            "order": "sent_at.desc.nullslast,updated_at.desc",
            "limit": "1",
        }
        r = self.session.get(url, headers=self.headers, params=params, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, list) and rows:
            return rows[0]
        return None


class ReportSenderWorker:
    def __init__(self, cfg: Dict[str, Any], dry_run: bool = False) -> None:
        self.cfg = cfg
        worker_cfg = cfg.get("worker", {})
        self.dry_run = dry_run or bool(worker_cfg.get("dry_run", False))
        log_level = str(worker_cfg.get("log_level", "INFO")).upper()
        logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
        self.log = logging.getLogger("report_sender_worker")

        timeout_seconds = int(worker_cfg.get("request_timeout_seconds", 40))
        self.sb = SupabaseRest(
            url=cfg["supabase"]["url"],
            service_role_key=cfg["supabase"]["service_role_key"],
            timeout_seconds=timeout_seconds,
        )
        self.http = requests.Session()
        self.worker_token = f"sender-{int(time.time())}-{random.randint(1000,9999)}"
        self.metrics = {
            "stuck_detected": 0,
            "auto_requeued": 0,
            "failed_timeout": 0,
            "status_reconciled": 0,
        }

    def _job_ctx(self, job: Dict[str, Any], status: Optional[Dict[str, Any]] = None) -> str:
        reqno = norm_text((status or {}).get("reqno") or job.get("reqno"))
        reqid = norm_text((status or {}).get("reqid") or job.get("reqid"))
        phone = norm_text(job.get("phone") or (status or {}).get("patient_phone"))
        return f"job_id={job.get('id')} reqno={reqno or '-'} reqid={reqid or '-'} phone={phone or '-'}"

    def _partial_cutoff_due(self, job: Dict[str, Any]) -> Tuple[bool, Optional[datetime]]:
        worker_cfg = self.cfg.get("worker", {})
        start_hhmm = int(worker_cfg.get("partial_send_cutoff_from_hhmm", 1700))
        end_hhmm = int(worker_cfg.get("partial_send_cutoff_to_hhmm", 1730))
        if end_hhmm < start_hhmm:
            end_hhmm = start_hhmm

        start_minutes = (start_hhmm // 100) * 60 + (start_hhmm % 100)
        end_minutes = (end_hhmm // 100) * 60 + (end_hhmm % 100)

        now_local = datetime.now().astimezone()
        today_local = now_local.date()

        seed_key = f"{norm_text(job.get('lab_id'))}:{norm_text(job.get('reqno'))}:{norm_text(job.get('reqid'))}:{today_local.isoformat()}"
        seed = int(hashlib.sha256(seed_key.encode("utf-8")).hexdigest()[:12], 16)
        rng = random.Random(seed)
        target_minutes = rng.randint(start_minutes, end_minutes)
        target_local = now_local.replace(hour=target_minutes // 60, minute=target_minutes % 60, second=0, microsecond=0)
        return now_local >= target_local, target_local.astimezone(timezone.utc)

    def _event(self, job: Dict[str, Any], event_type: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
        table_events = self.cfg["tables"]["events"]
        row = {
            "job_id": job.get("id"),
            "reqno": job.get("reqno"),
            "reqid": job.get("reqid"),
            "phone": job.get("phone"),
            "event_type": event_type,
            "message": message,
            "payload": payload or {},
            "created_at": utc_iso(),
        }
        if self.dry_run:
            self.log.info("[dry-run] event %s %s", event_type, message)
            return
        self.sb.insert_event(table_events, row)

    def _patch_job(self, job: Dict[str, Any], patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        patch = dict(patch)
        patch["updated_at"] = utc_iso()
        if self.dry_run:
            self.log.info("[dry-run] patch job id=%s %s", job.get("id"), patch)
            return {**job, **patch}
        return self.sb.patch_job(self.cfg["tables"]["jobs"], job.get("id"), patch)

    def _status_from_event(self, event_type: str) -> Optional[str]:
        m = {
            "queued_wait": "queued",
            "cooling_off": "cooling_off",
            "sent": "sent",
            "auto_requeue_stuck": "queued",
            "failed_timeout": "failed",
        }
        return m.get(norm_text(event_type).lower())

    def _reconcile_job_state(self, job: Dict[str, Any]) -> None:
        latest = self.sb.get_latest_event(self.cfg["tables"]["events"], job.get("id"))
        if not latest:
            return
        event_type = norm_text(latest.get("event_type")).lower()
        current = norm_text(job.get("status")).lower()
        expected = self._status_from_event(event_type)
        if event_type == "sent" and current != "sent":
            self._patch_job(job, {"status": "sent"})
            self.metrics["status_reconciled"] += 1
            self.log.info("status_reconciled %s prev=%s new=sent reason=latest_event_sent", self._job_ctx(job), current)
            return
        if expected and current != expected and current != "sent":
            self._patch_job(job, {"status": expected})
            self.metrics["status_reconciled"] += 1
            self.log.info("status_reconciled %s prev=%s new=%s reason=event_%s", self._job_ctx(job), current, expected, event_type)

    def _watchdog_stuck_jobs(self) -> None:
        worker_cfg = self.cfg.get("worker", {})
        queued_wait_hours = int(os.getenv("REPORT_SENDER_STUCK_QUEUED_WAIT_HOURS", worker_cfg.get("stuck_queued_wait_hours", 6)))
        cooling_hours = int(os.getenv("REPORT_SENDER_STUCK_COOLING_OFF_HOURS", worker_cfg.get("stuck_cooling_off_hours", 2)))
        max_requeues = int(os.getenv("REPORT_SENDER_STUCK_MAX_AUTO_REQUEUES", worker_cfg.get("stuck_max_auto_requeues", 3)))
        scan_limit = int(os.getenv("REPORT_SENDER_STUCK_SCAN_LIMIT", worker_cfg.get("stuck_scan_limit", 500)))
        now = utc_now()
        rows = self.sb.list_watchdog_candidates(self.cfg["tables"]["jobs"], limit=scan_limit)
        for job in rows:
            latest = self.sb.get_latest_event(self.cfg["tables"]["events"], job.get("id"))
            if not latest:
                continue
            e_type = norm_text(latest.get("event_type")).lower()
            if e_type not in {"queued_wait", "cooling_off"}:
                continue
            e_time = parse_iso(latest.get("created_at"))
            if e_time is None:
                continue
            age_hours = (now - e_time).total_seconds() / 3600.0
            threshold = queued_wait_hours if e_type == "queued_wait" else cooling_hours
            if age_hours < threshold:
                continue
            self.metrics["stuck_detected"] += 1
            meta = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
            stuck_count = int((meta or {}).get("stuck_requeue_count") or 0)
            if stuck_count >= max_requeues:
                self._patch_job(job, {"status": "failed", "last_error": "failed_timeout_stuck"})
                self._event(job, "failed_timeout", "Watchdog marked terminal failure after stuck wait", {
                    "previous_status": norm_text(job.get("status")),
                    "event_type": e_type,
                    "age_hours": round(age_hours, 2),
                    "retry_count": stuck_count,
                })
                self.metrics["failed_timeout"] += 1
                self.log.warning("failed_timeout job_id=%s reqno=%s prev=%s reason=%s", job.get("id"), norm_text(job.get("reqno")), norm_text(job.get("status")), e_type)
                continue
            new_meta = dict(meta or {})
            new_meta["stuck_requeue_count"] = stuck_count + 1
            self._patch_job(job, {"status": "queued", "next_attempt_at": utc_iso(now), "metadata": new_meta, "last_error": None})
            self._event(job, "auto_requeue_stuck", "Watchdog auto-requeued stuck wait-state job", {
                "previous_status": norm_text(job.get("status")),
                "event_type": e_type,
                "age_hours": round(age_hours, 2),
                "retry_count": stuck_count + 1,
            })
            self.metrics["auto_requeued"] += 1
            self.log.warning("auto_requeued job_id=%s reqno=%s prev=%s reason=%s", job.get("id"), norm_text(job.get("reqno")), norm_text(job.get("status")), e_type)

    def _recover_invalid_phone_jobs(self) -> None:
        rows = self.sb.list_failed_invalid_phone(self.cfg["tables"]["jobs"], limit=100)
        for job in rows:
            phone = norm_text(job.get("phone"))
            # Targeted refresh for this reqno/reqid only, to pick corrected source phone.
            try:
                live = self._fetch_status(job)
                src_phone = extract_phone_from_status(live)
                if src_phone and digits_only(src_phone) != digits_only(phone):
                    self._patch_job(job, {"phone": src_phone})
                    phone = src_phone
                    self._event(job, "phone_refreshed_from_source", "Updated phone from report-status source", {"old_phone": norm_text(job.get("phone")), "new_phone": src_phone})
                    self.log.info("phone-refresh %s old=%s new=%s", self._job_ctx(job), norm_text(job.get("phone")), src_phone)
            except Exception as e:
                self.log.warning("phone-refresh-skip %s err=%s", self._job_ctx(job), e)
            if not is_valid_india_phone(phone):
                continue
            self._patch_job(job, {
                "status": "queued",
                "attempt_count": 0,
                "last_error": None,
                "next_attempt_at": utc_iso(),
            })
            self._event(job, "admin_phone_updated_requeue", "Requeued after valid phone update", {"phone": phone})
            self.log.info("phone-requeue %s", self._job_ctx(job))

    def _fetch_status(self, job: Dict[str, Any]) -> Dict[str, Any]:
        base = self.cfg["labbit_py"]["base_url"].rstrip("/")
        mode = norm_text(self.cfg["labbit_py"].get("status_mode") or "reqno").lower()
        reqno = norm_text(job.get("reqno"))
        reqid = norm_text(job.get("reqid"))

        if mode == "reqid" and reqid:
            url = f"{base}/report-status-reqid/{reqid}"
        elif reqno:
            url = f"{base}/report-status/{reqno}"
        elif reqid:
            url = f"{base}/report-status-reqid/{reqid}"
        else:
            raise ValueError("Job missing reqno/reqid")

        timeout = int(self.cfg.get("worker", {}).get("request_timeout_seconds", 40))
        r = self.http.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected status response: {data}")
        return data

    def _build_report_document_url(self, job: Dict[str, Any], status: Dict[str, Any]) -> str:
        base = norm_text(self.cfg["labbit_py"].get("base_url")).rstrip("/")
        reqid = norm_text(job.get("reqid") or status.get("reqid"))
        reqno = norm_text(job.get("reqno") or status.get("reqno"))
        if not base or not reqid:
            raise ValueError("Missing base_url or reqid for report document URL")
        if reqno:
            return f"{base}/report/{reqid}?reqno={reqno}"
        return f"{base}/report/{reqid}"

    def _send_template(self, job: Dict[str, Any], status: Dict[str, Any], report_label: str) -> Dict[str, Any]:
        wa = self.cfg["whatsapp"]
        reqno = norm_text(job.get("reqno") or status.get("reqno"))
        reqid = norm_text(job.get("reqid") or status.get("reqid"))
        patient_name = norm_text(status.get("patient_name") or job.get("patient_name") or "Patient") or "Patient"
        payload = {
            "lab_id": wa["lab_id"],
            "phone": norm_text(job.get("phone") or status.get("patient_phone")),
            "patient_name": patient_name,
            "report_label": report_label,
            "report_source": "requisition_report",
            "reqno": reqno or None,
            "source_service": norm_text(wa.get("source_service") or "report_sender_worker")
        }

        if not payload["phone"]:
            raise ValueError("Missing phone for send")
        if not is_valid_india_phone(payload["phone"]):
            raise ValueError(INVALID_PHONE_SENTINEL)

        token = wa["internal_send_token"]
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "x-internal-token": token,
        }

        timeout = int(self.cfg.get("worker", {}).get("request_timeout_seconds", 40))
        r = self.http.post(wa["internal_send_url"], headers=headers, data=json.dumps(payload), timeout=timeout)
        if not r.ok:
            raise RuntimeError(f"Send failed: {r.status_code} {r.text[:300]}")
        return r.json() if r.text else {"ok": True}

    def _resolve_schedule(self, job: Dict[str, Any], status: Dict[str, Any]) -> datetime:
        existing = parse_iso(job.get("scheduled_at"))
        if existing:
            return existing

        worker_cfg = self.cfg.get("worker", {})
        cooloff_default = int(worker_cfg.get("cooloff_minutes_default", 30))
        cooloff_lab = int(worker_cfg.get("cooloff_lab_minutes", cooloff_default))
        cooloff_rad = int(worker_cfg.get("cooloff_radiology_minutes", 10))

        # Backward compatibility: if split cooloffs are absent, allow per-job cooloff override.
        job_cooloff = job.get("cooloff_minutes")
        if job_cooloff not in (None, "", 0, "0") and "cooloff_lab_minutes" not in worker_cfg and "cooloff_radiology_minutes" not in worker_cfg:
            j = int(job_cooloff)
            cooloff_lab = j
            cooloff_rad = j

        lab_ready_at, rad_ready_at = derive_group_ready_timestamps(status)
        candidates = []
        if lab_ready_at is not None:
            candidates.append(lab_ready_at + timedelta(minutes=cooloff_lab))
        if rad_ready_at is not None:
            candidates.append(rad_ready_at + timedelta(minutes=cooloff_rad))

        if candidates:
            return max(candidates)

        approved_at = parse_status_dt_ist_to_utc(status.get("latest_approved_at")) or utc_now()
        return approved_at + timedelta(minutes=cooloff_default)

    def process_job(self, job: Dict[str, Any]) -> None:
        if not job.get("id"):
            self.log.warning("Skipping row without id")
            return

        if bool(job.get("is_paused", False)):
            return

        status = self._fetch_status(job)
        report_label = build_template_report_label(status)
        self.log.info("status-check %s overall=%s label=%s", self._job_ctx(job, status), norm_text(status.get("overall_status") or "-"), report_label)
        sameday_total, sameday_ready, not_ready_tests = same_day_counts_and_pending(status)

        has_required = has_same_day_required_tests(status)
        all_ready = evaluate_same_day_readiness(status)
        if is_no_reportable_case(status):
            self._patch_job(job, {
                "status": "skipped",
                "report_label": report_label,
                "last_status_snapshot": status,
                "last_error": None,
                "next_attempt_at": None,
            })
            self._event(job, "skipped_no_reportable_tests", "No reportable lab/radiology tests found", {})
            self.log.info("skip-no-reportable %s", self._job_ctx(job, status))
            return
        if not has_required:
            overall = norm_text(status.get("overall_status")).upper()
            rad_ready = int(status.get("radiology_ready") or 0)
            all_ready = overall == "FULL_REPORT" or rad_ready > 0
        if not all_ready:
            cutoff_due, cutoff_at_utc = self._partial_cutoff_due(job)
            reg_cutoff = int(self.cfg.get("worker", {}).get("partial_same_day_registration_cutoff_hhmm", 0))
            after_reg_cutoff = requisition_after_cutoff(status, reg_cutoff)
            allow_partial = cutoff_due and has_any_ready_same_day(status) and not after_reg_cutoff
            if not allow_partial:
                now = utc_now()
                next_check = now + timedelta(minutes=10)
                if cutoff_at_utc and cutoff_at_utc > now:
                    next_check = min(next_check, cutoff_at_utc)
                cutoff_text = utc_iso(cutoff_at_utc) if cutoff_at_utc else None
                event_payload: Dict[str, Any] = {"label": report_label}
                event_payload["sameday_total"] = sameday_total
                event_payload["sameday_ready"] = sameday_ready
                event_payload["not_ready_tests"] = not_ready_tests[:30]
                if reg_cutoff > 0:
                    event_payload["registration_cutoff_hhmm"] = reg_cutoff
                    event_payload["after_registration_cutoff"] = after_reg_cutoff
                if cutoff_text:
                    event_payload["partial_cutoff_at"] = cutoff_text
                    event_payload["ready_any"] = False
                    event_message = "Waiting for all same-day reports (partial send allowed after cutoff if at least one report is ready)"
                else:
                    event_message = "Waiting for all same-day reports"
                self._patch_job(job, {
                    "status": "queued",
                    "report_label": report_label,
                    "last_status_snapshot": status,
                    "next_attempt_at": utc_iso(next_check),
                })
                prev_status = norm_text(job.get("status")).lower()
                prev_label = norm_text(job.get("report_label"))
                if prev_status != "queued" or prev_label != report_label:
                    self._event(job, "queued_wait", event_message, event_payload)
                return

            self._event(job, "queued_partial_cutoff", "Proceeding with partial send after evening cutoff", {
                "label": report_label,
                "partial_cutoff_at": utc_iso(cutoff_at_utc) if cutoff_at_utc else None,
            })

        scheduled_at = self._resolve_schedule(job, status)
        force_now = bool(job.get("force_send_now", False))
        now = utc_now()

        if not force_now and now < scheduled_at:
            prev_scheduled_at = parse_iso(job.get("scheduled_at"))
            self._patch_job(job, {
                "status": "cooling_off",
                "report_label": report_label,
                "scheduled_at": utc_iso(scheduled_at),
                "last_status_snapshot": status,
                "next_attempt_at": utc_iso(min(scheduled_at, now + timedelta(minutes=10))),
            })
            self.log.info("cooling-off %s scheduled_at=%s label=%s", self._job_ctx(job, status), utc_iso(scheduled_at), report_label)
            prev_status = norm_text(job.get("status")).lower()
            prev_label = norm_text(job.get("report_label"))
            schedule_changed = prev_scheduled_at is None or abs((scheduled_at - prev_scheduled_at).total_seconds()) >= 60
            if prev_status != "cooling_off" or prev_label != report_label or schedule_changed:
                self._event(job, "cooling_off", "Waiting for cooloff window", {"label": report_label, "scheduled_at": utc_iso(scheduled_at)})
            return

        attempts = int(job.get("attempt_count") or 0)

        # Duplicate-send guard: do not resend when same-day ready count has not increased.
        latest_sent = self.sb.get_latest_sent_job_for_reqno(self.cfg["tables"]["jobs"], norm_text(job.get("reqno")))
        if latest_sent:
            prev_snap = latest_sent.get("last_status_snapshot") if isinstance(latest_sent.get("last_status_snapshot"), dict) else {}
            prev_total, prev_ready, _ = same_day_counts_and_pending(prev_snap if isinstance(prev_snap, dict) else {})
            if sameday_total > 0 and sameday_ready <= prev_ready and sameday_total == prev_total:
                self._patch_job(job, {
                    "status": "queued",
                    "report_label": report_label,
                    "last_status_snapshot": status,
                    "next_attempt_at": utc_iso(utc_now() + timedelta(minutes=20)),
                    "last_error": None,
                })
                self._event(job, "queued_wait", "Skipped duplicate send: same-day ready count unchanged", {
                    "label": report_label,
                    "sameday_total": sameday_total,
                    "sameday_ready": sameday_ready,
                    "prev_sameday_ready": prev_ready,
                    "not_ready_tests": not_ready_tests[:30],
                })
                return

        self._patch_job(job, {
            "status": "sending",
            "report_label": report_label,
            "last_status_snapshot": status,
            "force_send_now": False,
            "attempt_count": attempts + 1,
            "last_attempt_at": utc_iso(),
        })

        try:
            report_url = self._build_report_document_url(job, status)
            self.log.info("sending %s label=%s report_url=%s", self._job_ctx(job, status), report_label, report_url)
            response = self._send_template(job, status, report_label)
            self._patch_job(job, {
                "status": "sent",
                "sent_at": utc_iso(),
                "last_error": None,
                "provider_response": response,
            })
            provider_id = norm_text((response or {}).get("provider_message_id") or (response or {}).get("id") or ((response or {}).get("messages") or [{}])[0].get("id") if isinstance((response or {}).get("messages"), list) and (response or {}).get("messages") else "")
            self.log.info("sent %s label=%s provider_message_id=%s", self._job_ctx(job, status), report_label, provider_id or "-")
            self._event(job, "sent", "Template sent successfully", {"response": response, "label": report_label})
        except Exception as exc:
            attempts = attempts + 1
            max_attempts = int(self.cfg.get("worker", {}).get("max_attempts", 5))
            backoffs = self.cfg.get("worker", {}).get("retry_backoff_seconds", [60, 180, 600, 1800, 3600])
            idx = min(max(0, attempts - 1), len(backoffs) - 1)
            delay = int(backoffs[idx])
            err_text = str(exc)
            invalid_phone = INVALID_PHONE_SENTINEL in err_text or "Phone must be 10 digits" in err_text
            terminal = invalid_phone or attempts >= max_attempts
            self._patch_job(job, {
                "status": "failed" if terminal else "retrying",
                "last_error": INVALID_PHONE_SENTINEL if invalid_phone else err_text,
                "next_attempt_at": None if terminal else utc_iso(utc_now() + timedelta(seconds=delay)),
            })
            self.log.error("send-failed %s attempt=%s terminal=%s error=%s", self._job_ctx(job, status), attempts, terminal, str(exc))
            self._event(job, "failed_invalid_phone" if invalid_phone else "send_failed", str(exc), {"attempt": attempts, "terminal": terminal})

    def _within_window(self) -> bool:
        start_hhmm = int(self.cfg.get("worker", {}).get("poll_start_hhmm", 730))
        end_hhmm = int(self.cfg.get("worker", {}).get("poll_end_hhmm", 2130))
        now = datetime.now()
        now_hhmm = now.hour * 100 + now.minute
        return start_hhmm <= now_hhmm <= end_hhmm

    def process_once(self) -> None:
        if not self._within_window():
            self.log.info("Outside sender polling window; skipping cycle")
            return

        self._watchdog_stuck_jobs()
        self._recover_invalid_phone_jobs()

        batch_size = int(self.cfg.get("worker", {}).get("batch_size", 25))
        max_scan_rows = int(self.cfg.get("worker", {}).get("max_scan_rows", max(100, batch_size * 8)))
        now_iso = utc_iso(utc_now())

        rows: List[Dict[str, Any]] = []
        offset = 0
        while len(rows) < max_scan_rows:
            page = self.sb.select_jobs(
                self.cfg["tables"]["jobs"],
                limit=batch_size,
                now_iso=now_iso,
                offset=offset,
            )
            if not page:
                break
            rows.extend(page)
            if len(page) < batch_size:
                break
            offset += batch_size

        if len(rows) > max_scan_rows:
            rows = rows[:max_scan_rows]

        self.log.info("Fetched %s jobs (batch_size=%s max_scan_rows=%s)", len(rows), batch_size, max_scan_rows)
        now = utc_now()
        skipped_future = 0
        for row in rows:
            next_attempt = parse_iso(row.get("next_attempt_at"))
            if next_attempt and next_attempt > now:
                scheduled_at = parse_iso(row.get("scheduled_at"))
                due_overdue_cooling = (
                    norm_text(row.get("status")).lower() == "cooling_off"
                    and scheduled_at is not None
                    and scheduled_at <= now
                )
                if not due_overdue_cooling:
                    skipped_future += 1
                    continue
            try:
                self._reconcile_job_state(row)
                claimed = self.sb.claim_job(self.cfg["tables"]["jobs"], row.get("id"), self.worker_token)
                if not claimed:
                    continue
                self.process_job(claimed)
            except Exception as exc:
                self.log.exception("Job processing error id=%s: %s", row.get("id"), exc)
        if skipped_future:
            self.log.info("Skipped %s future-scheduled jobs in fetched batch", skipped_future)
        self.log.info(
            "metrics stuck_detected=%s auto_requeued=%s failed_timeout=%s status_reconciled=%s",
            self.metrics["stuck_detected"],
            self.metrics["auto_requeued"],
            self.metrics["failed_timeout"],
            self.metrics["status_reconciled"],
        )

    def run_forever(self) -> None:
        poll_seconds = int(self.cfg.get("worker", {}).get("poll_seconds", 20))
        self.log.info("Starting report sender worker poll_seconds=%s dry_run=%s", poll_seconds, self.dry_run)
        while True:
            self.process_once()
            time.sleep(poll_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Report sender worker")
    parser.add_argument("--config", required=True, help="Path to worker config JSON")
    parser.add_argument("--dry-run", action="store_true", help="Run without writes/sends")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()

    cfg = load_json(args.config)
    worker = ReportSenderWorker(cfg, dry_run=args.dry_run)
    if args.once:
        worker.process_once()
        return 0
    worker.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
