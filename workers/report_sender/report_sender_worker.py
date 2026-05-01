#!/usr/bin/env python3
import argparse
import hashlib
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


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
        return datetime.fromisoformat(text)
    except Exception:
        return None


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def norm_text(value: Any) -> str:
    return str(value or "").strip()


def is_ready_test(row: Dict[str, Any]) -> bool:
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


def derive_group_ready_timestamps(status: Dict[str, Any]) -> Tuple[Optional[datetime], Optional[datetime]]:
    tests = status.get("tests") if isinstance(status.get("tests"), list) else []
    required = [t for t in tests if isinstance(t, dict) and is_same_day_required(t)]

    lab_latest = None
    rad_latest = None

    for row in required:
        if not is_ready_test(row):
            continue

        group = norm_text(row.get("GROUPNM") or row.get("groupnm")).upper()
        if not group:
            gid = norm_text(row.get("GROUPID") or row.get("groupid"))
            if gid == "GDEP0001":
                group = "LAB"
            elif gid == "GDEP0002":
                group = "RADIOLOGY"

        approved = parse_iso(row.get("approved_at") or row.get("APPROVED_AT"))
        if approved is None:
            approved = parse_iso(status.get("latest_approved_at"))
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
    def __init__(self, url: str, service_role_key: str, timeout_seconds: int = 20) -> None:
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
            # Only pull due-now rows to avoid starvation from a fixed LIMIT window.
            "or": f"(force_send_now.eq.true,next_attempt_at.is.null,next_attempt_at.lte.{now_iso})",
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


class ReportSenderWorker:
    def __init__(self, cfg: Dict[str, Any], dry_run: bool = False) -> None:
        self.cfg = cfg
        worker_cfg = cfg.get("worker", {})
        self.dry_run = dry_run or bool(worker_cfg.get("dry_run", False))
        log_level = str(worker_cfg.get("log_level", "INFO")).upper()
        logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
        self.log = logging.getLogger("report_sender_worker")

        timeout_seconds = int(worker_cfg.get("request_timeout_seconds", 20))
        self.sb = SupabaseRest(
            url=cfg["supabase"]["url"],
            service_role_key=cfg["supabase"]["service_role_key"],
            timeout_seconds=timeout_seconds,
        )
        self.http = requests.Session()

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

        timeout = int(self.cfg.get("worker", {}).get("request_timeout_seconds", 20))
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

        token = wa["internal_send_token"]
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "x-internal-token": token,
        }

        timeout = int(self.cfg.get("worker", {}).get("request_timeout_seconds", 20))
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

        approved_at = parse_iso(status.get("latest_approved_at")) or utc_now()
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

        all_ready = evaluate_same_day_readiness(status)
        if not all_ready:
            cutoff_due, cutoff_at_utc = self._partial_cutoff_due(job)
            allow_partial = cutoff_due and has_any_ready_same_day(status)
            if not allow_partial:
                now = utc_now()
                next_check = now + timedelta(minutes=10)
                if cutoff_at_utc and cutoff_at_utc > now:
                    next_check = min(next_check, cutoff_at_utc)
                cutoff_text = utc_iso(cutoff_at_utc) if cutoff_at_utc else None
                event_payload: Dict[str, Any] = {"label": report_label}
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
            self._patch_job(job, {
                "status": "cooling_off",
                "report_label": report_label,
                "scheduled_at": utc_iso(scheduled_at),
                "last_status_snapshot": status,
                "next_attempt_at": utc_iso(min(scheduled_at, now + timedelta(minutes=10))),
            })
            self.log.info("cooling-off %s scheduled_at=%s label=%s", self._job_ctx(job, status), utc_iso(scheduled_at), report_label)
            self._event(job, "cooling_off", "Waiting for cooloff window", {"label": report_label, "scheduled_at": utc_iso(scheduled_at)})
            return

        attempts = int(job.get("attempt_count") or 0)
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
            terminal = attempts >= max_attempts
            self._patch_job(job, {
                "status": "failed" if terminal else "retrying",
                "last_error": str(exc),
                "next_attempt_at": None if terminal else utc_iso(utc_now() + timedelta(seconds=delay)),
            })
            self.log.error("send-failed %s attempt=%s terminal=%s error=%s", self._job_ctx(job, status), attempts, terminal, str(exc))
            self._event(job, "send_failed", str(exc), {"attempt": attempts, "terminal": terminal})

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
                skipped_future += 1
                continue
            try:
                self.process_job(row)
            except Exception as exc:
                self.log.exception("Job processing error id=%s: %s", row.get("id"), exc)
        if skipped_future:
            self.log.info("Skipped %s future-scheduled jobs in fetched batch", skipped_future)

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
