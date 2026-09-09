#!/usr/bin/env python3
import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import time

import requests


def now_ist() -> datetime:
    # Host is expected to run in IST.
    return datetime.now()


def time_hhmm_now() -> int:
    n = now_ist()
    return n.hour * 100 + n.minute


def utc_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def norm(v: Any) -> str:
    return str(v or "").strip()


def shivam_reqid(v: Any) -> str:
    text = norm(v)
    return text.split(":", 1)[1] if text.startswith("archive:") else text


def digits_only(v: Any) -> str:
    return "".join(ch for ch in str(v or "") if ch.isdigit())


def parse_metadata(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


class SupabaseRest:
    def __init__(self, url: str, service_role_key: str, timeout: int = 20) -> None:
        self.base = url.rstrip("/") + "/rest/v1"
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        self.timeout = timeout
        self.http = requests.Session()

    def job_exists(self, table: str, reqno: str) -> bool:
        u = f"{self.base}/{table}"
        p = {"select": "id,status", "reqno": f"eq.{reqno}", "limit": "1", "order": "created_at.desc"}
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return bool(isinstance(rows, list) and rows)

    def latest_job(self, table: str, reqno: str) -> Dict[str, Any]:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,reqno,phone,status,last_error,updated_at,created_at",
            "reqno": f"eq.{reqno}",
            "order": "updated_at.desc,created_at.desc,id.desc",
            "limit": "1",
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, list) and rows:
            return rows[0]
        return {}

    def list_jobs_by_reqno(self, table: str, reqno: str, limit: int = 200) -> List[Dict[str, Any]]:
        u = f"{self.base}/{table}"
        p = {
            # phone + last_error are required by the churn guards
            # (_should_skip_failed_reenqueue, _should_retry_pdf_not_found) --
            # they were silently absent before, which is why the
            # INVALID_PHONE re-enqueue guard never actually fired.
            "select": "id,reqno,phone,status,last_error,metadata,updated_at,created_at",
            "reqno": f"eq.{reqno}",
            "order": "updated_at.desc,created_at.desc,id.desc",
            "limit": str(limit),
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return rows if isinstance(rows, list) else []

    def has_active_job(self, table: str, reqno: str) -> bool:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,status",
            "reqno": f"eq.{reqno}",
            "status": "in.(queued,cooling_off,eligible,retrying,sending)",
            "limit": "1"
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return bool(isinstance(rows, list) and rows)

    def has_sent_full(self, table: str, reqno: str) -> bool:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,report_label",
            "reqno": f"eq.{reqno}",
            "status": "eq.sent",
            "report_label": "ilike.*complete*",
            "limit": "1"
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return bool(isinstance(rows, list) and rows)

    def has_sent_non_full(self, table: str, reqno: str) -> bool:
        """True if a 'sent' job exists whose label wasn't a complete send -- covers
        both 'partial' (some tests ready) and 'pending' (overall_status was
        LAB_PENDING/NO_REPORT at send time, e.g. a same-day-required test forced
        an early send) labels, either of which is a valid follow-up candidate."""
        u = f"{self.base}/{table}"
        p = {
            "select": "id,report_label",
            "reqno": f"eq.{reqno}",
            "status": "eq.sent",
            "or": "(report_label.ilike.*partial*,report_label.ilike.*pending*)",
            "limit": "1"
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return bool(isinstance(rows, list) and rows)

    def latest_sent_snapshot(self, table: str, reqno: str) -> Dict[str, Any]:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,last_status_snapshot,sent_at,report_label,updated_at",
            "reqno": f"eq.{reqno}",
            "status": "eq.sent",
            "order": "sent_at.desc.nullslast,updated_at.desc",
            "limit": "1"
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, list) and rows:
            row = rows[0]
            snap = row.get("last_status_snapshot")
            if isinstance(snap, str):
                try:
                    snap = json.loads(snap)
                except Exception:
                    snap = {}
            row["last_status_snapshot"] = snap if isinstance(snap, dict) else {}
            return row
        return {}

    def list_recent_jobs(self, table: str, since_iso: str, limit: int = 500) -> List[Dict[str, Any]]:
        # Reconciliation only cares about jobs actively needing work + partial sends (no complete, no skipped).
        # Filter at database level to avoid fetching thousands of jobs we don't care about.
        u = f"{self.base}/{table}"
        p = {
            "select": "id,lab_id,reqno,reqid,mrno,phone,patient_name,status,report_label,last_error,is_paused,metadata,created_at,updated_at",
            "status": "in.(queued,cooling_off,eligible,retrying,failed,sending,sent)",
            "updated_at": f"gte.{since_iso}",
            "order": "updated_at.asc",
            "limit": str(limit)
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return rows if isinstance(rows, list) else []

    def list_recent_sent_regular_jobs(self, table: str, since_iso: str, limit: int = 500) -> List[Dict[str, Any]]:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,reqno,reqid,mrno,phone,patient_name,last_status_snapshot,metadata",
            "status": "eq.sent",
            "or": f"(created_at.gte.{since_iso},updated_at.gte.{since_iso})",
            "order": "updated_at.desc",
            "limit": str(limit),
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return rows if isinstance(rows, list) else []

    def list_recent_failed_regular_jobs(self, table: str, since_iso: str, limit: int = 200) -> List[Dict[str, Any]]:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,reqno,reqid,mrno,phone,patient_name,metadata",
            "status": "eq.failed",
            "updated_at": f"gte.{since_iso}",
            "order": "updated_at.desc",
            "limit": str(limit),
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return rows if isinstance(rows, list) else []

    def dispatched_exists(self, reqno: str, phone: str) -> bool:
        # report_dispatch_logs lives in labbit-main schema and indicates already sent dispatches.
        u = f"{self.base}/report_dispatch_logs"
        p = {
            "select": "id,status",
            "reqno": f"eq.{reqno}",
            "status": "eq.success",
            "limit": "1"
        }
        if norm(phone):
            p["phone"] = f"eq.{phone}"
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return bool(isinstance(rows, list) and rows)

    def insert_job(self, table: str, row: Dict[str, Any]) -> None:
        u = f"{self.base}/{table}"
        r = self.http.post(u, headers=self.headers, data=json.dumps(row), timeout=self.timeout)
        r.raise_for_status()

    def cancel_job_by_id(self, table: str, job_id: int) -> None:
        u = f"{self.base}/{table}"
        r = self.http.patch(
            u,
            headers=self.headers,
            params={"id": f"eq.{job_id}"},
            data=json.dumps({"status": "cancelled", "updated_at": utc_iso()}),
            timeout=self.timeout,
        )
        r.raise_for_status()

    def defer_job_by_id(self, table: str, job_id: int, hours: int, merged_meta: dict) -> None:
        next_at = (datetime.utcnow() + timedelta(hours=hours)).isoformat() + "Z"
        patch = {
            "status": "cooling_off",
            "next_attempt_at": next_at,
            "cooloff_minutes": hours * 60,
            "metadata": merged_meta,
            "updated_at": utc_iso(),
        }
        u = f"{self.base}/{table}"
        r = self.http.patch(u, headers=self.headers, params={"id": f"eq.{job_id}"},
                            data=json.dumps(patch), timeout=self.timeout)
        r.raise_for_status()

    def fail_job_by_id(self, table: str, job_id: int, last_error: str) -> None:
        u = f"{self.base}/{table}"
        r = self.http.patch(
            u,
            headers=self.headers,
            params={"id": f"eq.{job_id}"},
            data=json.dumps({"status": "failed", "last_error": last_error, "updated_at": utc_iso()}),
            timeout=self.timeout,
        )
        r.raise_for_status()

    def list_deferred_outsourced_jobs(self, table: str, limit: int = 200) -> List[Dict[str, Any]]:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,reqno,metadata,updated_at",
            "status": "eq.cooling_off",
            "metadata->>deferred_reason": "eq.outsourced_split_created",
            "limit": str(limit),
        }
        r = self.http.get(u, headers=self.headers, params=p, timeout=self.timeout)
        r.raise_for_status()
        rows = r.json()
        return rows if isinstance(rows, list) else []


class EnqueueWorker:
    def __init__(self, cfg: Dict[str, Any], dry_run: bool = False) -> None:
        self.cfg = cfg
        self.dry_run = dry_run
        lvl = str(cfg.get("enqueue", {}).get("log_level", "INFO")).upper()
        logging.basicConfig(level=getattr(logging, lvl, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
        self.log = logging.getLogger("enqueue_requisitions_worker")
        timeout = int(cfg.get("enqueue", {}).get("request_timeout_seconds", 20))
        self.sb = SupabaseRest(cfg["supabase"]["url"], cfg["supabase"]["service_role_key"], timeout=timeout)
        self.http = requests.Session()

    def _fetch_status(self, reqno: str, reqid: str) -> Dict[str, Any]:
        base = norm(self.cfg.get("labbit_py", {}).get("base_url")).rstrip("/")
        mode = norm(self.cfg.get("labbit_py", {}).get("status_mode") or "reqno").lower()
        timeout = int(self.cfg.get("enqueue", {}).get("request_timeout_seconds", 20))
        status_reqid = self._status_reqid(reqid)
        if mode == "reqid" and status_reqid:
            url = f"{base}/report-status-reqid/{status_reqid}"
        elif reqno:
            url = f"{base}/report-status/{reqno}"
        elif status_reqid:
            url = f"{base}/report-status-reqid/{status_reqid}"
        else:
            raise ValueError("Missing reqno/reqid for status fetch")
        r = self.http.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict):
            raise ValueError("Unexpected status response")
        return data

    def _strip_archive_reqid_enabled(self) -> bool:
        return bool(self.cfg.get("enqueue", {}).get("strip_archive_reqid_for_status", False))

    def _tag_job_origin_enabled(self) -> bool:
        return bool(self.cfg.get("enqueue", {}).get("tag_job_origin", False))

    def _status_reqid(self, reqid: Any) -> str:
        text = norm(reqid)
        return shivam_reqid(text) if self._strip_archive_reqid_enabled() else text

    def _source_metadata(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not self._tag_job_origin_enabled():
            return {}
        meta = parse_metadata(row.get("metadata"))
        explicit = (
            norm(row.get("source"))
            or norm(row.get("SOURCE"))
            or norm(row.get("source_backend"))
            or norm(row.get("SOURCE_BACKEND"))
            or norm(row.get("report_origin"))
            or norm(row.get("REPORT_ORIGIN"))
            or norm(meta.get("source_backend"))
            or norm(meta.get("report_origin"))
            or norm(self.cfg.get("enqueue", {}).get("source_backend"))
            or norm(self.cfg.get("enqueue", {}).get("report_origin"))
        )
        key = explicit.lower()
        if not key or key in {"shivam", "legacy", "neosoft"}:
            return {}
        if key in {"core", "labit", "labit-core"}:
            key = "labit_core"
        return {"source_backend": key, "report_origin": key}

    def _job_metadata(self, row: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
        meta = self._source_metadata(row)
        meta.update(extra)
        return meta

    def _is_same_day_required(self, row: Dict[str, Any]) -> bool:
        return norm(row.get("SAMEDAYREPORT") or row.get("samedayreport")) == "1"

    def _is_lab_or_radiology_test(self, row: Dict[str, Any]) -> bool:
        group = norm(row.get("GROUPNM") or row.get("groupnm")).upper()
        if group in {"LAB", "RADIOLOGY", "SCAN", "SCANS", "XRAY", "X-RAY", "CT", "MRI", "USG", "ULTRASOUND"}:
            return True
        gid = norm(row.get("GROUPID") or row.get("groupid"))
        return gid in {"GDEP0001", "GDEP0002"}

    def _is_ready_test(self, row: Dict[str, Any]) -> bool:
        status = norm(row.get("REPORT_STATUS") or row.get("report_status")).upper()
        approved = norm(row.get("APPROVEDFLG") or row.get("approvedflg")) == "1"
        return approved or status in {"LAB_READY", "RADIOLOGY_READY"}

    def _has_any_reportable_tests(self, status: Dict[str, Any]) -> bool:
        tests = status.get("tests") if isinstance(status.get("tests"), list) else []
        return any(isinstance(t, dict) and self._is_lab_or_radiology_test(t) for t in tests)

    def _is_outsourced_ready_test(self, row: Dict[str, Any]) -> bool:
        if not isinstance(row, dict):
            return False
        if not self._is_lab_or_radiology_test(row):
            return False
        report_status = norm(row.get("REPORT_STATUS") or row.get("report_status")).upper()
        approved = norm(row.get("APPROVEDFLG") or row.get("approvedflg")) == "1"
        return report_status == "OUTSOURCED" and approved

    def _extract_outsourced_ready_testids(self, status: Dict[str, Any]) -> List[str]:
        tests = status.get("tests") if isinstance(status.get("tests"), list) else []
        out: List[str] = []
        seen = set()
        for row in tests:
            if not self._is_outsourced_ready_test(row):
                continue
            testid = norm(row.get("TESTID") or row.get("testid"))
            if not testid or testid in seen:
                continue
            seen.add(testid)
            out.append(testid)
        return out

    def _is_outsourced_only_reportable(self, status: Dict[str, Any]) -> bool:
        tests = status.get("tests") if isinstance(status.get("tests"), list) else []
        reportable = [t for t in tests if isinstance(t, dict) and self._is_lab_or_radiology_test(t)]
        if not reportable:
            return False
        # Consider outsourced-only when every reportable test is OUTSOURCED.
        for row in reportable:
            report_status = norm(row.get("REPORT_STATUS") or row.get("report_status")).upper()
            if report_status != "OUTSOURCED":
                return False
        return True

    def _outsourced_tests_need_split_job(self, status: Dict[str, Any], reqid: str) -> bool:
        """True if at least one outsourced-ready test on this requisition will
        actually get its own attached-PDF split job from _reconcile_outsourced_jobs
        (mode attached_base/attached_qr). False for transcribed-only results,
        which are typed into the consolidated report and must go through the
        normal follow-up job instead -- otherwise the split-job path silently
        never creates anything for them and the regular path shouldn't skip either."""
        attached_modes = {"attached_base", "attached_qr"}
        for testid in self._extract_outsourced_ready_testids(status):
            meta = self._fetch_outsourced_meta(reqid=reqid, testid=testid)
            mode = norm(meta.get("outsourced_mode") or meta.get("mode")).lower()
            if mode in attached_modes:
                return True
        return False

    def _has_outsourced_job(self, jobs_table: str, reqno: str, testid: str, statuses: set[str]) -> bool:
        rows = self.sb.list_jobs_by_reqno(jobs_table, reqno=reqno, limit=300)
        wanted = norm(testid).upper()
        for row in rows:
            st = norm(row.get("status")).lower()
            if st not in statuses:
                continue
            meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            src = norm(meta.get("report_source")).lower()
            tid = norm(meta.get("outsourced_testid")).upper()
            if src == "outsourced_report" and tid == wanted:
                return True
        return False

    def _fetch_outsourced_meta(self, reqid: str, testid: str) -> Dict[str, Any]:
        base = norm(self.cfg.get("labbit_py", {}).get("base_url")).rstrip("/")
        reqid = self._status_reqid(reqid)
        if not base or not reqid or not testid:
            return {}
        timeout = int(self.cfg.get("enqueue", {}).get("request_timeout_seconds", 20))
        try:
            r = self.http.get(
                f"{base}/outsourced-report/meta",
                params={"reqid": reqid, "testid": testid},
                timeout=timeout,
            )
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else {}
        except Exception as e:
            self.log.warning("outsourced-meta fetch failed reqid=%s testid=%s err=%s", reqid, testid, e)
            return {}

    def _same_day_full_ready(self, status: Dict[str, Any]) -> bool:
        tests = status.get("tests") if isinstance(status.get("tests"), list) else []
        required = [t for t in tests if isinstance(t, dict) and self._is_same_day_required(t)]
        if not required:
            return False
        return all(self._is_ready_test(row) for row in required)

    def _same_day_ready_counts(self, status: Dict[str, Any]) -> tuple[int, int]:
        tests = status.get("tests") if isinstance(status.get("tests"), list) else []
        required = [t for t in tests if isinstance(t, dict) and self._is_same_day_required(t)]
        total = len(required)
        ready = sum(1 for row in required if self._is_ready_test(row))
        return total, ready

    def _is_overall_full_ready(self, status: Dict[str, Any]) -> bool:
        overall = norm(status.get("overall_status")).upper()
        return overall == "FULL_REPORT"

    def _is_partial_label(self, label: Any) -> bool:
        t = norm(label).lower()
        return "partial" in t

    def _is_full_label(self, label: Any) -> bool:
        t = norm(label).lower()
        return "complete" in t and "partial" not in t

    def _is_valid_phone(self, phone: Any) -> bool:
        """Check if phone is valid Indian format: 10 digits or 91+10 digits."""
        if not phone:
            return False
        digits = digits_only(phone)
        return len(digits) == 10 or (len(digits) == 12 and digits.startswith("91"))

    def _has_recent_followup(self, table: str, reqno: str) -> bool:
        """Check if an active (non-sent) follow-up job already exists for this requisition.
        Prevents duplicate follow-ups from being created every reconciliation cycle."""
        return self.sb.has_active_job(table, reqno)

    # WhatsApp delivery-failure signatures where re-sending the SAME number
    # cannot succeed (recipient has no WhatsApp, blocked the business, window
    # closed). Matched as substrings of last_error, which the delivery-status
    # webhook writes as e.g. "WA_DELIVERY_FAILED: 131026 Message undeliverable".
    _PERMANENT_RECIPIENT_FAILURE_TOKENS = (
        "INVALID_PHONE",
        "131026",          # Message undeliverable
        "131047",          # Re-engagement message required
        "131049",          # Not delivered (healthy-ecosystem limit)
        "131050",          # Recipient stopped receiving from this business
        "UNDELIVERABLE",
    )

    def _is_permanent_recipient_failure(self, last_error: Any) -> bool:
        e = norm(last_error).upper()
        return bool(e) and any(tok in e for tok in self._PERMANENT_RECIPIENT_FAILURE_TOKENS)

    def _should_skip_failed_reenqueue(self, jobs_table: str, reqno: str, incoming_phone: str) -> bool:
        """Churn guard against dead numbers. Stop re-creating a dispatch job for a
        (reqno, phone) when:
          * it has already failed with a PERMANENT recipient error for the SAME
            number (invalid, or WhatsApp says undeliverable/blocked) — retrying
            that exact number never succeeds; or
          * that (reqno, phone) has piled up >= max_failed_reenqueue failed jobs
            of ANY error — something is wrong and hammering it just burns
            provider throughput/reputation (Sep-3: 11 dead numbers, ~40 sends
            each = 439 "failures").
        A genuine phone correction (different last-10 digits) always resets this.
        """
        rows = self.sb.list_jobs_by_reqno(jobs_table, reqno=reqno, limit=300)
        cur_digits = digits_only(incoming_phone)
        if not cur_digits:
            return False
        cur_phone_10 = cur_digits[-10:]
        max_failed = int(self.cfg.get("worker", {}).get("max_failed_reenqueue", 3) or 3)
        same_phone_failures = 0
        for row in rows:
            prev_digits = digits_only(row.get("phone"))
            if not (prev_digits and prev_digits[-10:] == cur_phone_10):
                continue  # different number since — a real correction, allow retry
            if self._is_permanent_recipient_failure(row.get("last_error")):
                return True
            if norm(row.get("status")).lower() == "failed":
                same_phone_failures += 1
        return same_phone_failures >= max_failed

    # Back-compat alias for existing call sites / tests.
    _should_skip_invalid_phone_reenqueue = _should_skip_failed_reenqueue

    def _should_retry_pdf_not_found(self, jobs_table: str, reqno: str, reqid: str) -> bool:
        # PDF not found errors are often transient — check if PDF is available NOW
        # Only retry if PDF exists and hasn't been attempted too many times already
        if not reqid:
            return False

        # Find the last "PDF not found" failure for this reqno
        rows = self.sb.list_jobs_by_reqno(jobs_table, reqno=reqno, limit=100)
        pdf_not_found_jobs = [
            r for r in rows
            if norm(r.get("status")).lower() == "failed"
            and "PDF WAS NOT FOUND" in norm(r.get("last_error")).upper()
        ]

        if not pdf_not_found_jobs:
            return False

        # Check if PDF is actually available now
        try:
            live = self._fetch_status(reqno=reqno, reqid=reqid)
            if live and live.get("report_generated_at"):
                self.log.info("Reconcile PDF now available reqno=%s, will retry", reqno)
                return True
        except Exception as e:
            self.log.debug("Reconcile PDF check failed reqno=%s err=%s", reqno, e)

        return False

    def _reconcile_recent(self, jobs_table: str) -> int:
        lookback_hours = int(self.cfg.get("enqueue", {}).get("lookback_hours", 0) or 0)
        if lookback_hours <= 0:
            return 0

        since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        since_iso = since.isoformat()
        recent = self.sb.list_recent_jobs(jobs_table, since_iso, limit=int(self.cfg.get("enqueue", {}).get("lookback_max_rows", 500)))
        if not recent:
            return 0

        # Filter for reconcilable jobs: unsent/failed/etc + partial sends (exclude complete sends)
        candidates: List[Dict[str, Any]] = []
        for row in recent:
            status = norm(row.get("status")).lower()
            if status in {"queued", "cooling_off", "eligible", "retrying", "failed", "sending", "skipped"}:
                candidates.append(row)
                continue
            # For sent jobs, only include partials (complete sends don't need reconciliation)
            if status == "sent" and self._is_partial_label(row.get("report_label")):
                candidates.append(row)

        added = 0
        lab_id = norm(self.cfg.get("whatsapp", {}).get("lab_id"))
        paused_default = bool(self.cfg.get("enqueue", {}).get("enqueue_paused_default", True))
        cooloff = int(self.cfg.get("worker", {}).get("cooloff_minutes_default", 30))
        max_attempts = int(self.cfg.get("worker", {}).get("max_attempts", 5))

        seen_reqnos = set()
        for row in candidates:
            reqno = norm(row.get("reqno"))
            reqid = norm(row.get("reqid"))
            phone = norm(row.get("phone"))
            if not reqno or not phone or reqno in seen_reqnos:
                continue
            seen_reqnos.add(reqno)

            # If already has active queue job, let sender handle current flow.
            if self.sb.has_active_job(jobs_table, reqno):
                continue
            # Only reconcile if this reqno has sent a non-full report (candidate for follow-up).
            if not self.sb.has_sent_non_full(jobs_table, reqno):
                continue

            # Skip reconciled follow-up when already fully sent before.
            if norm(row.get("status")).lower() == "sent" and self._is_full_label(row.get("report_label")):
                continue

            # Early exit: if job was already FULL_REPORT when sent, no point reconciling it.
            # (It was complete then, still complete now—no improvement possible.)
            prev_snap = row.get("last_status_snapshot") if isinstance(row.get("last_status_snapshot"), dict) else {}
            prev_overall = norm(prev_snap.get("overall_status")).upper()
            if prev_overall == "FULL_REPORT":
                self.log.debug("Reconcile skip reqno=%s reason=already_full_at_send", reqno)
                continue

            try:
                live = self._fetch_status(reqno=reqno, reqid=reqid)
            except Exception as e:
                self.log.warning("Reconcile status fetch failed reqno=%s err=%s", reqno, e)
                continue

            latest_status = norm(row.get("status")).lower()
            has_reportable = self._has_any_reportable_tests(live)

            # For previously skipped rows: if reportable tests exist, reactivate into queued.
            # If still no reportable tests, keep skipped.
            if latest_status == "skipped":
                if not has_reportable:
                    self.log.info("Reconcile keep-skipped reqno=%s reason=no_reportable_tests", reqno)
                    continue
                if self._should_skip_invalid_phone_reenqueue(jobs_table, reqno, phone):
                    self.log.info(
                        "Reconcile skip reqno=%s reason=failed_reenqueue_capped phone=%s",
                        reqno,
                        phone,
                    )
                    continue
                new_job = {
                    "lab_id": lab_id,
                    "reqno": reqno,
                    "reqid": reqid or None,
                    "mrno": norm(row.get("mrno")) or None,
                    "phone": phone,
                    "patient_name": norm(row.get("patient_name")) or None,
                    "status": "queued",
                    "is_paused": paused_default,
                    "force_send_now": False,
                    "cooloff_minutes": cooloff,
                    "attempt_count": 0,
                    "max_attempts": max_attempts,
                    "next_attempt_at": utc_iso(),
                    "metadata": self._job_metadata(row, {
                        "reconcile": True,
                        "lookback_hours": lookback_hours,
                        "reason": "reactivate_from_skipped_reportable"
                    }),
                    "created_at": utc_iso(),
                    "updated_at": utc_iso(),
                }
                if self.dry_run:
                    self.log.info("[dry-run] reconcile re-activate reqno=%s from=skipped", reqno)
                else:
                    self.sb.insert_job(jobs_table, new_job)
                added += 1
                continue

            # Regular job that the sender already determined is outsourced-only: PDF will never be at
            # the regular URL. Let _reconcile_outsourced_jobs create the correct split job instead.
            if norm(row.get("last_error")).lower() == "outsourced_only_regular_job":
                self.log.info("Reconcile skip reqno=%s reason=outsourced_only_regular_job", reqno)
                continue

            # For non-skipped rows, follow-up only when fully ready.
            if not self._is_overall_full_ready(live):
                continue

            # If live status shows all reportable tests are outsourced, a regular job
            # only fails when the result is an attached PDF (not at the regular report
            # URL) -- mirror run_once and let _reconcile_outsourced_jobs create the
            # split job instead. But a "transcribed" outsourced result is typed
            # directly into the consolidated report, so the regular URL DOES have it;
            # _reconcile_outsourced_jobs deliberately never makes a split job for
            # transcribed mode ("remains on regular flow"). Skipping here too would
            # leave the requisition in a gap where neither path ever sends it -- only
            # skip when at least one outsourced-ready test actually needs a split job.
            if self._is_outsourced_only_reportable(live) and self._outsourced_tests_need_split_job(live, reqid):
                self.log.info("Reconcile skip reqno=%s reason=outsourced_only_all_tests", reqno)
                continue

            # Duplicate guard: only enqueue follow-up if overall_status changed from PARTIAL to FULL.
            # Check against previous sent job's status snapshot (handles outsourced tests correctly).
            latest_sent = self.sb.latest_sent_snapshot(jobs_table, reqno)
            if latest_sent:
                prev_snap = latest_sent.get("last_status_snapshot") if isinstance(latest_sent.get("last_status_snapshot"), dict) else {}
                prev_overall = norm(prev_snap.get("overall_status")).upper()
                cur_overall = norm(live.get("overall_status")).upper()
                # Skip if overall status didn't improve (e.g., still PARTIAL, or was already FULL).
                # Proceed for any non-FULL prior state now reaching FULL -- covers PARTIAL_REPORT,
                # UNSENT, and also LAB_PENDING/NO_REPORT (a same-day-required test can force an
                # early "pending lab" send before every test, incl. a later-approved SPECIAL TESTS
                # result, is ready).
                if prev_overall and prev_overall != "FULL_REPORT" and cur_overall == "FULL_REPORT":
                    self.log.info("Reconcile follow-up reqno=%s overall_status %s→%s", reqno, prev_overall, cur_overall)
                else:
                    self.log.info("Reconcile skip reqno=%s overall_status %s→%s (no improvement)", reqno, prev_overall, cur_overall)
                    continue

            # If partial was sent and now fully ready, enqueue a follow-up send job.

            # Check if previous "PDF not found" error is now resolved (PDF generated)
            last_error = norm(row.get("last_error")).upper()
            if "PDF WAS NOT FOUND" in last_error:
                if not self._should_retry_pdf_not_found(jobs_table, reqno, reqid):
                    self.log.info(
                        "Reconcile skip reqno=%s reason=pdf_still_not_available",
                        reqno,
                    )
                    continue
                else:
                    self.log.info(
                        "Reconcile retry reqno=%s reason=pdf_now_available",
                        reqno,
                    )

            # Skip if invalid phone hasn't changed
            if self._should_skip_invalid_phone_reenqueue(jobs_table, reqno, phone):
                self.log.info(
                    "Reconcile skip reqno=%s reason=failed_reenqueue_capped phone=%s",
                    reqno,
                    phone,
                )
                continue

            # Skip requisitions with invalid phones — don't create follow-ups that will fail
            if not self._is_valid_phone(phone):
                self.log.warning("Reconcile skip reqno=%s reason=invalid_phone_format phone=%s", reqno, phone)
                continue

            # Dedup: skip if a follow-up job was already created recently for this requisition.
            if self._has_recent_followup(jobs_table, reqno):
                self.log.debug("Reconcile skip reqno=%s reason=followup_already_created", reqno)
                continue

            new_job = {
                "lab_id": lab_id,
                "reqno": reqno,
                "reqid": reqid or None,
                "mrno": norm(row.get("mrno")) or None,
                "phone": phone,
                "patient_name": norm(row.get("patient_name")) or None,
                "status": "queued",
                "is_paused": paused_default,
                "force_send_now": False,
                "cooloff_minutes": cooloff,
                "attempt_count": 0,
                "max_attempts": max_attempts,
                "next_attempt_at": utc_iso(),
                "metadata": self._job_metadata(row, {
                    "reconcile": True,
                    "lookback_hours": lookback_hours,
                    "reason": "partial_or_unsent_now_full_ready"
                }),
                "created_at": utc_iso(),
                "updated_at": utc_iso(),
            }
            if self.dry_run:
                self.log.info("[dry-run] reconcile enqueue reqno=%s", reqno)
            else:
                self.sb.insert_job(jobs_table, new_job)
            added += 1

        if added:
            self.log.info("Reconcile complete. new_followup_jobs=%s", added)
        return added

    def _reconcile_outsourced_jobs(self, jobs_table: str) -> int:
        """
        Detect outsourced split jobs that need to be created for previous-day requisitions.
        Runs against sent regular jobs whose snapshot showed outsourced-ready tests, then
        confirms PDF availability via the meta endpoint before inserting a split job.
        Controlled by enqueue.outsourced_lookback_hours (default 0 = disabled).
        """
        lookback_hours = int(self.cfg.get("enqueue", {}).get("outsourced_lookback_hours", 0) or 0)
        if lookback_hours <= 0:
            return 0

        since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        rows = self.sb.list_recent_sent_regular_jobs(
            jobs_table,
            since.isoformat(),
            limit=int(self.cfg.get("enqueue", {}).get("outsourced_lookback_max_rows", 500)),
        )
        if not rows:
            return 0

        lab_id = norm(self.cfg.get("whatsapp", {}).get("lab_id"))
        paused_default = bool(self.cfg.get("enqueue", {}).get("enqueue_paused_default", True))
        cooloff = int(self.cfg.get("worker", {}).get("cooloff_minutes_default", 30))
        max_attempts = int(
            self.cfg.get("worker", {}).get("outsourced_max_attempts")
            or self.cfg.get("worker", {}).get("max_attempts", 5)
        )
        attached_modes = {"attached_base", "attached_qr"}
        added = 0
        seen_reqnos: set = set()

        for row in rows:
            row_meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            if norm(row_meta.get("report_source")).lower() == "outsourced_report":
                continue

            reqno = norm(row.get("reqno"))
            reqid = norm(row.get("reqid"))
            phone = norm(row.get("phone"))
            if not reqno or not phone or reqno in seen_reqnos:
                continue
            seen_reqnos.add(reqno)

            # Pre-filter using stored snapshot — avoids live fetch for rows with no outsourced tests.
            snap = row.get("last_status_snapshot")
            if isinstance(snap, str):
                try:
                    snap = json.loads(snap)
                except Exception:
                    snap = {}
            if not isinstance(snap, dict) or not snap:
                continue
            if not self._extract_outsourced_ready_testids(snap):
                continue

            try:
                live = self._fetch_status(reqno=reqno, reqid=reqid)
            except Exception as e:
                self.log.warning("reconcile-outsourced status-fetch-failed reqno=%s err=%s", reqno, e)
                continue

            outsourced_testids = self._extract_outsourced_ready_testids(live)
            if not outsourced_testids:
                continue

            mrno = norm(row.get("mrno"))
            name = norm(row.get("patient_name"))

            for testid in outsourced_testids:
                if self._has_outsourced_job(
                    jobs_table,
                    reqno=reqno,
                    testid=testid,
                    statuses={"queued", "cooling_off", "eligible", "retrying", "sending", "processing", "sent"},
                ):
                    continue

                # Confirm PDF is available — fail-closed: skip if unavailable or endpoint unreachable.
                meta_resp = self._fetch_outsourced_meta(reqid=reqid, testid=testid)
                mode = norm(meta_resp.get("outsourced_mode") or meta_resp.get("mode")).lower()
                if not mode or mode not in attached_modes:
                    self.log.info(
                        "reconcile-outsourced skip reqno=%s testid=%s mode=%s reason=pdf_not_available",
                        reqno, testid, mode or "unknown",
                    )
                    continue

                job = {
                    "lab_id": lab_id,
                    "reqno": reqno,
                    "reqid": reqid or None,
                    "mrno": mrno or None,
                    "phone": phone,
                    "patient_name": name or None,
                    "status": "queued",
                    "is_paused": paused_default,
                    "force_send_now": False,
                    "cooloff_minutes": cooloff,
                    "attempt_count": 0,
                    "max_attempts": max_attempts,
                    "next_attempt_at": utc_iso(),
                    "metadata": self._job_metadata(row, {
                        "report_source": "outsourced_report",
                        "outsourced_testid": testid,
                        "outsourced_mode": mode,
                        "reason": "outsourced_reconcile",
                    }),
                    "created_at": utc_iso(),
                    "updated_at": utc_iso(),
                }
                if self.dry_run:
                    self.log.info("[dry-run] reconcile-outsourced enqueue reqno=%s testid=%s mode=%s", reqno, testid, mode)
                else:
                    self.sb.insert_job(jobs_table, job)
                self.log.info("reconcile-outsourced enqueued reqno=%s testid=%s mode=%s", reqno, testid, mode)
                added += 1

        # Also scan failed regular jobs — PDF may have arrived after they exhausted retries.
        # No snapshot pre-filter here: failed jobs may have been created before the test was outsourced.
        failed_rows = self.sb.list_recent_failed_regular_jobs(
            jobs_table,
            since.isoformat(),
            limit=int(self.cfg.get("enqueue", {}).get("outsourced_lookback_max_rows", 200)),
        )
        for row in failed_rows:
            row_meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            if norm(row_meta.get("report_source")).lower() == "outsourced_report":
                continue

            source_job_id = row.get("id")
            reqno = norm(row.get("reqno"))
            reqid = norm(row.get("reqid"))
            phone = norm(row.get("phone"))
            if not reqno or not phone or reqno in seen_reqnos:
                continue
            seen_reqnos.add(reqno)

            try:
                live = self._fetch_status(reqno=reqno, reqid=reqid)
            except Exception as e:
                self.log.warning("reconcile-outsourced-failed status-fetch-failed reqno=%s err=%s", reqno, e)
                continue

            outsourced_testids = self._extract_outsourced_ready_testids(live)
            if not outsourced_testids:
                continue

            mrno = norm(row.get("mrno"))
            name = norm(row.get("patient_name"))

            for testid in outsourced_testids:
                if self._has_outsourced_job(
                    jobs_table,
                    reqno=reqno,
                    testid=testid,
                    statuses={"queued", "cooling_off", "eligible", "retrying", "sending", "processing", "sent"},
                ):
                    continue

                meta_resp = self._fetch_outsourced_meta(reqid=reqid, testid=testid)
                mode = norm(meta_resp.get("outsourced_mode") or meta_resp.get("mode")).lower()
                if not mode or mode not in attached_modes:
                    self.log.info(
                        "reconcile-outsourced-failed skip reqno=%s testid=%s mode=%s reason=pdf_not_available",
                        reqno, testid, mode or "unknown",
                    )
                    continue

                job = {
                    "lab_id": lab_id,
                    "reqno": reqno,
                    "reqid": reqid or None,
                    "mrno": mrno or None,
                    "phone": phone,
                    "patient_name": name or None,
                    "status": "queued",
                    "is_paused": paused_default,
                    "force_send_now": False,
                    "cooloff_minutes": cooloff,
                    "attempt_count": 0,
                    "max_attempts": max_attempts,
                    "next_attempt_at": utc_iso(),
                    "metadata": self._job_metadata(row, {
                        "report_source": "outsourced_report",
                        "outsourced_testid": testid,
                        "outsourced_mode": mode,
                        "reason": "outsourced_reconcile_from_failed",
                    }),
                    "created_at": utc_iso(),
                    "updated_at": utc_iso(),
                }
                if self.dry_run:
                    self.log.info("[dry-run] reconcile-outsourced-failed enqueue reqno=%s testid=%s mode=%s", reqno, testid, mode)
                else:
                    self.sb.insert_job(jobs_table, job)
                    if source_job_id:
                        try:
                            defer_hours = int(self.cfg.get("enqueue", {}).get("outsourced_defer_hours", 6))
                            src_meta = dict(row.get("metadata") or {})
                            src_meta["deferred_reason"] = "outsourced_split_created"
                            src_meta["deferred_at"] = utc_iso()
                            self.sb.defer_job_by_id(jobs_table, source_job_id, hours=defer_hours, merged_meta=src_meta)
                            self.log.info("reconcile-outsourced-failed deferred source job_id=%s reqno=%s hours=%s", source_job_id, reqno, defer_hours)
                            source_job_id = None  # only defer once per reqno
                        except Exception as e:
                            self.log.warning("reconcile-outsourced-failed defer-failed job_id=%s err=%s", source_job_id, e)
                self.log.info("reconcile-outsourced-failed enqueued reqno=%s testid=%s mode=%s", reqno, testid, mode)
                added += 1

        if added:
            self.log.info("Reconcile-outsourced complete. new_outsourced_jobs=%s", added)
        return added

    def _expire_deferred_jobs(self, jobs_table: str) -> None:
        expire_days = int(self.cfg.get("enqueue", {}).get("outsourced_defer_expire_days", 10))
        cutoff = datetime.now(timezone.utc) - timedelta(days=expire_days)
        rows = self.sb.list_deferred_outsourced_jobs(jobs_table, limit=200)
        for row in rows:
            meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            deferred_at_str = meta.get("deferred_at")
            if not deferred_at_str:
                continue
            try:
                deferred_at = datetime.fromisoformat(deferred_at_str.replace("Z", "+00:00"))
                if deferred_at.tzinfo is None:
                    deferred_at = deferred_at.replace(tzinfo=timezone.utc)
                if deferred_at > cutoff:
                    continue
            except Exception:
                continue
            job_id = row.get("id")
            try:
                self.sb.fail_job_by_id(jobs_table, job_id, last_error="outsourced_defer_expired")
                self.log.warning("outsourced-defer-expired job_id=%s reqno=%s deferred_at=%s", job_id, row.get("reqno"), deferred_at_str)
            except Exception as e:
                self.log.warning("outsourced-defer-expired fail-error job_id=%s err=%s", job_id, e)

    def _today_ist(self) -> str:
        return now_ist().strftime("%Y-%m-%d")

    def _within_window(self) -> bool:
        start_hhmm = int(self.cfg.get("enqueue", {}).get("poll_start_hhmm", 730))
        end_hhmm = int(self.cfg.get("enqueue", {}).get("poll_end_hhmm", 2130))
        now_hhmm = time_hhmm_now()
        return start_hhmm <= now_hhmm <= end_hhmm

    def _can_start(self) -> bool:
        start_date = norm(self.cfg.get("enqueue", {}).get("start_date") or "")
        if start_date and self._today_ist() < start_date:
            return False
        return self._within_window()

    def _fetch_rows(self) -> List[Dict[str, Any]]:
        endpoint = norm(self.cfg.get("shivam", {}).get("requisitions_url"))
        if not endpoint:
            raise ValueError("Missing shivam.requisitions_url")

        method = norm(self.cfg.get("shivam", {}).get("method") or "POST").upper()
        payload = {
            "date": self._today_ist(),
            "limit": int(self.cfg.get("enqueue", {}).get("fetch_limit", 1000))
        }
        timeout = int(self.cfg.get("enqueue", {}).get("request_timeout_seconds", 20))

        if method == "GET":
            dated_endpoint = endpoint
            if "{" in endpoint and "}" in endpoint:
                dated_endpoint = endpoint.replace("{date}", self._today_ist())
            elif not endpoint.rstrip("/").endswith(self._today_ist()):
                dated_endpoint = endpoint.rstrip("/") + "/" + self._today_ist()
            r = self.http.get(dated_endpoint, timeout=timeout)
        else:
            r = self.http.post(endpoint, json=payload, timeout=timeout)

        r.raise_for_status()
        data = r.json()
        rows = (data.get("rows") or data.get("requisitions")) if isinstance(data, dict) else data
        if not isinstance(rows, list):
            raise ValueError("Unexpected requisitions response")
        return rows

    def run_once(self) -> None:
        if not self._can_start():
            self.log.info("Outside active window or before start date; skipping enqueue cycle")
            return

        jobs_table = self.cfg["tables"]["jobs"]
        rows = self._fetch_rows()
        self.log.info("Fetched %s requisition rows for %s", len(rows), self._today_ist())

        paused_default = bool(self.cfg.get("enqueue", {}).get("enqueue_paused_default", True))
        lab_id = norm(self.cfg.get("whatsapp", {}).get("lab_id"))
        cooloff = int(self.cfg.get("worker", {}).get("cooloff_minutes_default", 30))
        enqueued = 0

        for row in rows:
            reqno = norm(row.get("REQNO") or row.get("reqno"))
            reqid = norm(row.get("REQID") or row.get("reqid"))
            phone = norm(row.get("PHONENO") or row.get("phoneno") or row.get("MOBILENO") or row.get("mobileno") or row.get("phone"))
            name = norm(row.get("PATIENTNM") or row.get("patient_name"))
            mrno = norm(row.get("MRNO") or row.get("mrno"))
            if not reqno or not phone:
                continue

            if self._should_skip_invalid_phone_reenqueue(jobs_table, reqno, phone):
                self.log.info(
                    "Skip enqueue reqno=%s reason=failed_reenqueue_capped phone=%s",
                    reqno,
                    phone,
                )
                continue

            latest = self.sb.latest_job(jobs_table, reqno)
            if latest:
                latest_status = norm(latest.get("status")).lower()
                if latest_status in {"queued", "cooling_off", "eligible", "retrying", "sending", "processing", "sent"}:
                    continue
                # If latest is skipped/failed, re-evaluate live status and allow re-activation
                # when reportable tests are present (e.g., non-same-day culture/TMT pending).
                if latest_status in {"skipped", "failed"}:
                    try:
                        live = self._fetch_status(reqno=reqno, reqid=reqid)
                    except Exception as e:
                        self.log.warning("Skip reactivation reqno=%s status-fetch-failed err=%s", reqno, e)
                        continue
                    if not self._has_any_reportable_tests(live):
                        continue

            if self.sb.dispatched_exists(reqno, phone):
                continue

            # Live status is used for outsourced split-job detection and reactivation decisions.
            try:
                live = self._fetch_status(reqno=reqno, reqid=reqid)
            except Exception as e:
                self.log.warning("Skip enqueue reqno=%s reason=status-fetch-failed err=%s", reqno, e)
                continue

            # Split outsourced attached-PDF tests into separate jobs (works for mixed and outsourced-only requisitions).
            outsourced_testids = self._extract_outsourced_ready_testids(live)
            attached_modes = {"attached_base", "attached_qr"}
            outsourced_enqueued = 0
            for testid in outsourced_testids:
                # Dedupe by reqno+testid for active/sent outsourced jobs.
                if self._has_outsourced_job(
                    jobs_table,
                    reqno=reqno,
                    testid=testid,
                    statuses={"queued", "cooling_off", "eligible", "retrying", "sending", "processing", "sent"},
                ):
                    continue
                meta = self._fetch_outsourced_meta(reqid=reqid, testid=testid)
                mode = norm(meta.get("outsourced_mode") or meta.get("mode")).lower()
                # Enqueue separate outsourced jobs for attached-PDF routes.
                # If mode resolver is unavailable, fail-open to split-job enqueue so
                # outsourced ready tests are not silently dropped.
                # Transcribed rows remain on regular requisition flow.
                if mode and mode not in attached_modes:
                    continue
                normalized_mode = mode or "unavailable"
                job = {
                    "lab_id": lab_id,
                    "reqno": reqno,
                    "reqid": reqid or None,
                    "mrno": mrno or None,
                    "phone": phone,
                    "patient_name": name or None,
                    "status": "queued",
                    "is_paused": paused_default,
                    "force_send_now": False,
                    "cooloff_minutes": cooloff,
                    "attempt_count": 0,
                    "max_attempts": int(self.cfg.get("worker", {}).get("max_attempts", 5)),
                    "next_attempt_at": utc_iso(),
                    "metadata": self._job_metadata(row, {
                        "report_source": "outsourced_report",
                        "outsourced_testid": testid,
                        "outsourced_mode": normalized_mode,
                        "reason": "outsourced_separate_job",
                    }),
                    "created_at": utc_iso(),
                    "updated_at": utc_iso(),
                }
                if self.dry_run:
                    self.log.info("[dry-run] enqueue-outsourced reqno=%s testid=%s mode=%s", reqno, testid, mode)
                else:
                    self.sb.insert_job(jobs_table, job)
                enqueued += 1
                outsourced_enqueued += 1

            # For outsourced-only requisitions, convert existing regular job or create outsourced job
            if self._is_outsourced_only_reportable(live) and outsourced_enqueued > 0:
                # If a single outsourced test exists and there's an existing regular job, convert it
                if len(outsourced_testids) == 1:
                    existing_regular = self.sb.latest_job(jobs_table, reqno)
                    if existing_regular and norm(existing_regular.get("status")).lower() in {"failed", "skipped", "queued", "cooling_off"}:
                        # Convert existing regular job to outsourced
                        meta = parse_metadata(existing_regular.get("metadata"))
                        meta["report_source"] = "outsourced_report"
                        meta["outsourced_testid"] = outsourced_testids[0]
                        if self.dry_run:
                            self.log.info("[dry-run] convert-to-outsourced reqno=%s", reqno)
                        else:
                            self.sb.patch_job(jobs_table, existing_regular.get("id"), {
                                "metadata": meta,
                                "status": "queued",
                                "next_attempt_at": utc_iso(),
                                "last_error": None,
                                "updated_at": utc_iso(),
                            })
                        self.log.info("Convert regular job to outsourced reqno=%s testid=%s", reqno, outsourced_testids[0])
                continue

            job = {
                "lab_id": lab_id,
                "reqno": reqno,
                "reqid": reqid or None,
                "mrno": mrno or None,
                "phone": phone,
                "patient_name": name or None,
                "status": "queued",
                "is_paused": paused_default,
                "force_send_now": False,
                "cooloff_minutes": cooloff,
                "attempt_count": 0,
                "max_attempts": int(self.cfg.get("worker", {}).get("max_attempts", 5)),
                "next_attempt_at": utc_iso(),
                "created_at": utc_iso(),
                "updated_at": utc_iso(),
            }
            meta = self._source_metadata(row)
            if meta:
                job["metadata"] = meta
            if self.dry_run:
                self.log.info("[dry-run] enqueue reqno=%s", reqno)
            else:
                self.sb.insert_job(jobs_table, job)
            enqueued += 1

        self.log.info("Enqueue complete. new_jobs=%s", enqueued)
        self._reconcile_recent(jobs_table)
        self._reconcile_outsourced_jobs(jobs_table)
        self._expire_deferred_jobs(jobs_table)

    # ---------------------------------------------------------------------
    # Patient message jobs (framework) -- Shivam's SMS Event/Template/Variable
    # master pattern. Rides this poll cycle, creates NO dispatch job. Config is
    # cfg["patient_message_jobs"]; per job we fetch trigger.source_url and POST
    # each row to labit-main /api/internal/whatsapp/campaign-send, which owns
    # dedup + variable resolution + attachment + send + audit. The loop here
    # never changes per job.

    def _campaign_send_url(self) -> str:
        explicit = norm(self.cfg.get("patient_message_jobs_send_url"))
        if explicit:
            return explicit
        base = norm(self.cfg.get("whatsapp", {}).get("internal_send_url"))
        if base.endswith("/report-template-send"):
            return base[: -len("/report-template-send")] + "/campaign-send"
        return base

    def run_patient_message_jobs_once(self) -> None:
        jobs = self.cfg.get("patient_message_jobs", [])
        if not isinstance(jobs, list) or not jobs:
            return
        url = self._campaign_send_url()
        if not url:
            return
        lab_id = norm(self.cfg.get("whatsapp", {}).get("lab_id"))
        token = norm(self.cfg.get("whatsapp", {}).get("internal_send_token"))
        timeout = int(self.cfg.get("enqueue", {}).get("request_timeout_seconds", 20))
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "x-internal-token": token,
        }
        now_hhmm = time_hhmm_now()
        today = self._today_ist()
        for job in jobs:
            key = norm(job.get("key"))
            if not key or not bool(job.get("enabled", False)):
                continue
            w = job.get("window", {}) if isinstance(job.get("window"), dict) else {}
            if not (int(w.get("start_hhmm", 700)) <= now_hhmm <= int(w.get("end_hhmm", 2130))):
                continue
            start_date = norm(job.get("start_date"))
            if start_date and today < start_date:
                continue
            trig = job.get("trigger", {}) if isinstance(job.get("trigger"), dict) else {}
            src = norm(trig.get("source_url")).replace("{today}", today).replace("{date}", today)
            if not src:
                continue
            try:
                resp = self.http.get(src, timeout=timeout)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                self.log.warning("pmj %s: source fetch failed: %s", key, exc)
                continue
            rows = data
            for seg in [x for x in norm(trig.get("rows_path")).split(".") if x]:
                rows = rows.get(seg) if isinstance(rows, dict) else None
            if not isinstance(rows, list):
                rows = data if isinstance(data, list) else []
            cap = int(job.get("max_per_cycle", 100))
            sent = skipped = failed = 0
            for row in rows[:cap]:
                if self.dry_run:
                    self.log.info("[dry-run] pmj %s row=%s", key, norm(row.get("reqno") or row.get("reqid")))
                    continue
                payload = {"key": key, "lab_id": lab_id, "context": row}
                try:
                    r = self.http.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
                    if r.ok:
                        j = r.json() if r.text else {}
                        if j.get("sent"):
                            sent += 1
                        elif j.get("skipped"):
                            skipped += 1
                        else:
                            failed += 1
                    else:
                        failed += 1
                        self.log.warning("pmj %s send -> %s %s", key, r.status_code, r.text[:200])
                except Exception as exc:
                    failed += 1
                    self.log.warning("pmj %s send failed: %s", key, exc)
            self.log.info("pmj %s: rows=%s sent=%s skipped=%s failed=%s", key, len(rows), sent, skipped, failed)

def main() -> int:
    parser = argparse.ArgumentParser(description="Requisition enqueue worker")
    parser.add_argument("--config", required=True, help="Path to worker config JSON")
    parser.add_argument("--dry-run", action="store_true", help="Do not write jobs")
    parser.add_argument("--watch", action="store_true", help="Run continuously with lazy morning cadence")
    args = parser.parse_args()

    cfg = load_json(args.config)
    worker = EnqueueWorker(cfg, dry_run=args.dry_run)

    if not args.watch:
        worker.run_once()
        return 0

    # Enqueue keeps its lazy morning cadence (hourly pre-10am, 5-min after).
    # Patient-message-jobs need tighter, window-independent coverage (a
    # requisition registered at 07:15 should not wait an hour for its
    # welcome), so the loop itself ticks on the SHORTER interval and only
    # runs the heavy enqueue pass on its own schedule.
    pmj_active = any(
        isinstance(j, dict) and j.get("enabled")
        for j in (cfg.get("patient_message_jobs") or [])
    )
    pmj_poll = int(cfg.get("patient_message_jobs_poll_seconds", 300))
    last_enqueue_at = 0.0

    while True:
        now = datetime.now()
        hhmm = now.hour * 100 + now.minute
        fast_after_hhmm = int(cfg.get("enqueue", {}).get("poll_fast_after_hhmm", 1000))
        if hhmm < fast_after_hhmm:
            enqueue_interval = int(cfg.get("enqueue", {}).get("poll_seconds_pre_10am", 3600))
        else:
            enqueue_interval = int(cfg.get("enqueue", {}).get("poll_seconds_post_10am", 300))

        if time.time() - last_enqueue_at >= enqueue_interval:
            try:
                worker.run_once()
            except Exception as exc:
                worker.log.exception("run_once failed, will retry after sleep: %s", exc)
            last_enqueue_at = time.time()

        try:
            worker.run_patient_message_jobs_once()
        except Exception as exc:
            worker.log.exception("patient_message_jobs failed, will retry after sleep: %s", exc)

        loop_sleep = min(enqueue_interval, pmj_poll) if pmj_active else enqueue_interval
        time.sleep(max(30, loop_sleep))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
