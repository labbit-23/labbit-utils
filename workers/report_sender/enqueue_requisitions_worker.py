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


def digits_only(v: Any) -> str:
    return "".join(ch for ch in str(v or "") if ch.isdigit())


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
            "select": "id,reqno,status,metadata,updated_at,created_at",
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

    def list_recent_jobs(self, table: str, since_iso: str, limit: int = 2000) -> List[Dict[str, Any]]:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,lab_id,reqno,reqid,mrno,phone,patient_name,status,report_label,last_error,is_paused,created_at,updated_at",
            "or": f"(created_at.gte.{since_iso},updated_at.gte.{since_iso})",
            "order": "updated_at.desc",
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
        if mode == "reqid" and reqid:
            url = f"{base}/report-status-reqid/{reqid}"
        elif reqno:
            url = f"{base}/report-status/{reqno}"
        elif reqid:
            url = f"{base}/report-status-reqid/{reqid}"
        else:
            raise ValueError("Missing reqno/reqid for status fetch")
        r = self.http.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict):
            raise ValueError("Unexpected status response")
        return data

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

    def _should_skip_invalid_phone_reenqueue(self, jobs_table: str, reqno: str, incoming_phone: str) -> bool:
        # Guard against churn: if ANY recent failed INVALID_PHONE exists for this reqno
        # with the same phone, do not create yet another job.
        # Only retry if phone number has actually changed.
        rows = self.sb.list_jobs_by_reqno(jobs_table, reqno=reqno, limit=200)
        cur_digits = digits_only(incoming_phone)
        if not cur_digits:
            return False
        # Compare last 10 digits to handle country-code prefix mismatches.
        cur_phone_10 = cur_digits[-10:]
        for row in rows:
            status = norm(row.get("status")).lower()
            last_error = norm(row.get("last_error")).upper()
            if status == "failed" and last_error == "INVALID_PHONE":
                prev_digits = digits_only(row.get("phone"))
                if prev_digits and prev_digits[-10:] == cur_phone_10:
                    return True
        return False

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
        recent = self.sb.list_recent_jobs(jobs_table, since.isoformat(), limit=int(self.cfg.get("enqueue", {}).get("lookback_max_rows", 2000)))
        if not recent:
            return 0

        # Hit status API for unsent/partial rows, including skipped rows so they can be re-evaluated.
        candidates: List[Dict[str, Any]] = []
        for row in recent:
            status = norm(row.get("status")).lower()
            if status in {"queued", "cooling_off", "eligible", "retrying", "failed", "sending", "skipped"}:
                candidates.append(row)
                continue
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
            if self.sb.has_sent_full(jobs_table, reqno):
                continue

            # Skip reconciled follow-up when already fully sent before.
            if norm(row.get("status")).lower() == "sent" and self._is_full_label(row.get("report_label")):
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
                        "Reconcile skip reqno=%s reason=failed_invalid_phone_unchanged phone=%s",
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
                    "metadata": {
                        "reconcile": True,
                        "lookback_hours": lookback_hours,
                        "reason": "reactivate_from_skipped_reportable"
                    },
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

            # If live status shows all reportable tests are outsourced, creating a regular job
            # will always fail (PDF not at regular URL). Mirror run_once behaviour and let
            # _reconcile_outsourced_jobs create the correct split job via the meta endpoint.
            if self._is_outsourced_only_reportable(live):
                self.log.info("Reconcile skip reqno=%s reason=outsourced_only_all_tests", reqno)
                continue

            # Duplicate guard: only enqueue follow-up if ready-count increased from latest sent snapshot.
            latest_sent = self.sb.latest_sent_snapshot(jobs_table, reqno)
            if latest_sent:
                prev_snap = latest_sent.get("last_status_snapshot") if isinstance(latest_sent.get("last_status_snapshot"), dict) else {}
                prev_total, prev_ready = self._same_day_ready_counts(prev_snap if isinstance(prev_snap, dict) else {})
                cur_total, cur_ready = self._same_day_ready_counts(live)
                if cur_total > 0 and cur_total == prev_total and cur_ready <= prev_ready:
                    self.log.info("Reconcile skip reqno=%s ready-count unchanged prev=%s/%s cur=%s/%s", reqno, prev_ready, prev_total, cur_ready, cur_total)
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
                    "Reconcile skip reqno=%s reason=failed_invalid_phone_unchanged phone=%s",
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
                "metadata": {
                    "reconcile": True,
                    "lookback_hours": lookback_hours,
                    "reason": "partial_or_unsent_now_full_ready"
                },
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
                    "metadata": {
                        "report_source": "outsourced_report",
                        "outsourced_testid": testid,
                        "outsourced_mode": mode,
                        "reason": "outsourced_reconcile",
                    },
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
                    "metadata": {
                        "report_source": "outsourced_report",
                        "outsourced_testid": testid,
                        "outsourced_mode": mode,
                        "reason": "outsourced_reconcile_from_failed",
                    },
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
                    "Skip enqueue reqno=%s reason=failed_invalid_phone_unchanged phone=%s",
                    reqno,
                    phone,
                )
                continue

            latest = self.sb.latest_job(jobs_table, reqno)
            if latest:
                latest_status = norm(latest.get("status")).lower()
                if latest_status in {"queued", "cooling_off", "eligible", "retrying", "sending", "processing", "sent"}:
                    continue
                # Never re-enqueue a job that already failed with INVALID_PHONE — the phone
                # must be corrected manually before retrying.
                if norm(latest.get("last_error")).upper() == "INVALID_PHONE":
                    self.log.info(
                        "Skip enqueue reqno=%s reason=latest_job_invalid_phone phone=%s",
                        reqno, phone,
                    )
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
                    "metadata": {
                        "report_source": "outsourced_report",
                        "outsourced_testid": testid,
                        "outsourced_mode": normalized_mode,
                        "reason": "outsourced_separate_job",
                    },
                    "created_at": utc_iso(),
                    "updated_at": utc_iso(),
                }
                if self.dry_run:
                    self.log.info("[dry-run] enqueue-outsourced reqno=%s testid=%s mode=%s", reqno, testid, mode)
                else:
                    self.sb.insert_job(jobs_table, job)
                enqueued += 1
                outsourced_enqueued += 1

            # For outsourced-only requisitions, avoid creating a regular combined job when
            # at least one attached outsourced job was created.
            if self._is_outsourced_only_reportable(live) and outsourced_enqueued > 0:
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
            if self.dry_run:
                self.log.info("[dry-run] enqueue reqno=%s", reqno)
            else:
                self.sb.insert_job(jobs_table, job)
            enqueued += 1

        self.log.info("Enqueue complete. new_jobs=%s", enqueued)
        self._reconcile_recent(jobs_table)
        self._reconcile_outsourced_jobs(jobs_table)
        self._expire_deferred_jobs(jobs_table)


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

    while True:
        now = datetime.now()
        hhmm = now.hour * 100 + now.minute
        if hhmm < 1000:
            sleep_seconds = int(cfg.get("enqueue", {}).get("poll_seconds_pre_10am", 3600))
        else:
            sleep_seconds = int(cfg.get("enqueue", {}).get("poll_seconds_post_10am", 300))

        try:
            worker.run_once()
        except Exception as exc:
            worker.log.exception("run_once failed, will retry after sleep: %s", exc)
        worker.log.info("Sleeping %s seconds before next enqueue cycle", sleep_seconds)
        time.sleep(max(30, sleep_seconds))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
