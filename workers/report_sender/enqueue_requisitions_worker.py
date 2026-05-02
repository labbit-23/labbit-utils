#!/usr/bin/env python3
import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

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

    def list_recent_jobs(self, table: str, since_iso: str, limit: int = 2000) -> List[Dict[str, Any]]:
        u = f"{self.base}/{table}"
        p = {
            "select": "id,lab_id,reqno,reqid,mrno,phone,patient_name,status,report_label,is_paused,created_at,updated_at",
            "or": f"(created_at.gte.{since_iso},updated_at.gte.{since_iso})",
            "order": "updated_at.desc",
            "limit": str(limit)
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

    def _is_ready_test(self, row: Dict[str, Any]) -> bool:
        status = norm(row.get("REPORT_STATUS") or row.get("report_status")).upper()
        approved = norm(row.get("APPROVEDFLG") or row.get("approvedflg")) == "1"
        return approved or status in {"LAB_READY", "RADIOLOGY_READY"}

    def _same_day_full_ready(self, status: Dict[str, Any]) -> bool:
        tests = status.get("tests") if isinstance(status.get("tests"), list) else []
        required = [t for t in tests if isinstance(t, dict) and self._is_same_day_required(t)]
        if not required:
            return False
        return all(self._is_ready_test(row) for row in required)

    def _is_partial_label(self, label: Any) -> bool:
        t = norm(label).lower()
        return "partial" in t

    def _is_full_label(self, label: Any) -> bool:
        t = norm(label).lower()
        return "complete" in t and "partial" not in t

    def _reconcile_recent(self, jobs_table: str) -> int:
        lookback_hours = int(self.cfg.get("enqueue", {}).get("lookback_hours", 0) or 0)
        if lookback_hours <= 0:
            return 0

        since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        recent = self.sb.list_recent_jobs(jobs_table, since.isoformat(), limit=int(self.cfg.get("enqueue", {}).get("lookback_max_rows", 2000)))
        if not recent:
            return 0

        # Only hit status API for unsent or partial-sent rows.
        candidates: List[Dict[str, Any]] = []
        for row in recent:
            status = norm(row.get("status")).lower()
            if status in {"queued", "cooling_off", "eligible", "retrying", "failed", "sending"}:
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

            if not self._same_day_full_ready(live):
                continue

            # If partial was sent and now fully ready, enqueue a follow-up send job.
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

            if self.sb.job_exists(jobs_table, reqno):
                continue

            if self.sb.dispatched_exists(reqno, phone):
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

        worker.run_once()
        worker.log.info("Sleeping %s seconds before next enqueue cycle", sleep_seconds)
        import time
        time.sleep(max(30, sleep_seconds))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
