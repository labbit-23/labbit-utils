#!/usr/bin/env python3
import argparse
import json
import logging
from datetime import datetime
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
