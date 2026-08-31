import unittest
from datetime import timedelta
from unittest import mock

from report_sender_worker import ReportSenderWorker, ready_reportable_testids, utc_now, utc_iso


def base_cfg():
    return {
        "supabase": {"url": "https://example.supabase.co", "service_role_key": "k"},
        "tables": {"jobs": "report_auto_dispatch_jobs", "events": "report_auto_dispatch_events"},
        "labbit_py": {"base_url": "https://api.sdrc.in/py", "status_mode": "reqno"},
        "whatsapp": {"internal_send_url": "https://lab/api/internal/whatsapp/send", "internal_send_token": "t", "lab_id": "lab"},
        "worker": {
            "poll_seconds": 20,
            "poll_start_hhmm": 0,
            "poll_end_hhmm": 2359,
            "batch_size": 10,
            "max_scan_rows": 10,
            "max_attempts": 3,
            "stuck_queued_wait_hours": 6,
            "stuck_cooling_off_hours": 2,
            "stuck_max_auto_requeues": 2,
            "stuck_scan_limit": 100,
        },
    }


class FakeSB:
    def __init__(self):
        self.jobs = []
        self.latest_event = {}
        self.patches = []
        self.events = []
        self.claimed = {}

    def list_watchdog_candidates(self, table, limit=500):
        return list(self.jobs)

    def list_by_status(self, table, status, limit=500):
        return [j for j in self.jobs if j.get("status") == status][:limit]

    def list_failed_invalid_phone(self, table, limit=100):
        return []

    def list_paused_jobs(self, table, limit=25):
        return []

    def list_stale_inflight(self, table, before_iso, limit=200):
        return []

    def get_latest_event(self, table, job_id):
        return self.latest_event.get(job_id)

    def patch_job(self, table, row_id, patch):
        self.patches.append((row_id, dict(patch)))
        return {"id": row_id, **patch}

    def insert_event(self, table, row):
        self.events.append(dict(row))

    def select_jobs(self, table, limit, now_iso, offset=0):
        return list(self.jobs)

    def claim_job(self, table, row_id, worker_token):
        if self.claimed.get(row_id) is False:
            return None
        self.claimed[row_id] = False
        for j in self.jobs:
            if j.get("id") == row_id:
                return dict(j)
        return None


class WorkerTests(unittest.TestCase):
    def make_worker(self):
        with mock.patch.object(ReportSenderWorker, "_recover_cooling_off_jobs", return_value=None):
            w = ReportSenderWorker(base_cfg(), dry_run=False)
        w.sb = FakeSB()
        return w

    def test_watchdog_auto_requeue(self):
        w = self.make_worker()
        j = {"id": 1, "reqno": "R1", "status": "queued", "metadata": {}, "sent_at": None}
        w.sb.jobs = [j]
        old = utc_now() - timedelta(hours=7)
        w.sb.latest_event[1] = {"event_type": "queued_wait", "created_at": utc_iso(old)}
        w._watchdog_stuck_jobs()
        self.assertTrue(any(p[1].get("status") == "queued" for p in w.sb.patches))
        self.assertTrue(any(e.get("event_type") == "auto_requeue_stuck" for e in w.sb.events))

    def test_watchdog_terminal_after_cap(self):
        w = self.make_worker()
        j = {"id": 2, "reqno": "R2", "status": "cooling_off", "metadata": {"stuck_requeue_count": 2}, "sent_at": None}
        w.sb.jobs = [j]
        old = utc_now() - timedelta(hours=3)
        w.sb.latest_event[2] = {"event_type": "cooling_off", "created_at": utc_iso(old)}
        w._watchdog_stuck_jobs()
        self.assertTrue(any(p[1].get("status") == "failed" for p in w.sb.patches))
        self.assertTrue(any(e.get("event_type") == "failed_timeout" for e in w.sb.events))

    def test_reconcile_sent_terminal(self):
        w = self.make_worker()
        job = {"id": 3, "reqno": "R3", "status": "queued"}
        w.sb.latest_event[3] = {"event_type": "sent", "created_at": utc_iso()}
        w._reconcile_job_state(job)
        self.assertTrue(any(p[1].get("status") == "sent" for p in w.sb.patches))

    def test_process_once_claim_guard(self):
        w = self.make_worker()
        w.sb.jobs = [{"id": 4, "reqno": "R4", "status": "queued", "next_attempt_at": utc_iso(utc_now() - timedelta(minutes=1))}]
        called = {"count": 0}
        w._watchdog_stuck_jobs = lambda: None
        w._reconcile_job_state = lambda job: None
        w.process_job = lambda job: called.__setitem__("count", called["count"] + 1)
        w.process_once()
        w.process_once()
        self.assertEqual(called["count"], 1)

    def test_ready_reportable_testids_only_ready_lab_radiology(self):
        status = {
            "tests": [
                {"TEST_ID": "T1", "GROUPNM": "LAB", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
                {"TEST_ID": "T2", "GROUPNM": "LAB", "REPORT_STATUS": "PENDING", "APPROVEDFLG": "0"},
                {"TEST_ID": "T3", "GROUPNM": "RADIOLOGY", "REPORT_STATUS": "RADIOLOGY_READY", "APPROVEDFLG": "1"},
                {"TEST_ID": "T4", "GROUPNM": "OTHER", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
            ]
        }
        self.assertEqual(ready_reportable_testids(status), ["T1", "T3"])

    def test_mark_delivery_status_posts_testids(self):
        w = self.make_worker()
        calls = []

        class FakeResponse:
            ok = True
            text = '{"ok":true}'

            def json(self):
                return {"ok": True}

        w.http.post = lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse()
        result = w._mark_delivery_status(
            {"id": 5, "reqno": "R5", "metadata": {}},
            {"reqno": "R5"},
            ["T1", "T3"],
        )
        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls[0][0], "https://api.sdrc.in/py/delivery/status/update")
        self.assertIn('"testids": ["T1", "T3"]', calls[0][1]["data"])


if __name__ == "__main__":
    unittest.main()
