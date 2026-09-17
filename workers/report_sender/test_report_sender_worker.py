import unittest
from datetime import datetime, timedelta
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
        self.active_chat_session = None
        self.recent_inbound_message = None
        self.half_day_dates = set()

    def half_day_exists(self, date_iso):
        return date_iso in self.half_day_dates

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

    def list_stale_sent_inflight(self, table, before_iso, limit=200):
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

    def find_active_chat_session(self, table, *, lab_id, phone_variants, since_iso):
        return self.active_chat_session

    def find_recent_inbound_message(self, table, *, lab_id, phone_variants, since_iso):
        return self.recent_inbound_message


class FakeResponse:
    def __init__(self, ok=True, text='{"ok":true,"provider_message_id":"wamid.test"}', status_code=200):
        self.ok = ok
        self.text = text
        self.status_code = status_code

    def json(self):
        import json
        return json.loads(self.text) if self.text else {}


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

    def test_partial_cutoff_window_sunday_override(self):
        w = self.make_worker()
        w.cfg["worker"]["partial_send_cutoff_from_hhmm"] = 1730
        w.cfg["worker"]["partial_send_cutoff_to_hhmm"] = 1800
        w.cfg["worker"]["partial_send_cutoff_overrides"] = {
            "sunday": {"from_hhmm": 1430, "to_hhmm": 1500}
        }

        monday = datetime(2026, 9, 14).astimezone()  # a Monday
        sunday = datetime(2026, 9, 13).astimezone()  # a Sunday
        self.assertEqual(w._partial_cutoff_window(monday), (1730, 1800))
        self.assertEqual(w._partial_cutoff_window(sunday), (1430, 1500))

    def test_partial_cutoff_window_sunday_falls_back_when_unset(self):
        # No overrides configured at all -- Sunday must behave exactly
        # like every other day (this is a no-op until someone opts in).
        w = self.make_worker()
        w.cfg["worker"]["partial_send_cutoff_from_hhmm"] = 1730
        w.cfg["worker"]["partial_send_cutoff_to_hhmm"] = 1800
        sunday = datetime(2026, 9, 13).astimezone()
        self.assertEqual(w._partial_cutoff_window(sunday), (1730, 1800))

    def test_partial_cutoff_window_seeded_half_day_takes_priority(self):
        w = self.make_worker()
        w.cfg["worker"]["partial_send_cutoff_from_hhmm"] = 1730
        w.cfg["worker"]["partial_send_cutoff_to_hhmm"] = 1800
        w.cfg["worker"]["partial_send_cutoff_overrides"] = {
            "half_day": {"from_hhmm": 1430, "to_hhmm": 1500}
        }
        tuesday = datetime(2026, 9, 15).astimezone()  # a Tuesday, no weekday override
        w.sb.half_day_dates.add(tuesday.date().isoformat())
        self.assertEqual(w._partial_cutoff_window(tuesday), (1430, 1500))
        # cached -- a second call must not need another lookup
        w.sb.half_day_dates.clear()
        self.assertEqual(w._partial_cutoff_window(tuesday), (1430, 1500))

    def test_partial_cutoff_window_other_weekday_override(self):
        # Config-driven for ANY day, not just Sunday -- e.g. a Saturday
        # half-day, with no code change needed.
        w = self.make_worker()
        w.cfg["worker"]["partial_send_cutoff_from_hhmm"] = 1730
        w.cfg["worker"]["partial_send_cutoff_to_hhmm"] = 1800
        w.cfg["worker"]["partial_send_cutoff_overrides"] = {
            "saturday": {"from_hhmm": 1500, "to_hhmm": 1530}
        }
        saturday = datetime(2026, 9, 12).astimezone()  # a Saturday
        sunday = datetime(2026, 9, 13).astimezone()
        self.assertEqual(w._partial_cutoff_window(saturday), (1500, 1530))
        self.assertEqual(w._partial_cutoff_window(sunday), (1730, 1800))

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

        w.http.post = lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse(text='{"ok":true}')
        result = w._mark_delivery_status(
            {"id": 5, "reqno": "R5", "metadata": {}},
            {"reqno": "R5"},
            ["T1", "T3"],
        )
        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls[0][0], "https://api.sdrc.in/py/delivery/status/update")
        self.assertIn('"testids": ["T1", "T3"]', calls[0][1]["data"])

    def test_session_document_route_sends_document_when_patient_window_active(self):
        w = self.make_worker()
        w.cfg["whatsapp"].update({
            "internal_send_url": "https://lab/api/internal/whatsapp/report-template-send",
            "session_document_send_url": "https://lab/api/internal/whatsapp/send",
            "session_document_route": {
                "enabled": True,
                "trial_numbers": ["9849025601"],
                "trial_until": "2999-01-01T00:00:00+00:00",
                "label": "Testing 090322026",
            },
        })
        w.sb.active_chat_session = {
            "id": "sess-1",
            "phone": "919849025601",
            "last_user_message_at": utc_iso(),
        }
        calls = []
        w.http.post = lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse()

        result = w._send_report_message(
            {"id": 6, "reqno": "R6", "reqid": "REQ6", "phone": "9849025601", "metadata": {}},
            {"reqno": "R6", "reqid": "REQ6", "patient_name": "Patient", "tests": []},
            "complete lab",
            "https://api.sdrc.in/py/report/REQ6?reqno=R6",
        )

        self.assertEqual(calls[0][0], "https://lab/api/internal/whatsapp/send")
        self.assertIn('"message_type": "document"', calls[0][1]["data"])
        self.assertIn('"session_document_route_label": "Testing 090322026"', calls[0][1]["data"])
        self.assertEqual(result["dispatch_route"], "session_document")
        self.assertTrue(any(e.get("event_type") == "session_document_sent" for e in w.sb.events))

    def test_session_document_route_falls_back_to_template_without_active_window(self):
        w = self.make_worker()
        w.cfg["whatsapp"].update({
            "internal_send_url": "https://lab/api/internal/whatsapp/report-template-send",
            "session_document_send_url": "https://lab/api/internal/whatsapp/send",
            "session_document_route": {
                "enabled": True,
                "trial_numbers": ["9849025601"],
                "trial_until": "2999-01-01T00:00:00+00:00",
            },
        })
        calls = []
        w.http.post = lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse()

        w._send_report_message(
            {"id": 7, "reqno": "R7", "reqid": "REQ7", "phone": "9849025601", "metadata": {}},
            {"reqno": "R7", "reqid": "REQ7", "patient_name": "Patient", "tests": []},
            "complete lab",
            "https://api.sdrc.in/py/report/REQ7?reqno=R7",
        )

        self.assertEqual(calls[0][0], "https://lab/api/internal/whatsapp/report-template-send")
        self.assertIn('"report_source": "requisition_report"', calls[0][1]["data"])
        self.assertTrue(any(e.get("event_type") == "session_document_ineligible" for e in w.sb.events))

    def test_session_document_failure_falls_back_to_template(self):
        w = self.make_worker()
        w.cfg["whatsapp"].update({
            "internal_send_url": "https://lab/api/internal/whatsapp/report-template-send",
            "session_document_send_url": "https://lab/api/internal/whatsapp/send",
            "session_document_route": {
                "enabled": True,
                "trial_numbers": ["9849025601"],
                "trial_until": "2999-01-01T00:00:00+00:00",
            },
        })
        w.sb.recent_inbound_message = {
            "id": "msg-1",
            "phone": "919849025601",
            "created_at": utc_iso(),
        }
        calls = []

        def fake_post(url, **kwargs):
            calls.append((url, kwargs))
            if url.endswith("/send"):
                return FakeResponse(ok=False, text="gateway rejected document", status_code=502)
            return FakeResponse()

        w.http.post = fake_post
        result = w._send_report_message(
            {"id": 8, "reqno": "R8", "reqid": "REQ8", "phone": "9849025601", "metadata": {}},
            {"reqno": "R8", "reqid": "REQ8", "patient_name": "Patient", "tests": []},
            "complete lab",
            "https://api.sdrc.in/py/report/REQ8?reqno=R8",
        )

        self.assertEqual([call[0] for call in calls], [
            "https://lab/api/internal/whatsapp/send",
            "https://lab/api/internal/whatsapp/report-template-send",
        ])
        self.assertEqual(result["provider_message_id"], "wamid.test")
        self.assertTrue(any(e.get("event_type") == "session_document_failed_fallback_template" for e in w.sb.events))


if __name__ == "__main__":
    unittest.main()
