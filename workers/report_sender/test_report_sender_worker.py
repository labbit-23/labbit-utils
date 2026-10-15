import unittest
from datetime import datetime, timedelta
from unittest import mock

from report_sender_worker import (
    ReportSenderWorker,
    is_outsourced_only_reportable,
    ready_reportable_testids,
    ready_special_testids,
    resolve_job_report_label,
    utc_now,
    utc_iso,
)


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
        self.latest_sent_job = None

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

    def get_latest_sent_job_for_reqno(self, table, reqno):
        return self.latest_sent_job


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

    def test_report_document_url_changes_when_ready_test_set_changes(self):
        w = self.make_worker()
        job = {"id": 7, "reqid": "req-id", "reqno": "R7", "metadata": {}}
        partial = {
            "tests": [
                {"TEST_ID": "T1", "GROUPNM": "LAB", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
                {"TEST_ID": "T2", "GROUPNM": "LAB", "REPORT_STATUS": "PENDING", "APPROVEDFLG": "0"},
            ]
        }
        complete = {
            "tests": [
                {"TEST_ID": "T1", "GROUPNM": "LAB", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
                {"TEST_ID": "T2", "GROUPNM": "LAB", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
            ]
        }
        partial_url = w._build_report_document_url(job, partial)
        complete_url = w._build_report_document_url(job, complete)
        self.assertIn("reqno=R7&v=", partial_url)
        self.assertNotEqual(partial_url, complete_url)

    def test_reconcile_job_does_not_resend_unchanged_partial(self):
        w = self.make_worker()
        partial = {
            "reqno": "R8",
            "reqid": "req-id",
            "patient_name": "Patient",
            "overall_status": "PARTIAL_REPORT",
            "lab_total": 4,
            "lab_ready": 3,
            "radiology_total": 0,
            "radiology_ready": 0,
            "tests": [
                {"TEST_ID": "T1", "GROUPNM": "LAB", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1", "SAMEDAYREPORT": "1"},
                {"TEST_ID": "T2", "GROUPNM": "LAB", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1", "SAMEDAYREPORT": "1"},
                {"TEST_ID": "T3", "GROUPNM": "LAB", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1", "SAMEDAYREPORT": "1"},
                {"TEST_ID": "T4", "GROUPNM": "LAB", "REPORT_STATUS": "Awaiting approval", "APPROVEDFLG": "0", "SAMEDAYREPORT": "1"},
            ],
        }
        w.sb.latest_sent_job = {
            "last_status_snapshot": partial,
            "provider_response": {"sent_testids": ["T1", "T2", "T3"]},
        }
        w._fetch_status = lambda _job: partial
        w._partial_cutoff_due = lambda _job: (True, utc_now())
        sent = []
        w._send_report_message = lambda *args: sent.append(args)
        w.process_job({
            "id": 8,
            "reqno": "R8",
            "reqid": "req-id",
            "phone": "9989843434",
            "status": "queued",
            "force_send_now": True,
            "attempt_count": 0,
            "metadata": {"reconcile": True},
        })
        self.assertEqual(sent, [])
        self.assertTrue(any(
            event.get("event_type") == "queued_wait"
            and "Skipped duplicate send" in event.get("message", "")
            for event in w.sb.events
        ))

    def test_regular_and_special_testids_are_disjoint(self):
        status = {
            "tests": [
                {"TEST_ID": "REG", "GROUPNM": "LAB", "DEPARTMENT": "BIOCHEMISTRY", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
                {"TEST_ID": "SP", "GROUPNM": "LAB", "DEPARTMENT": "SPECIAL TESTS", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
            ]
        }
        markers = {"DPT00033", "SPECIAL TESTS"}
        self.assertEqual(ready_reportable_testids(status, markers), ["REG"])
        self.assertEqual(ready_special_testids(status, markers), ["SP"])

    def test_regular_label_uses_non_special_status_and_follow_note(self):
        status = {
            "overall_status": "PARTIAL_REPORT",
            "overall_status_excluding_special": "FULL_REPORT",
            "lab_total": 2,
            "lab_ready": 1,
            "radiology_total": 0,
            "radiology_ready": 0,
            "pending_special_tests": [{"TEST_ID": "SP", "TEST_NAME": "Special"}],
            "tests": [
                {"TEST_ID": "REG", "GROUPNM": "LAB", "DEPARTMENT": "BIOCHEMISTRY", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
                {"TEST_ID": "SP", "GROUPNM": "LAB", "DEPARTMENT": "SPECIAL TESTS", "REPORT_STATUS": "PENDING", "APPROVEDFLG": "0"},
            ],
        }
        label = resolve_job_report_label({"metadata": {}}, status, {"SPECIAL TESTS"})
        self.assertEqual(label, "complete lab; special report to follow separately")

    def test_special_only_detection_uses_department_not_old_status_literal(self):
        status = {
            "tests": [
                {"TEST_ID": "SP", "GROUPNM": "LAB", "DEPARTMENT": "SPECIAL TESTS", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"},
            ]
        }
        self.assertTrue(is_outsourced_only_reportable(status, {"SPECIAL TESTS"}))

    def test_mark_delivery_status_posts_testids(self):
        w = self.make_worker()
        calls = []

        w.http.post = lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse(text='{"ok":true}')
        result = w._mark_delivery_status(
            {"id": 5, "reqno": "R5", "metadata": {}},
            {"reqno": "R5"},
            ["T1", "T3"],
            sent_at="2026-10-06T15:42:10+05:30",
            external_event_id="wamid.test",
        )
        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls[0][0], "https://api.sdrc.in/py/delivery/status/update")
        self.assertIn('"testids": ["T1", "T3"]', calls[0][1]["data"])
        self.assertIn('"channel_code": "whatsapp"', calls[0][1]["data"])
        self.assertIn('"sent_at": "2026-10-06T15:42:10+05:30"', calls[0][1]["data"])
        self.assertIn('"external_event_id": "wamid.test"', calls[0][1]["data"])

    def test_special_delivery_status_uses_special_scope(self):
        w = self.make_worker()
        calls = []
        w.http.post = lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse(text='{"ok":true}')
        w._mark_delivery_status(
            {"id": 5, "reqno": "R5", "metadata": {"report_source": "outsourced_report"}},
            {"reqno": "R5"},
            ["SP"],
            sent_at="2026-10-06T15:42:10+05:30",
            external_event_id="wamid.special",
        )
        self.assertIn('"scope": "special"', calls[0][1]["data"])

    def test_outsourced_delivery_uses_enqueued_testids_not_later_ready_tests(self):
        w = self.make_worker()
        job = {
            "metadata": {
                "report_source": "outsourced_report",
                "outsourced_testids": ["SP-SELECTED"],
            }
        }
        status = {
            "tests": [
                {"TEST_ID": "SP-SELECTED", "DEPARTMENT": "SPECIAL TESTS", "APPROVEDFLG": "1"},
                {"TEST_ID": "SP-LATER", "DEPARTMENT": "SPECIAL TESTS", "APPROVEDFLG": "1"},
            ]
        }
        self.assertEqual(w._sent_testids_for_job(job, status), ["SP-SELECTED"])

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


class CancelledTestsDoNotHoldSameDayWaitTests(unittest.TestCase):
    def test_cancelled_same_day_test_is_not_required(self):
        import report_sender_worker as m
        cancelled = {"SAMEDAYREPORT": "1", "REPORT_STATUS": "cancelled", "APPROVEDFLG": "0"}
        pending = {"SAMEDAYREPORT": "1", "REPORT_STATUS": "Awaiting results", "APPROVEDFLG": "0"}
        self.assertFalse(m.is_same_day_required(cancelled))
        self.assertTrue(m.is_same_day_required(pending))
        ready = {"SAMEDAYREPORT": "1", "REPORT_STATUS": "Printed / dispatched", "APPROVEDFLG": "1"}
        status = {"tests": [ready, cancelled]}
        self.assertTrue(m.evaluate_same_day_readiness(status))
        total, ready_n, pending_names = m.same_day_counts_and_pending(status)
        self.assertEqual((total, ready_n, pending_names), (1, 1, []))


class DirectSendModalityTests(unittest.TestCase):
    """Tests a device worker delivers itself (enqueue.direct_send_modalities, default empty)."""

    def setUp(self):
        import report_sender_worker as m
        self.m = m
        m.configure_direct_send({"enqueue": {"direct_send_modalities": ["ECG"]}})

    def tearDown(self):
        self.m.configure_direct_send({})

    @staticmethod
    def ecg(**extra):
        row = {"GROUPID": "GDEP0002", "GROUPNM": "RADIOLOGY", "TEST_NAME": "ECG", "IMAGING_MODALITY": "ecg",
               "SAMEDAYREPORT": "1", "REPORT_STATUS": "Awaiting report", "APPROVEDFLG": "0"}
        row.update(extra)
        return row

    def test_default_is_empty_so_nothing_is_special_cased(self):
        self.assertEqual(self.m.direct_send_modalities({}), set())
        self.m.configure_direct_send({})
        self.assertTrue(self.m.is_lab_or_radiology_test(self.ecg()))

    def test_configured_modality_is_not_a_reportable_test(self):
        self.assertFalse(self.m.is_lab_or_radiology_test(self.ecg()))
        xray = self.ecg(IMAGING_MODALITY="xray", TEST_NAME="X-Ray")
        self.assertTrue(self.m.is_lab_or_radiology_test(xray))
        self.assertTrue(self.m.is_lab_or_radiology_test({"GROUPID": "GDEP0001", "TEST_NAME": "CBC"}))

    def test_ecg_only_requisition_has_nothing_to_report(self):
        status = {"tests": [self.ecg()]}
        self.assertFalse(self.m.has_any_reportable_test(status))

    def test_pending_ecg_does_not_hold_the_same_day_wait(self):
        lab = {"GROUPID": "GDEP0001", "TEST_NAME": "CBC", "SAMEDAYREPORT": "1", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1"}
        status = {"tests": [lab, self.ecg()]}
        self.assertTrue(self.m.evaluate_same_day_readiness(status))

    def test_label_ignores_the_direct_send_test_in_radiology_counts(self):
        status = {"overall_status": "FULL_REPORT", "radiology_total": 1, "radiology_ready": 0, "tests": [self.ecg()]}
        self.assertEqual(self.m.build_template_report_label(status), "complete lab")

    def test_env_var_overrides_config(self):
        import os
        os.environ["DIRECT_SEND_TEST_MODALITIES"] = "bmd, ecg"
        try:
            self.assertEqual(self.m.direct_send_modalities({"enqueue": {"direct_send_modalities": ["x"]}}), {"bmd", "ecg"})
        finally:
            del os.environ["DIRECT_SEND_TEST_MODALITIES"]


class StuckWaitTerminalDaysTests(unittest.TestCase):
    """A job waiting for results must not be failed for good just because it waited (cultures, biopsy)."""

    def run_watchdog(self, terminal_days, age_days, stuck_count):
        from datetime import datetime, timedelta, timezone
        from unittest.mock import MagicMock
        import report_sender_worker as m
        now = datetime.now(timezone.utc)
        worker = m.ReportSenderWorker.__new__(m.ReportSenderWorker)
        worker.cfg = {"tables": {"jobs": "jobs", "events": "events"},
                      "worker": {"stuck_wait_terminal_days": terminal_days, "stuck_max_auto_requeues": 3,
                                 "stuck_queued_wait_hours": 6, "stuck_cooling_off_hours": 7}}
        job = {"id": 1, "reqno": "R1", "status": "queued", "metadata": {"stuck_requeue_count": stuck_count},
               "created_at": (now - timedelta(days=age_days)).isoformat()}
        worker.sb = MagicMock()
        worker.sb.list_watchdog_candidates.return_value = [job]
        worker.sb.list_stale_inflight.return_value = []
        worker.sb.get_latest_event.return_value = {
            "event_type": "queued_wait", "created_at": (now - timedelta(hours=30)).isoformat()}
        worker.metrics = {"stuck_detected": 0, "auto_requeued": 0, "failed_timeout": 0}
        worker.log = MagicMock()
        patches = []
        worker._patch_job = lambda j, patch: patches.append(patch)
        worker._event = lambda *a, **k: None
        worker._watchdog_stuck_jobs()
        return patches

    def test_default_keeps_the_legacy_terminal_failure(self):
        patches = self.run_watchdog(0, 3, 3)
        self.assertEqual(patches[-1]["status"], "failed")
        self.assertEqual(patches[-1]["last_error"], "failed_timeout_stuck")

    def test_young_waiting_job_stays_queued_when_configured(self):
        patches = self.run_watchdog(14, 3, 3)
        self.assertEqual(patches[-1]["status"], "queued")
        self.assertNotIn("failed", [p.get("status") for p in patches])

    def test_job_older_than_the_limit_still_fails(self):
        patches = self.run_watchdog(14, 20, 3)
        self.assertEqual(patches[-1]["status"], "failed")

    def test_requeue_below_the_cap_is_unchanged(self):
        patches = self.run_watchdog(14, 3, 1)
        self.assertEqual(patches[-1]["status"], "queued")
        self.assertEqual(patches[-1]["metadata"]["stuck_requeue_count"], 2)

if __name__ == "__main__":
    unittest.main()
