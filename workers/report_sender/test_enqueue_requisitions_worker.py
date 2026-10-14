import unittest
from unittest.mock import MagicMock

from enqueue_requisitions_worker import EnqueueWorker


def base_cfg():
    return {
        "supabase": {"url": "https://example.supabase.co", "service_role_key": "k"},
        "tables": {"jobs": "report_auto_dispatch_jobs", "events": "report_auto_dispatch_events"},
        "labbit_py": {"base_url": "https://api.sdrc.in/py", "status_mode": "reqid"},
        "shivam": {"requisitions_url": "https://api.sdrc.in/py/delivery/requisitions-by-date/{date}", "method": "GET"},
        "whatsapp": {"lab_id": "lab"},
        "worker": {"cooloff_minutes_default": 30, "max_attempts": 5},
        "enqueue": {"request_timeout_seconds": 20},
    }


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"overall_status": "FULL_REPORT", "tests": []}


class EnqueueWorkerTests(unittest.TestCase):
    def make_worker(self, extra_enqueue=None):
        cfg = base_cfg()
        cfg["enqueue"].update(extra_enqueue or {})
        return EnqueueWorker(cfg, dry_run=True)

    def test_archive_reqid_is_not_stripped_by_default(self):
        worker = self.make_worker()
        calls = []
        worker.http.get = lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse()

        worker._fetch_status(reqno="", reqid="archive:P1611609")

        self.assertEqual(calls[0][0], "https://api.sdrc.in/py/report-status-reqid/archive:P1611609")

    def test_archive_reqid_strip_is_config_gated(self):
        worker = self.make_worker({"strip_archive_reqid_for_status": True})
        calls = []
        worker.http.get = lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse()

        worker._fetch_status(reqno="", reqid="archive:P1611609")

        self.assertEqual(calls[0][0], "https://api.sdrc.in/py/report-status-reqid/P1611609")

    def test_source_tagging_is_config_gated(self):
        worker = self.make_worker({"source_backend": "labit_core"})
        self.assertEqual(worker._source_metadata({}), {})

        worker = self.make_worker({"tag_job_origin": True, "source_backend": "labit_core"})
        self.assertEqual(
            worker._source_metadata({}),
            {"source_backend": "labit_core", "report_origin": "labit_core"},
        )

    def test_source_tagging_honors_row_source_when_enabled(self):
        worker = self.make_worker({"tag_job_origin": True, "source_backend": "labit_core"})

        self.assertEqual(
            worker._source_metadata({"source": "shivam_archive"}),
            {"source_backend": "shivam_archive", "report_origin": "shivam_archive"},
        )

    def test_full_ready_for_regular_bundle_ignores_pending_special_only(self):
        worker = self.make_worker()
        self.assertTrue(worker._is_overall_full_ready({
            "overall_status": "PARTIAL_REPORT",
            "overall_status_excluding_special": "FULL_REPORT",
        }))

    def test_sent_regular_job_does_not_suppress_new_special_bundle(self):
        worker = EnqueueWorker(base_cfg(), dry_run=False)

        class FakeSB:
            def __init__(self):
                self.inserted = []

            def latest_job(self, table, reqno):
                return {"id": "regular", "status": "sent", "metadata": {}}

            def dispatched_exists(self, reqno, phone):
                return True

            def list_jobs_by_reqno(self, table, reqno, limit=300):
                return [{"id": "regular", "status": "sent", "metadata": {}}]

            def insert_job(self, table, job):
                self.inserted.append(job)

        worker.sb = FakeSB()
        worker._can_start = lambda: True
        worker._today_ist = lambda: "2026-10-04"
        worker._fetch_rows = lambda: [{
            "REQNO": "R1", "REQID": "REQ1", "PHONENO": "9849025601",
            "PATIENTNM": "Patient", "MRNO": "M1",
        }]
        worker._fetch_status = lambda **kwargs: {
            "tests": [{
                "TEST_ID": "SP", "GROUPNM": "LAB", "DEPARTMENT": "SPECIAL TESTS",
                "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1",
            }]
        }
        worker._should_skip_invalid_phone_reenqueue = lambda *args: False
        worker._reconcile_recent = lambda *args: None
        worker._reconcile_outsourced_jobs = lambda *args: 0
        worker._expire_deferred_jobs = lambda *args: None

        worker.run_once()

        self.assertEqual(len(worker.sb.inserted), 1)
        self.assertEqual(worker.sb.inserted[0]["metadata"]["report_source"], "outsourced_report")


class PatientMessageJobsTests(unittest.TestCase):
    def make_worker(self, jobs, max_per_cycle=2, skip_first_n=0, skip_first_n_date=None):
        cfg = base_cfg()
        cfg["whatsapp"]["internal_send_url"] = "https://lab.sdrc.in/api/internal/whatsapp/report-template-send"
        cfg["patient_message_jobs_send_url"] = "https://lab.sdrc.in/api/internal/whatsapp/campaign-send"
        cfg["patient_message_jobs"] = [
            {
                "key": "requisition_welcome",
                "enabled": True,
                "window": {"start_hhmm": 0, "end_hhmm": 2359},
                "max_per_cycle": max_per_cycle,
                "skip_first_n": skip_first_n,
                "skip_first_n_date": skip_first_n_date,
                "trigger": {"source_url": "https://api.sdrc.in/py/delivery/requisitions-by-date/{today}", "rows_path": "requisitions"},
            }
        ]
        worker = EnqueueWorker(cfg, dry_run=False)
        worker.http.get = lambda url, **kwargs: FakeJsonResponse({"requisitions": jobs})
        return worker

    def test_pre_slice_would_starve_rows_past_cap(self):
        # Reproduces the 2026-09-13 incident: once the first `cap` rows of
        # the day are already sent, a pre-sliced rows[:cap] window never
        # reaches any row beyond position `cap`, no matter how many cycles
        # run. rows[0] and rows[1] are already sent (skipped); rows[2] is
        # new and must still be reached and sent within the same cycle.
        rows = [
            {"reqno": "R1", "reqid": "r1"},
            {"reqno": "R2", "reqid": "r2"},
            {"reqno": "R3", "reqid": "r3"},
        ]
        worker = self.make_worker(rows, max_per_cycle=2)

        posted = []

        def fake_post(url, headers=None, data=None, timeout=None):
            import json as _json
            payload = _json.loads(data)
            reqid = payload["context"]["reqid"]
            posted.append(reqid)
            if reqid in {"r1", "r2"}:
                return FakeJsonResponse({"ok": True, "skipped": True, "reason": "already_sent"})
            return FakeJsonResponse({"ok": True, "sent": True, "provider_message_id": "wamid.1"})

        worker.http.post = fake_post
        worker.run_patient_message_jobs_once()

        # All three rows must be considered (already_sent skips are cheap
        # and don't count against the cap), and the genuinely new row (r3)
        # must actually be sent within this cycle.
        self.assertEqual(posted, ["r1", "r2", "r3"])

    def test_cap_still_limits_real_send_attempts(self):
        rows = [
            {"reqno": "R1", "reqid": "r1"},
            {"reqno": "R2", "reqid": "r2"},
            {"reqno": "R3", "reqid": "r3"},
        ]
        worker = self.make_worker(rows, max_per_cycle=1)

        posted = []

        def fake_post(url, headers=None, data=None, timeout=None):
            import json as _json
            payload = _json.loads(data)
            reqid = payload["context"]["reqid"]
            posted.append(reqid)
            return FakeJsonResponse({"ok": True, "sent": True, "provider_message_id": "wamid.1"})

        worker.http.post = fake_post
        worker.run_patient_message_jobs_once()

        # cap=1 real send attempt per cycle -- stops after the first genuine send.
        self.assertEqual(posted, ["r1"])

    def test_skip_first_n_excludes_backlog_from_todays_run(self):
        # One-time 2026-09-24 cutover: rows[0:2] are today's already-stranded
        # backlog (deliberately left unsent, not retroactively caught up);
        # only r3 (arrived after the cutover) should be attempted.
        rows = [
            {"reqno": "R1", "reqid": "r1"},
            {"reqno": "R2", "reqid": "r2"},
            {"reqno": "R3", "reqid": "r3"},
        ]
        worker = self.make_worker(rows, max_per_cycle=60, skip_first_n=2, skip_first_n_date="2026-09-24")
        worker._today_ist = lambda: "2026-09-24"

        posted = []

        def fake_post(url, headers=None, data=None, timeout=None):
            import json as _json
            payload = _json.loads(data)
            posted.append(payload["context"]["reqid"])
            return FakeJsonResponse({"ok": True, "sent": True, "provider_message_id": "wamid.1"})

        worker.http.post = fake_post
        worker.run_patient_message_jobs_once()

        self.assertEqual(posted, ["r3"])

    def test_skip_first_n_is_a_noop_on_a_later_day(self):
        # Simulates the next day: skip_first_n_date no longer matches today,
        # so a leftover skip_first_n from a prior day's cutover has no effect
        # even though the (short) new list is well under that count.
        rows = [{"reqno": "R1", "reqid": "r1"}]
        worker = self.make_worker(rows, max_per_cycle=60, skip_first_n=100, skip_first_n_date="2026-09-24")
        worker._today_ist = lambda: "2026-09-25"

        posted = []
        worker.http.post = lambda url, headers=None, data=None, timeout=None: (
            posted.append(__import__("json").loads(data)["context"]["reqid"])
            or FakeJsonResponse({"ok": True, "sent": True, "provider_message_id": "wamid.1"})
        )
        worker.run_patient_message_jobs_once()

        self.assertEqual(posted, ["r1"])


class FakeJsonResponse:
    def __init__(self, payload):
        self._payload = payload
        self.text = "x"
        self.ok = True
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


    def test_special_skip_if_any_sent_flag_suppresses_duplicate_special_job(self):
        class FakeSB:
            def __init__(self, rows):
                self.rows, self.inserted = rows, []

            def list_jobs_by_reqno(self, table, reqno, limit=300):
                return self.rows

            def insert_job(self, table, job):
                self.inserted.append(job)

        def run(flag, rows):
            cfg = base_cfg()
            cfg["enqueue"]["special_skip_if_any_sent"] = flag
            worker = EnqueueWorker(cfg, dry_run=False)
            worker.sb = FakeSB(rows)
            ok = worker._maybe_enqueue_special_job(
                "report_auto_dispatch_jobs", row={}, reqno="R1", reqid="REQ1", phone="9849025601", mrno="M1", name="P",
                ready_testids=["SP"], lab_id="lab", paused_default=False, cooloff=30, max_attempts=5, reason="t")
            return ok, worker.sb.inserted

        sent_regular = [{"id": "a", "status": "sent", "report_label": "complete lab", "metadata": {}}]
        self.assertEqual(run(True, sent_regular)[0], False)       # flag on + something already sent: no duplicate special job
        self.assertEqual(run(False, sent_regular)[0], True)       # flag off (default): designed behaviour unchanged
        self.assertEqual(run(True, [])[0], True)                  # flag on but nothing sent yet: the special job is created
        self.assertEqual(run(True, [{"id": "f", "status": "failed", "metadata": {}}])[0], True)  # failed sends do not count

    def test_special_legacy_cutoff_only_counts_sends_before_the_cutover(self):
        class FakeSB:
            def __init__(self, rows):
                self.rows = rows

            def list_jobs_by_reqno(self, table, reqno, limit=300):
                return self.rows

            def insert_job(self, table, job):
                pass

        def run(cutoff, rows):
            cfg = base_cfg()
            cfg["enqueue"]["special_skip_if_any_sent"] = True
            if cutoff:
                cfg["enqueue"]["special_legacy_cutoff"] = cutoff
            worker = EnqueueWorker(cfg, dry_run=False)
            worker.sb = FakeSB(rows)
            return worker._maybe_enqueue_special_job(
                "report_auto_dispatch_jobs", row={}, reqno="R1", reqid="REQ1", phone="9849025601", mrno="M1", name="P",
                ready_testids=["SP"], lab_id="lab", paused_default=False, cooloff=30, max_attempts=5, reason="t")

        before = [{"id": "a", "status": "sent", "sent_at": "2026-10-10T08:00:00+00:00", "metadata": {}}]
        after = [{"id": "b", "status": "sent", "sent_at": "2026-10-10T10:00:00+00:00", "metadata": {}}]
        undated = [{"id": "c", "status": "sent", "metadata": {}}]
        cutoff = "2026-10-10T09:00:00+00:00"
        self.assertEqual(run(cutoff, before), False)   # sent before the cutover: that bundle carried the special tests -> no duplicate
        self.assertEqual(run(cutoff, after), True)     # sent after: regular bundle excludes special tests -> the special job is still owed
        self.assertEqual(run(cutoff, undated), False)  # no sent_at: cannot prove it is post-cutover -> treated as legacy
        self.assertEqual(run(None, after), False)      # no cutoff configured: original behaviour, any sent job suppresses
        self.assertEqual(run("not-a-date", after), False)  # unparseable cutoff is ignored (original behaviour), not a crash

# ---------------------------------------------------------------------------
# Incremental follow-ups (director's rule, 2026-10-10)
# ---------------------------------------------------------------------------
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock


def _t(test_id, same_day, ready, group="LAB", report_status=None):
    return {
        "TEST_ID": test_id,
        "TEST_NAME": test_id,
        "GROUPNM": group,
        "SAMEDAYREPORT": "1" if same_day else "0",
        "APPROVEDFLG": "1" if ready else "0",
        "REPORT_STATUS": report_status or ("Printed / dispatched" if ready else "Awaiting results"),
    }


def _status(*rows, overall="PARTIAL_REPORT"):
    return {"overall_status": overall, "tests": list(rows)}


def _visit(**ready):
    """Haemogram, ESR, HIV, Creatinine = same-day; Urine/Blood culture = not."""
    same = {"CREAT", "HAEMO", "ESR", "HIV"}
    ids = ["CREAT", "HAEMO", "ESR", "HIV", "URINE_CULT", "BLOOD_CULT"]
    return _status(*[_t(i, i in same, ready.get(i, False)) for i in ids])


class IncrementalFollowupDecisionTests(unittest.TestCase):
    def setUp(self):
        self.w = EnqueueWorker(base_cfg(), dry_run=True)

    def test_directors_walkthrough(self):
        # Evening day 0: Creatinine ready, partial sent.
        s0 = _visit(CREAT=True)
        # Day 1, 11:00: Haemogram + ESR ready, HIV still pending -> follow-up job is
        # created (the sender's same-day gate holds it until HIV is ready: ONE send).
        d1 = self.w._followup_decision(s0, _visit(CREAT=True, HAEMO=True, ESR=True))
        self.assertTrue(d1["enqueue"])
        self.assertEqual(d1["new_ready"], ["ESR", "HAEMO"])
        self.assertEqual(d1["pending_same_day"], ["HIV"])
        self.assertFalse(d1["complete_after"])
        # Day 1, 13:00: HIV ready -> the (single, cumulative) send goes out; snapshot s1.
        s1 = _visit(CREAT=True, HAEMO=True, ESR=True, HIV=True)
        # Day 2: Urine culture approved -> its own follow-up (non-same-day).
        d2 = self.w._followup_decision(s1, _visit(CREAT=True, HAEMO=True, ESR=True, HIV=True, URINE_CULT=True))
        self.assertTrue(d2["enqueue"])
        self.assertEqual(d2["new_non_same_day"], ["URINE_CULT"])
        self.assertEqual(d2["new_same_day"], [])
        self.assertEqual(d2["pending_same_day"], [])
        self.assertFalse(d2["complete_after"])
        # Day 5: Blood culture approved -> final, full bundle.
        s2 = _visit(CREAT=True, HAEMO=True, ESR=True, HIV=True, URINE_CULT=True)
        d3 = self.w._followup_decision(s2, _visit(CREAT=True, HAEMO=True, ESR=True, HIV=True, URINE_CULT=True, BLOOD_CULT=True))
        self.assertTrue(d3["enqueue"])
        self.assertEqual(d3["new_ready"], ["BLOOD_CULT"])
        self.assertTrue(d3["complete_after"])

    def test_nothing_new_means_no_followup(self):
        s = _visit(CREAT=True, HAEMO=True)
        d = self.w._followup_decision(s, _visit(CREAT=True, HAEMO=True))
        self.assertIs(d["enqueue"], False)

    def test_cancelled_tests_are_not_pending_and_do_not_block_complete(self):
        prev = _status(_t("A", True, True), _t("RAD", False, False, group="RADIOLOGY", report_status="Awaiting report"))
        live = _status(
            _t("A", True, True), _t("B", False, True),
            _t("RAD", False, False, group="RADIOLOGY", report_status="cancelled"),
        )
        d = self.w._followup_decision(prev, live)
        self.assertTrue(d["enqueue"])
        self.assertTrue(d["complete_after"])  # the cancelled radiology test is ignored

    def test_cancelled_test_never_counts_as_new_ready(self):
        prev = _status(_t("A", True, True))
        live = _status(_t("A", True, True), _t("C", False, False, report_status="cancelled"))
        self.assertIs(self.w._followup_decision(prev, live)["enqueue"], False)

    def test_outsourced_tests_are_left_to_the_special_path(self):
        prev = _status(_t("A", True, True))
        sp = _t("SP", False, True)
        sp["REPORT_STATUS"] = "OUTSOURCED"
        live = _status(_t("A", True, True), sp)
        self.assertIs(self.w._followup_decision(prev, live)["enqueue"], False)

    def test_old_snapshot_without_tests_falls_back_to_legacy_rule(self):
        d = self.w._followup_decision({"overall_status": "PARTIAL_REPORT"}, _visit(CREAT=True))
        self.assertIsNone(d["enqueue"])
        d = self.w._followup_decision({}, _visit(CREAT=True))
        self.assertIsNone(d["enqueue"])


class ReconcileModeTests(unittest.TestCase):
    PHONE = "919999999999"

    def make(self, mode, age_days=2):
        cfg = base_cfg()
        cfg["enqueue"].update({"lookback_hours": 150, "lookback_max_rows": 500, "incremental_followups": mode})
        w = EnqueueWorker(cfg, dry_run=False)
        sent = {
            "id": 1, "reqno": "R1", "reqid": "RQ1", "phone": self.PHONE, "status": "sent",
            "report_label": "partial lab", "last_status_snapshot": _visit(CREAT=True),
        }
        created = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
        sb = MagicMock()
        sb.list_recent_jobs.return_value = [sent]
        sb.has_active_job.return_value = False
        sb.has_sent_non_full.return_value = True
        sb.latest_sent_snapshot.return_value = {"last_status_snapshot": _visit(CREAT=True)}
        sb.list_jobs_by_reqno.return_value = [{"id": 1, "status": "sent", "created_at": created, "phone": self.PHONE}]
        w.sb = sb
        # Live: Haemogram + ESR newly ready, still PARTIAL overall (HIV and cultures pending).
        w._fetch_status = lambda reqno, reqid: _visit(CREAT=True, HAEMO=True, ESR=True)
        return w, sb

    def test_off_keeps_the_legacy_full_only_rule(self):
        w, sb = self.make("off")
        self.assertEqual(w._reconcile_recent("jobs"), 0)
        sb.insert_job.assert_not_called()

    def test_preview_logs_but_never_inserts(self):
        w, sb = self.make("preview")
        with self.assertLogs(w.log, level="INFO") as logs:
            self.assertEqual(w._reconcile_recent("jobs"), 0)
        sb.insert_job.assert_not_called()
        self.assertTrue(any("[followup-preview]" in m and "R1" in m for m in logs.output))

    def test_on_enqueues_one_cumulative_followup_with_context(self):
        w, sb = self.make("on")
        self.assertEqual(w._reconcile_recent("jobs"), 1)
        job = sb.insert_job.call_args[0][1]
        self.assertEqual(job["reqno"], "R1")
        self.assertTrue(job["metadata"]["reconcile"])
        self.assertEqual(job["metadata"]["reason"], "incremental_new_ready_tests")
        self.assertEqual(job["metadata"]["new_ready_testids"], ["ESR", "HAEMO"])
        self.assertEqual(job["metadata"]["pending_same_day_testids"], ["HIV"])

    def test_on_respects_the_max_age_window(self):
        w, sb = self.make("on", age_days=20)
        self.assertEqual(w._reconcile_recent("jobs"), 0)
        sb.insert_job.assert_not_called()

    def test_on_rechecks_a_requisition_only_every_interval(self):
        w, sb = self.make("on")
        w.cfg["enqueue"]["followup_check_interval_minutes"] = 15
        self.assertEqual(w._reconcile_recent("jobs"), 1)
        sb.has_active_job.return_value = False
        self.assertEqual(w._reconcile_recent("jobs"), 0)  # checked moments ago -> skipped

class SpecialFollowupAfterRegularSendTests(unittest.TestCase):
    """R202610110047 (2026-10-14): the regular job went out as 'complete lab; special
    report to follow separately' while the SPECIAL TESTS test was still pending; the
    snapshot is frozen at send time, so a later approval was never followed up."""

    @staticmethod
    def _status(special_ready):
        return {"tests": [
            {"TEST_ID": "T1", "TEST_NAME": "CBC", "GROUPID": "GDEP0001", "REPORT_STATUS": "LAB_READY", "APPROVEDFLG": "1",
             "DEPARTMENT": "HAEMATOLOGY"},
            {"TEST_ID": "T2", "TEST_NAME": "CA 15.3", "GROUPID": "GDEP0001", "DEPARTMENT": "SPECIAL TESTS",
             "REPORT_STATUS": "LAB_READY" if special_ready else "Awaiting results",
             "APPROVEDFLG": "1" if special_ready else "0"},
        ]}

    def make(self, live_special_ready=True, snapshot=None):
        cfg = base_cfg()
        cfg["enqueue"].update({"outsourced_lookback_hours": 200})
        w = EnqueueWorker(cfg, dry_run=False)
        sent = {"id": 7, "reqno": "R1", "reqid": "RQ1", "phone": "919999999999", "mrno": "M", "patient_name": "P",
                "metadata": {}, "last_status_snapshot": snapshot if snapshot is not None else self._status(False)}
        sb = MagicMock()
        sb.list_recent_sent_regular_jobs.return_value = [sent]
        sb.list_recent_failed_regular_jobs.return_value = []
        sb.has_active_job.return_value = False
        sb.list_jobs_by_reqno.return_value = []
        w.sb = sb
        w._fetch_status = lambda reqno, reqid: self._status(live_special_ready)
        return w, sb

    def test_special_approved_after_regular_send_is_followed_up(self):
        w, sb = self.make(live_special_ready=True)
        self.assertEqual(w._reconcile_outsourced_jobs("jobs"), 1)
        self.assertEqual(sb.insert_job.call_count, 1)

    def test_special_still_pending_does_not_enqueue(self):
        w, sb = self.make(live_special_ready=False)
        self.assertEqual(w._reconcile_outsourced_jobs("jobs"), 0)
        sb.insert_job.assert_not_called()

    def test_snapshot_without_any_special_test_is_not_refetched(self):
        snap = {"tests": [self._status(False)["tests"][0]]}
        w, sb = self.make(live_special_ready=True, snapshot=snap)
        calls = []
        w._fetch_status = lambda reqno, reqid: calls.append(reqno) or self._status(True)
        self.assertEqual(w._reconcile_outsourced_jobs("jobs"), 0)
        self.assertEqual(calls, [])


class LegacyCutoffUsesSentAtTests(unittest.TestCase):
    def worker(self, rows):
        cfg = base_cfg()
        cfg["enqueue"].update({"special_skip_if_any_sent": True, "special_legacy_cutoff": "2026-10-10T06:29:26+00:00"})
        w = EnqueueWorker(cfg, dry_run=True)
        w.sb = MagicMock()
        w.sb.list_jobs_by_reqno.return_value = rows
        return w

    def test_send_after_cutoff_does_not_suppress_special_followup(self):
        w = self.worker([{"status": "sent", "sent_at": "2026-10-11T08:01:24+00:00"}])
        self.assertFalse(w._has_any_sent_job("jobs", "R1"))

    def test_send_before_cutoff_still_suppresses(self):
        w = self.worker([{"status": "sent", "sent_at": "2026-10-09T08:01:24+00:00"}])
        self.assertTrue(w._has_any_sent_job("jobs", "R1"))

    def test_jobs_by_reqno_selects_sent_at(self):
        import enqueue_requisitions_worker as m
        sb = m.SupabaseRest.__new__(m.SupabaseRest)
        sb.base, sb.headers, sb.timeout = "http://x", {}, 1
        seen = {}
        class R:
            def raise_for_status(self): pass
            def json(self): return []
        class H:
            def get(self, u, headers=None, params=None, timeout=None):
                seen.update(params); return R()
        sb.http = H()
        sb.list_jobs_by_reqno("jobs", "R1")
        self.assertIn("sent_at", seen["select"].split(","))


class DirectSendModalityEnqueueTests(unittest.TestCase):
    def make(self, modalities):
        cfg = base_cfg()
        cfg["enqueue"]["direct_send_modalities"] = modalities
        return EnqueueWorker(cfg, dry_run=True)

    ECG = {"GROUPID": "GDEP0002", "GROUPNM": "RADIOLOGY", "TEST_NAME": "ECG", "IMAGING_MODALITY": "ecg"}

    def test_ecg_only_requisition_has_no_reportable_tests(self):
        w = self.make(["ecg"])
        self.assertFalse(w._has_any_reportable_tests({"tests": [self.ECG]}))

    def test_without_config_the_ecg_is_an_ordinary_radiology_test(self):
        w = self.make([])
        self.assertTrue(w._has_any_reportable_tests({"tests": [self.ECG]}))

    def test_other_tests_stay_reportable_next_to_an_ecg(self):
        w = self.make(["ecg"])
        lab = {"GROUPID": "GDEP0001", "GROUPNM": "LAB", "TEST_NAME": "CBC"}
        self.assertTrue(w._has_any_reportable_tests({"tests": [self.ECG, lab]}))


class UnjobbedLookbackTests(unittest.TestCase):
    """R202610110051: a special-only requisition got no job on its own day and was never looked at again."""

    def make(self, days, have, rows_by_day):
        cfg = base_cfg()
        cfg["enqueue"]["unjobbed_lookback_days"] = days
        w = EnqueueWorker(cfg, dry_run=True)
        sb = MagicMock()
        sb.list_reqnos_with_any_job.return_value = set(have)
        w.sb = sb
        w._fetch_rows = lambda day=None: rows_by_day.get(day, [])
        return w

    def test_off_by_default(self):
        w = self.make(0, [], {})
        self.assertEqual(w._unjobbed_rows_from_previous_days("jobs"), [])

    def test_returns_only_requisitions_without_any_job(self):
        from enqueue_requisitions_worker import now_ist
        from datetime import timedelta
        d1 = (now_ist().date() - timedelta(days=1)).strftime("%Y-%m-%d")
        rows = {d1: [{"reqno": "R1"}, {"reqno": "R2"}, {"reqno": ""}]}
        w = self.make(2, ["R1"], rows)
        out = w._unjobbed_rows_from_previous_days("jobs")
        self.assertEqual([r["reqno"] for r in out], ["R2"])

    def test_job_list_failure_never_raises(self):
        w = self.make(2, [], {})
        w.sb.list_reqnos_with_any_job.side_effect = RuntimeError("boom")
        self.assertEqual(w._unjobbed_rows_from_previous_days("jobs"), [])

if __name__ == "__main__":
    unittest.main()
