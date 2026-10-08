import unittest

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

if __name__ == "__main__":
    unittest.main()
