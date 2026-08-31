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


if __name__ == "__main__":
    unittest.main()
