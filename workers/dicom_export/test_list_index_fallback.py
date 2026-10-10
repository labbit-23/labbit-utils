"""Regression tests for the table-first, Orthanc-completing LIST path."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
import main


class FakeIndex:
    def __init__(self):
        self.saved = []

    def record_from_api_row(self, row, source="orthanc"):
        return {"orthanc_study_id": row["studyId"], "source": source}

    def upsert_rows(self, rows):
        self.saved.extend(rows)
        return True


class ListFallbackTests(unittest.TestCase):
    def test_orthanc_only_study_is_added_without_replacing_delivery_state(self):
        indexed = [{
            "studyId": "sent-study",
            "accession": "R202610100001",
            "status": "SENT",
            "source": "primary",
        }]
        study_rows = [
            {"ID": "sent-study", "RequestedTags": {"ModalitiesInStudy": "CR"}},
            {"ID": "pending-study", "RequestedTags": {"ModalitiesInStudy": "CT"}},
        ]
        live_rows = {
            "pending-study": {
                "studyId": "pending-study",
                "accession": "R202610100002",
                "status": "",
            }
        }
        index = FakeIndex()

        with patch.object(
            main,
            "_resolve_studies_for_date",
            return_value=(object(), "primary", study_rows),
        ), patch.object(
            main,
            "_fetch_study_row",
            side_effect=lambda _client, item: live_rows[item["ID"]],
        ):
            result = main._complete_index_rows_from_orthanc(
                index,
                object(),
                None,
                "20261010",
                indexed,
            )

        self.assertEqual(
            [row["studyId"] for row in result],
            ["pending-study", "sent-study"],
        )
        self.assertEqual(result[1]["status"], "SENT")
        self.assertEqual(index.saved, [{
            "orthanc_study_id": "pending-study",
            "source": "primary",
        }])

    def test_modality_filter_does_not_add_other_modalities(self):
        study_rows = [
            {"ID": "cr-study", "RequestedTags": {"ModalitiesInStudy": "CR"}},
            {"ID": "ct-study", "RequestedTags": {"ModalitiesInStudy": "CT"}},
        ]
        with patch.object(
            main,
            "_resolve_studies_for_date",
            return_value=(object(), "primary", study_rows),
        ), patch.object(
            main,
            "_fetch_study_row",
            side_effect=lambda _client, item: {
                "studyId": item["ID"],
                "accession": item["ID"],
                "status": "",
            },
        ):
            result = main._complete_index_rows_from_orthanc(
                FakeIndex(),
                object(),
                None,
                "20261010",
                [],
                "CT",
            )

        self.assertEqual([row["studyId"] for row in result], ["ct-study"])


if __name__ == "__main__":
    unittest.main()
