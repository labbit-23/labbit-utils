import unittest

from manual_send import detect_quality_flag, quality_flag_for_row


class QualityGateTests(unittest.TestCase):
    def test_detects_lead_reversal_from_diagnosis(self):
        text = "Limb Lead Reversal Suspected, Sinus Rhythm."
        self.assertEqual(detect_quality_flag(text), text)

    def test_detects_incorrect_chest_placement(self):
        text = "Sinus Rhythm, Incorrect Chest Lead Placement Suspected"
        self.assertEqual(detect_quality_flag(text), text)

    def test_detects_poor_quality(self):
        text = "Poor Quality ECG"
        self.assertEqual(detect_quality_flag(text), text)

    def test_normal_ecg_is_not_flagged(self):
        self.assertIsNone(detect_quality_flag("Sinus Rhythm"))

    def test_derives_flag_when_only_raw_diagnosis_has_it(self):
        text = "Limb Lead Reversal Suspected"
        row = {"raw_json": {"diagnosis": text}}
        self.assertEqual(quality_flag_for_row(row), text)


if __name__ == "__main__":
    unittest.main()
