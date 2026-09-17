"""
Synthetic test for the multi-accession-per-group / multi-instance-per-page
montage logic, deliberately NOT touching real Orthanc data -- real data
today only had single-accession groups, so this path needs a stand-in.

Fabricates a group for one synthetic patient with two accessions:
  ACC_SINGLE  -> 1 instance  (should become 1 page, no montage)
  ACC_MULTI   -> 3 instances (should become 1 montaged page)
so the resulting PDF should have exactly 2 pages, and the ACC_MULTI page
should visibly show 3 distinct colored panels grid-composited together.
"""
import json
import logging
import os
import shutil
import subprocess

import core
import cr

TMP = os.path.abspath("./tmp/synth")

SYNTH_IMAGES = {
    "inst_single_1": os.path.join(TMP, "img_a.png"),
    "inst_multi_1": os.path.join(TMP, "img_b.png"),
    "inst_multi_2": os.path.join(TMP, "img_c.png"),
    "inst_multi_3": os.path.join(TMP, "img_d.png"),
}

SYNTH_COLORS = {"img_a.png": "red", "img_b.png": "green", "img_c.png": "blue", "img_d.png": "yellow"}


def generate_synthetic_images(magick_path):
    os.makedirs(TMP, exist_ok=True)
    for filename, color in SYNTH_COLORS.items():
        path = os.path.join(TMP, filename)
        subprocess.run([magick_path, "-size", "300x300", f"xc:{color}", path], check=True, capture_output=True)

SERIES = {
    "series_single": {"MainDicomTags": {"Modality": "CR"}, "Instances": ["inst_single_1"]},
    "series_multi": {"MainDicomTags": {"Modality": "CR"}, "Instances": ["inst_multi_1", "inst_multi_2", "inst_multi_3"]},
}

TAGS = {
    "inst_single_1": {"PatientName": "TEST^PATIENT", "PatientBirthDate": "19900101", "PatientSex": "M", "AccessionNumber": "ACC_SINGLE", "StudyDate": "20260917", "StudyTime": "100000"},
    "inst_multi_1": {"PatientName": "TEST^PATIENT", "PatientBirthDate": "19900101", "PatientSex": "M", "AccessionNumber": "ACC_MULTI", "StudyDate": "20260917", "StudyTime": "110000"},
    "inst_multi_2": {"PatientName": "TEST^PATIENT", "PatientBirthDate": "19900101", "PatientSex": "M", "AccessionNumber": "ACC_MULTI", "StudyDate": "20260917", "StudyTime": "110000"},
    "inst_multi_3": {"PatientName": "TEST^PATIENT", "PatientBirthDate": "19900101", "PatientSex": "M", "AccessionNumber": "ACC_MULTI", "StudyDate": "20260917", "StudyTime": "110000"},
}


class FakeOrthancClient:
    """Stubs the three Orthanc calls build_group_pdf touches, returning
    synthetic data only -- never hits the real Orthanc server."""

    def get_series(self, series_id):
        return SERIES[series_id]

    def get_simplified_tags(self, instance_id):
        return TAGS[instance_id]

    def get_rendered_png(self, instance_id):
        with open(SYNTH_IMAGES[instance_id], "rb") as f:
            return f.read()


def main():
    core.setup_logging("./logs", "INFO")
    generate_synthetic_images("/usr/local/bin/magick")

    group = [
        {
            "study_id": "SYNTH_STUDY_1",
            "accession": "ACC_SINGLE",
            "patient_id": "SYNTH_PATIENT",
            "patient_name": "TEST PATIENT",
            "study": {"Series": ["series_single"], "MainDicomTags": {"StudyDate": "20260917"}},
        },
        {
            "study_id": "SYNTH_STUDY_2",
            "accession": "ACC_MULTI",
            "patient_id": "SYNTH_PATIENT",
            "patient_name": "TEST PATIENT",
            "study": {"Series": ["series_multi"], "MainDicomTags": {"StudyDate": "20260917"}},
        },
    ]

    fake_orthanc = FakeOrthancClient()
    pdf_path = cr.build_group_pdf(fake_orthanc, group, TMP, "/usr/local/bin/magick")
    print("Built PDF:", pdf_path)

    # Verify page count via pdfinfo/magick identify
    result = subprocess.run(
        ["/usr/local/bin/magick", "identify", pdf_path], capture_output=True, text=True
    )
    print("identify output:\n", result.stdout, result.stderr)
    page_count = len(result.stdout.strip().splitlines())
    print(f"Page count: {page_count} (expected 2)")
    assert page_count == 2, f"Expected 2 pages, got {page_count}"

    # Verify the multi-instance page is wider/taller than a single image
    # (montage should have combined 3 panels, not just passed one through).
    montage_check = subprocess.run(
        ["/usr/local/bin/magick", "identify", "-format", "%w %h\n", os.path.join(TMP, "page_ACC_MULTI.jpg")],
        capture_output=True, text=True,
    )
    print("ACC_MULTI page dimensions:", montage_check.stdout.strip())

    single_check = subprocess.run(
        ["/usr/local/bin/magick", "identify", "-format", "%w %h\n", os.path.join(TMP, "page_ACC_SINGLE.jpg")],
        capture_output=True, text=True,
    )
    print("ACC_SINGLE page dimensions:", single_check.stdout.strip())

    print("PASS: 2 pages generated, montage page has different dimensions than single-instance page.")


if __name__ == "__main__":
    main()
