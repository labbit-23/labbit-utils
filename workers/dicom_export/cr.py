"""
CR (X-ray) film consolidation.

Groups today's CR studies by (PatientID, StudyDate), builds one PDF per
group with one page per distinct AccessionNumber (multiple instances on
an accession are gridded onto that one page via ImageMagick montage),
uploads it via FTP, and sends one WhatsApp document message per group.

State lives entirely in Orthanc study metadata (WhatsappStatus /
WhatsappAttempts / WhatsappTimestamp), written to every study in a group
so the existing per-study dashboard lookup shows correct status for each
accession. No local database.
"""

import logging
import os
import subprocess
import time
from collections import defaultdict
from datetime import datetime

import core

log = logging.getLogger("dicom_export")

MAX_ATTEMPTS = 3


def find_todays_cr_studies(orthanc):
    today = datetime.now().strftime("%Y%m%d")
    study_ids = orthanc.find_studies({"StudyDate": today, "ModalitiesInStudy": "CR"})
    return study_ids or []


def group_by_patient_and_date(orthanc, study_ids):
    """Returns {(patient_id, study_date): [study_detail, ...]}."""
    groups = defaultdict(list)
    for study_id in study_ids:
        try:
            study = orthanc.get_study(study_id)
        except Exception as exc:
            log.warning("Could not fetch study %s: %s", study_id, exc)
            continue

        main_tags = study.get("MainDicomTags") or {}
        patient_tags = study.get("PatientMainDicomTags") or {}
        accession = main_tags.get("AccessionNumber")
        if not accession:
            log.info("[StudyId=%s] No accession. Skipping.", study_id)
            continue

        patient_id = patient_tags.get("PatientID", "UNKNOWN")
        study_date = main_tags.get("StudyDate", "UNKNOWN")

        groups[(patient_id, study_date)].append(
            {
                "study_id": study_id,
                "accession": accession,
                "patient_id": patient_id,
                "patient_name": (patient_tags.get("PatientName") or "").replace("^", " ").strip(),
                "study": study,
            }
        )
    return groups


def group_status(orthanc, group):
    """Reads metadata across every study in a group; returns
    (status, attempts) representing the group as a whole. status=='SENT'
    if any member already shows SENT (idempotent re-runs). attempts is
    the max across members."""
    max_attempts = 0
    for member in group:
        status = orthanc.get_metadata(member["study_id"], "WhatsappStatus").upper()
        if status == "SENT":
            return "SENT", max_attempts
        if status == "PROCESSING":
            return "PROCESSING", max_attempts
        try:
            attempts = int(orthanc.get_metadata(member["study_id"], "WhatsappAttempts") or 0)
        except ValueError:
            attempts = 0
        max_attempts = max(max_attempts, attempts)
    return "", max_attempts


def lock_group(orthanc, group, attempts, dry_run):
    for member in group:
        orthanc.put_metadata(member["study_id"], "WhatsappStatus", "PROCESSING", dry_run=dry_run)
        orthanc.put_metadata(member["study_id"], "WhatsappAttempts", attempts + 1, dry_run=dry_run)


def mark_group_sent(orthanc, group, dry_run):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for member in group:
        orthanc.put_metadata(member["study_id"], "WhatsappStatus", "SENT", dry_run=dry_run)
        orthanc.put_metadata(member["study_id"], "WhatsappTimestamp", ts, dry_run=dry_run)


def mark_group_error(orthanc, group, dry_run):
    for member in group:
        orthanc.put_metadata(member["study_id"], "WhatsappStatus", "ERROR", dry_run=dry_run)


def download_and_annotate(orthanc, instance_id, tags, tmp_dir, magick_path):
    """Fetch the rendered PNG and burn in a header strip (patient name,
    age/sex, accession, date/time) -- same fields the Mirth channel
    already annotates, via the same ImageMagick binary."""
    png_bytes = orthanc.get_rendered_png(instance_id)
    raw_path = os.path.join(tmp_dir, f"{instance_id}_raw.png")
    with open(raw_path, "wb") as f:
        f.write(png_bytes)

    patient_name = (tags.get("PatientName") or "").replace("^", " ")
    age_sex = f"{tags.get('PatientBirthDate', '')} | {tags.get('PatientSex', '')}"
    accession = tags.get("AccessionNumber", "")
    study_date = tags.get("StudyDate", "")
    study_time = tags.get("StudyTime", "")

    annotated_path = os.path.join(tmp_dir, f"{instance_id}_annotated.jpg")
    cmd = [
        magick_path,
        raw_path,
        # Full diagnostic resolution (often 4000px+) is unnecessary for a
        # WhatsApp-delivered patient report and is what was blowing past
        # the 5MB PDF cap; cap the long edge before annotating.
        "-resize", "1600x1600>",
        "-gravity", "North",
        "-background", "black",
        "-splice", "0x60",
        "-fill", "white",
        "-pointsize", "22",
        "-annotate", "+0+8", f"{patient_name}  |  {age_sex}  |  Acc: {accession}  |  {study_date} {study_time}",
        "-quality", "82",
        annotated_path,
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return annotated_path


def build_accession_page(annotated_paths, out_path, magick_path):
    """Combine one accession's annotated images into a single page image.
    Single image -> used directly; multiple -> gridded via montage."""
    if len(annotated_paths) == 1:
        os.rename(annotated_paths[0], out_path)
        return out_path

    cols = 2 if len(annotated_paths) <= 4 else 3
    cmd = [
        magick_path.replace("magick", "montage") if "magick" in magick_path else "montage",
        *annotated_paths,
        "-tile", f"{cols}x",
        "-geometry", "+10+10",
        "-background", "white",
        out_path,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fall back to `magick montage` invocation form if the standalone
        # `montage` binary isn't on PATH.
        cmd = [magick_path, "montage", *annotated_paths, "-tile", f"{cols}x", "-geometry", "+10+10", "-background", "white", out_path]
        subprocess.run(cmd, check=True, capture_output=True)
    return out_path


def build_accession_annotated_images(orthanc, study, tmp_dir, magick_path):
    """Resolve every CR instance under one study to an annotated JPEG path."""
    annotated = []
    for series_id in study.get("Series", []):
        series = orthanc.get_series(series_id)
        if (series.get("MainDicomTags") or {}).get("Modality") != "CR":
            continue
        for instance_id in series.get("Instances", []):
            tags = orthanc.get_simplified_tags(instance_id)
            annotated.append(download_and_annotate(orthanc, instance_id, tags, tmp_dir, magick_path))
    return annotated


def build_group_pdf(orthanc, group, tmp_dir, magick_path):
    """One PDF, one page per distinct accession within the group."""
    by_accession = defaultdict(list)
    for member in group:
        by_accession[member["accession"]].append(member)

    page_paths = []
    for accession, members in sorted(by_accession.items()):
        annotated = []
        for member in members:
            annotated.extend(
                build_accession_annotated_images(orthanc, member["study"], tmp_dir, magick_path)
            )

        if not annotated:
            log.warning("Accession=%s has no CR instances, skipping page.", accession)
            continue

        page_path = os.path.join(tmp_dir, f"page_{accession}.jpg")
        page_paths.append(build_accession_page(annotated, page_path, magick_path))

    if not page_paths:
        return None

    patient_id = group[0]["patient_id"]
    study_date = group[0]["study"].get("MainDicomTags", {}).get("StudyDate", "")
    pdf_path = os.path.join(tmp_dir, f"CR_{patient_id}_{study_date}.pdf")
    # Without an explicit density, ImageMagick assumes 1 pixel = 1 point
    # (72 DPI), turning a ~1600px-wide page image into a ~22-inch-wide PDF
    # page -- correct physical page size here instead.
    cmd = [magick_path, "-density", "150", *page_paths, pdf_path]
    subprocess.run(cmd, check=True, capture_output=True)
    return pdf_path


def process_once(cfg, orthanc):
    dry_run = cfg["dry_run"]
    tmp_dir = os.path.abspath(cfg["worker"]["tmp_dir"])
    os.makedirs(tmp_dir, exist_ok=True)
    magick_path = cfg["worker"]["magick_path"]

    study_ids = find_todays_cr_studies(orthanc)
    if not study_ids:
        log.info("No CR studies found today.")
        return 0

    groups = group_by_patient_and_date(orthanc, study_ids)
    log.info("Found %d CR studies in %d patient-day groups.", len(study_ids), len(groups))

    sent_count = 0
    for key, group in groups.items():
        patient_id, study_date = key
        log_prefix = f"[Patient={patient_id} | Date={study_date}] "

        status, attempts = group_status(orthanc, group)
        if status == "SENT":
            log.info(log_prefix + "Already SENT. Skipping.")
            continue
        if status == "PROCESSING":
            log.info(log_prefix + "Currently PROCESSING. Skipping.")
            continue
        if attempts >= MAX_ATTEMPTS:
            log.info(log_prefix + "Max attempts reached. Skipping.")
            continue

        accessions = sorted({m["accession"] for m in group})
        log.info(log_prefix + f"Locking for processing. Accessions={accessions}")
        lock_group(orthanc, group, attempts, dry_run)

        try:
            pdf_path = build_group_pdf(orthanc, group, tmp_dir, magick_path)
            if not pdf_path:
                raise RuntimeError("No pages generated for this group.")

            reqno = group[0]["accession"]  # phone lookup keyed off any one accession in the visit
            phone = core.fetch_phone_from_labit(
                reqno, cfg["labit"]["base_url"], cfg["labit"]["dispatch_user"], cfg["labit"]["dispatch_password"]
            )
            if not phone:
                phone = cfg["whatsapp"]["default_phone"]
                log.warning(log_prefix + f"No phone from Labit, using default: {phone}")

            remote_folder = group[0]["study_id"]  # matches Mirth's studyId-as-folder convention
            public_url = core.upload_file_to_ftp(pdf_path, remote_folder, cfg["ftp"], dry_run=dry_run)

            patient_name = group[0]["patient_name"] or "Patient"
            core.send_whatsapp_document(
                phone, patient_name, public_url, os.path.basename(pdf_path), cfg["whatsapp"], dry_run=dry_run
            )

            mark_group_sent(orthanc, group, dry_run)
            log.info(log_prefix + f"Sent. PDF={pdf_path} URL={public_url}")
            sent_count += 1
            time.sleep(1)

        except Exception as exc:
            log.exception(log_prefix + f"Failed: {exc}")
            mark_group_error(orthanc, group, dry_run)

    return sent_count


def manual_send(cfg, orthanc, accession, phone):
    """
    Explicit human-triggered override for one accession, matching the
    dashboard's manual-send form (accession + phone, no date/grouping).
    Bypasses WhatsappStatus/Attempts gating entirely -- a human asked for
    this specific resend, so the normal skip-if-already-sent/skip-if-
    max-attempts logic does not apply. Still respects DRY_RUN.
    """
    dry_run = cfg["dry_run"]
    tmp_dir = os.path.abspath(cfg["worker"]["tmp_dir"])
    os.makedirs(tmp_dir, exist_ok=True)
    magick_path = cfg["worker"]["magick_path"]
    log_prefix = f"[ManualSend | Accession={accession}] "

    study_ids = orthanc.find_studies({"AccessionNumber": accession})
    if not study_ids:
        raise ValueError(f"No study found for accession={accession}")
    study_id = study_ids[0]
    study = orthanc.get_study(study_id)
    patient_tags = study.get("PatientMainDicomTags") or {}
    patient_name = (patient_tags.get("PatientName") or "").replace("^", " ").strip() or "Patient"

    log.info(log_prefix + f"StudyId={study_id} Phone={phone} (manual override, bypassing status/attempts gate)")

    annotated = build_accession_annotated_images(orthanc, study, tmp_dir, magick_path)
    if not annotated:
        raise RuntimeError(f"No CR instances found for accession={accession}")

    page_path = os.path.join(tmp_dir, f"page_{accession}.jpg")
    build_accession_page(annotated, page_path, magick_path)

    pdf_path = os.path.join(tmp_dir, f"CR_MANUAL_{accession}.pdf")
    subprocess.run([magick_path, "-density", "150", page_path, pdf_path], check=True, capture_output=True)

    effective_phone = phone or core.fetch_phone_from_labit(
        accession, cfg["labit"]["base_url"], cfg["labit"]["dispatch_user"], cfg["labit"]["dispatch_password"]
    ) or cfg["whatsapp"]["default_phone"]

    public_url = core.upload_file_to_ftp(pdf_path, study_id, cfg["ftp"], dry_run=dry_run)
    core.send_whatsapp_document(
        effective_phone, patient_name, public_url, os.path.basename(pdf_path), cfg["whatsapp"], dry_run=dry_run
    )

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    orthanc.put_metadata(study_id, "WhatsappStatus", "SENT", dry_run=dry_run)
    orthanc.put_metadata(study_id, "WhatsappTimestamp", ts, dry_run=dry_run)

    log.info(log_prefix + f"Manual send complete. PDF={pdf_path} URL={public_url}")
    return {"ok": True, "studyId": study_id, "publicUrl": public_url, "phone": effective_phone}
