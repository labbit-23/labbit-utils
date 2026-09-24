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

import json
import logging
import os
import re
import subprocess
import time
from collections import defaultdict
from datetime import datetime

import core

log = logging.getLogger("dicom_export")


def _filename_safe(text, max_len=40):
    """Strip to filesystem-safe characters for use in a PDF filename."""
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", (text or "").strip()).strip("_")
    return cleaned[:max_len] or "UNKNOWN"

MAX_ATTEMPTS = 3


def find_todays_cr_studies(orthanc):
    today = datetime.now().strftime("%Y%m%d")
    study_ids = orthanc.find_studies({"StudyDate": today, "ModalitiesInStudy": "CR"})
    return study_ids or []


def group_by_accession(orthanc, study_ids):
    """Returns {accession: [study_detail, ...]}; accession is the hard outer boundary."""
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

        # PatientID is often blank in real data here (seen directly on real
        # studies -- RIS jams age into PatientName instead, e.g.
        # "KAMALESH^KUMAR^66Y" with PatientID=""). .get(key, default) only
        # falls back when the key is MISSING, not when it's present-but-
        # empty, so this used to silently group every blank-PatientID
        # patient together under one shared key. Orthanc's own
        # ParentPatient is NOT a safe fallback either -- confirmed on real
        # data that Orthanc's patient-matching also merges genuinely
        # different patients under one Patient resource when PatientID is
        # blank (found 4 distinct real patients sharing one ParentPatient).
        # Never risk combining two different patients' images into one
        # PDF/message: when PatientID is blank, treat this study as its own
        # unmerged group using the study's own unique ID as the key.
        patient_id = patient_tags.get("PatientID") or f"NOID-{study_id}"

        groups[accession].append(
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
    the max across members.

    raise_on_error=True on the WhatsappStatus read specifically: this is
    the gate that decides whether a real send happens, so a transient
    Orthanc read failure must propagate and abort this cycle rather than
    being silently treated as "no status" (which would look identical to
    a genuinely-never-sent study and cause a duplicate real send -- this
    happened live on 2026-09-17, see get_metadata's docstring)."""
    max_attempts = 0
    for member in group:
        status = orthanc.get_metadata(member["study_id"], "WhatsappStatus", raise_on_error=True).upper()
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


def mark_group_sent(orthanc, group, dry_run, public_url="", phone=""):
    """Mark a group sent and persist all delivered PDF URLs.

    WhatsappPdfUrl remains the first-URL compatibility field;
    WhatsappPdfUrls is the canonical JSON list for multi-part previews.
    """
    urls = public_url if isinstance(public_url, list) else ([public_url] if public_url else [])
    urls = [str(url) for url in urls if str(url).strip()]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for member in group:
        orthanc.put_metadata(member["study_id"], "WhatsappStatus", "SENT", dry_run=dry_run)
        orthanc.put_metadata(member["study_id"], "WhatsappTimestamp", ts, dry_run=dry_run)
        if phone:
            orthanc.put_metadata(member["study_id"], "WhatsappPhone", phone, dry_run=dry_run)
        if urls:
            orthanc.put_metadata(member["study_id"], "WhatsappPdfUrl", json.dumps(urls) if len(urls) > 1 else urls[0], dry_run=dry_run)

def mark_group_error(orthanc, group, dry_run):
    for member in group:
        orthanc.put_metadata(member["study_id"], "WhatsappStatus", "ERROR", dry_run=dry_run)


def download_and_annotate(orthanc, instance_id, tags, tmp_dir, magick_path, institution_name=""):
    """Fetch the rendered PNG and burn in a header strip (patient name,
    age/sex, accession, date/time) and a footer strip (institution
    branding) -- same fields/layout the Mirth channel already annotates,
    via the same ImageMagick binary. Institution name prefers the DICOM
    tag (matching the original's per-image InstitutionName source) and
    falls back to the configured default."""
    png_bytes = orthanc.get_rendered_png(instance_id)
    raw_path = os.path.join(tmp_dir, f"{instance_id}_raw.png")
    with open(raw_path, "wb") as f:
        f.write(png_bytes)

    patient_name = (tags.get("PatientName") or "").replace("^", " ")
    age_sex = f"{tags.get('PatientBirthDate', '')} | {tags.get('PatientSex', '')}"
    patient_id = tags.get("PatientID", "")
    accession = tags.get("AccessionNumber", "")
    study_date = tags.get("StudyDate", "")
    study_time = tags.get("StudyTime", "")
    centre_name = tags.get("InstitutionName") or institution_name
    # Distinguishes e.g. "X-RAY CHEST PA VIEW" from "...AP VIEW" -- without
    # this, two views of the same body part are visually indistinguishable
    # in the header. BodyPartExamined alone doesn't carry the PA/AP
    # distinction (both say just "CHEST"), so prefer the more specific tag.
    study_desc = tags.get("AcquisitionDeviceProcessingDescription") or tags.get("BodyPartExamined") or ""

    annotated_path = os.path.join(tmp_dir, f"{instance_id}_annotated.jpg")
    cmd = [
        magick_path,
        raw_path,
        # Full diagnostic resolution (often 4000px+) is unnecessary for a
        # WhatsApp-delivered patient report and is what was blowing past
        # the 5MB PDF cap; cap the long edge before annotating. Real CR
        # X-rays are always comfortably above 900px so this floor never
        # fires for them -- it exists because CT source images (thumbnail-
        # derived, via ct.py) can be narrow enough that the fixed-size
        # header/footer text clips off the edge of the canvas; found on a
        # real CT test render ("SHIVA B" clipped to "IVA B"). Enlarge-if-
        # too-small first, then the existing shrink-if-too-large cap.
        "-resize", "900x900<",
        "-resize", "1600x1600>",
        "-gravity", "North",
        "-background", "black",
        "-splice", "0x60",
        "-fill", "white",
        "-pointsize", "22",
        "-annotate", "+0+8", f"{patient_name}  |  ID: {patient_id}  |  {age_sex}  |  Acc: {accession}  |  {study_date} {study_time}  |  {study_desc}",
        "-gravity", "South",
        "-background", "black",
        "-splice", "0x50",
        "-fill", "white",
        "-pointsize", "24",
        "-annotate", "+0+8", centre_name,
        "-quality", "82",
        annotated_path,
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return annotated_path



def download_and_annotate_ct(orthanc, instance_id, tags, tmp_dir, magick_path, institution_name=""):
    """Render a future CT tile with plain white corner labels only."""
    png_bytes = orthanc.get_rendered_png(instance_id)
    raw_path = os.path.join(tmp_dir, f"{instance_id}_raw.png")
    with open(raw_path, "wb") as handle:
        handle.write(png_bytes)

    patient_name = (tags.get("PatientName") or "").replace("^", " ")
    patient_id = tags.get("PatientID", "")
    accession = tags.get("AccessionNumber", "")
    sex = tags.get("PatientSex", "") or "—"
    timestamp = f"{tags.get('StudyDate', '')} {tags.get('StudyTime', '')}".strip()
    series_no = tags.get("SeriesNumber", "") or "—"
    instance_no = tags.get("InstanceNumber", "") or "—"

    annotated_path = os.path.join(tmp_dir, f"{instance_id}_annotated.jpg")
    cmd = [
        magick_path, raw_path,
        "-resize", "1600x1600>",
        "-fill", "white", "-stroke", "none", "-pointsize", "20", "-interline-spacing", "2",
        "-gravity", "NorthWest", "-annotate", "+18+18",
        f"{patient_name}\nPatient ID: {patient_id}\nAcc: {accession}",
        "-gravity", "NorthEast", "-annotate", "+18+18",
        f"{timestamp}\nSex: {sex}\nSeries {series_no}",
        "-pointsize", "18",
        "-gravity", "SouthWest", "-annotate", "+18+18", f"Instance {instance_no}",
        "-quality", "82", annotated_path,
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return annotated_path

def _is_landscape(path, magick_path):
    try:
        out = subprocess.run(
            [magick_path, "identify", "-format", "%w %h", path],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
        width, height = (int(v) for v in out.split())
        return width > height
    except Exception:
        return False


def build_accession_page(annotated_paths, out_path, magick_path, footer_text=""):
    """Combine one accession's annotated images into a single page image.
    Single image -> used directly; multiple -> gridded via montage."""
    if len(annotated_paths) == 1:
        if footer_text:
            subprocess.run([magick_path, annotated_paths[0], "-gravity", "South", "-background", "black", "-splice", "0x44", "-fill", "white", "-pointsize", "18", "-annotate", "+0+8", footer_text, out_path], check=True, capture_output=True)
        else:
            os.rename(annotated_paths[0], out_path)
        return out_path

    # Two landscape films (e.g. Chest PA + AP) side by side each get
    # squeezed into a narrow column for no reason -- stack them vertically
    # instead so each keeps its natural width. Only handling the 2-image
    # case; beyond that every layout choice is a judgment call and the
    # existing fixed grid is already clear enough for reference viewing.
    if len(annotated_paths) == 2 and all(_is_landscape(p, magick_path) for p in annotated_paths):
        cols = 1
    else:
        cols = 2 if len(annotated_paths) <= 4 else 3
    cmd = [
        magick_path.replace("magick", "montage") if "magick" in magick_path else "montage",
        *annotated_paths,
        "-tile", f"{cols}x",
        "-geometry", "+10+10",
        # Black, not white, for the gap/padding fill between grid tiles --
        # true to the X-ray viewing look (the header/footer strips are
        # already black; a white montage background looked inconsistent
        # against them).
        "-background", "black",
        out_path,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fall back to `magick montage` invocation form if the standalone
        # `montage` binary isn't on PATH.
        cmd = [magick_path, "montage", *annotated_paths, "-tile", f"{cols}x", "-geometry", "+10+10", "-background", "black", out_path]
        subprocess.run(cmd, check=True, capture_output=True)
    if footer_text:
        footer_path = out_path + ".footer.jpg"
        subprocess.run([magick_path, out_path, "-gravity", "South", "-background", "black", "-splice", "0x44", "-fill", "white", "-pointsize", "18", "-annotate", "+0+8", footer_text, footer_path], check=True, capture_output=True)
        os.replace(footer_path, out_path)
    return out_path


def build_accession_annotated_images(orthanc, study, tmp_dir, magick_path, institution_name=""):
    """Resolve every CR instance under one study to an annotated JPEG path."""
    annotated = []
    for series_id in study.get("Series", []):
        series = orthanc.get_series(series_id)
        if (series.get("MainDicomTags") or {}).get("Modality") != "CR":
            continue
        for instance_id in series.get("Instances", []):
            tags = orthanc.get_simplified_tags(instance_id)
            annotated.append(download_and_annotate(orthanc, instance_id, tags, tmp_dir, magick_path, institution_name))
    return annotated


def build_group_pdf(orthanc, group, tmp_dir, magick_path, institution_name=""):
    """Build one PDF with one page per named exam within each accession.

    The current ingest does not reliably populate StudyDescription, so the
    temporary exam key is SeriesDescription, then BodyPartExamined, then
    ProtocolName. Series sharing that name are montaged onto one page; the
    accession remains an outer boundary so unrelated orders cannot merge.
    """
    exam_rows = defaultdict(list)
    for member in group:
        for series_id in member["study"].get("Series", []):
            series = orthanc.get_series(series_id)
            tags = series.get("MainDicomTags") or {}
            if tags.get("Modality") != "CR":
                continue
            exam_name = (
                tags.get("SeriesDescription")
                or tags.get("BodyPartExamined")
                or tags.get("ProtocolName")
                or "UNSPECIFIED"
            )
            exam_name = re.sub(r"\\s+", " ", str(exam_name)).strip().upper()
            try:
                series_number = int(str(tags.get("SeriesNumber", "")).strip())
            except (TypeError, ValueError):
                series_number = 10**9
            exam_rows[(member["accession"], exam_name)].append(
                (member, series_number, series_id, series)
            )

    page_paths = []
    exams_per_accession = defaultdict(int)
    for accession, _ in exam_rows:
        exams_per_accession[accession] += 1

    for (accession, exam_name), rows in sorted(exam_rows.items()):
        rows.sort(key=lambda row: (row[1], row[2]))
        annotated = []
        for _, _, _, series in rows:
            for instance_id in series.get("Instances", []):
                tags = orthanc.get_simplified_tags(instance_id)
                annotated.append(download_and_annotate(
                    orthanc, instance_id, tags, tmp_dir, magick_path, institution_name
                ))

        if not annotated:
            log.warning("Accession=%s exam=%s has no CR instances, skipping page.", accession, exam_name)
            continue

        if exams_per_accession[accession] == 1:
            page_name = f"page_{accession}.jpg"
        else:
            page_name = f"page_{accession}_exam_{_filename_safe(exam_name)}.jpg"
        page_path = os.path.join(tmp_dir, page_name)
        page_paths.append(build_accession_page(annotated, page_path, magick_path))

    if not page_paths:
        return None

    patient_id = group[0]["patient_id"]
    patient_name_safe = _filename_safe(group[0]["patient_name"])
    study_date = group[0]["study"].get("MainDicomTags", {}).get("StudyDate", "")
    pdf_path = os.path.join(tmp_dir, f"CR_{patient_name_safe}_{patient_id}_{study_date}.pdf")
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
    institution_name = cfg.get("institution", {}).get("name", "")

    study_ids = find_todays_cr_studies(orthanc)
    if not study_ids:
        log.info("No CR studies found today.")
        return 0

    groups = group_by_accession(orthanc, study_ids)
    log.info("Found %d CR studies in %d accession groups.", len(study_ids), len(groups))

    sent_count = 0
    for accession, group in groups.items():
        log_prefix = f"[Accession={accession}] "

        try:
            status, attempts = group_status(orthanc, group)
        except Exception as exc:
            # Could not confirm this group's real WhatsappStatus (e.g. a
            # transient Orthanc connection failure) -- skip this cycle
            # rather than risk treating "unknown" as "not sent". Retried
            # automatically on the next poll once Orthanc is responsive.
            log.warning(log_prefix + f"Could not verify send status this cycle, skipping: {exc}")
            continue
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
            pdf_path = build_group_pdf(orthanc, group, tmp_dir, magick_path, institution_name)
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

            # Link-only push to Labit Core -- one PDF can cover multiple
            # accessions in this group (same patient/day), so push the
            # same URL to every accession's own reqno, not just the one
            # used for phone lookup. Never let a Labit-side failure here
            # block the WhatsApp send, which is the critical path.
            for reqno_for_link in accessions:
                try:
                    core.push_report_link_to_labit(
                        reqno_for_link, [public_url], cfg["labit"]["base_url"], cfg["labit"]["internal_token"],
                        dry_run=dry_run,
                    )
                except Exception as exc:
                    log.warning(log_prefix + f"push_report_link_to_labit failed for reqno={reqno_for_link}: {exc}")

            patient_name = group[0]["patient_name"] or "Patient"
            core.send_whatsapp_document(
                phone, patient_name, public_url, os.path.basename(pdf_path), cfg["whatsapp"], dry_run=dry_run
            )

            mark_group_sent(orthanc, group, dry_run, public_url, phone)
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
    institution_name = cfg.get("institution", {}).get("name", "")
    log_prefix = f"[ManualSend | Accession={accession}] "

    study_ids = orthanc.find_studies({"AccessionNumber": accession})
    if not study_ids:
        raise ValueError(f"No study found for accession={accession}")
    study_id = study_ids[0]
    study = orthanc.get_study(study_id)
    patient_tags = study.get("PatientMainDicomTags") or {}
    patient_name = (patient_tags.get("PatientName") or "").replace("^", " ").strip() or "Patient"

    log.info(log_prefix + f"StudyId={study_id} Phone={phone} (manual override, bypassing status/attempts gate)")

    annotated = build_accession_annotated_images(orthanc, study, tmp_dir, magick_path, institution_name)
    if not annotated:
        raise RuntimeError(f"No CR instances found for accession={accession}")

    page_path = os.path.join(tmp_dir, f"page_{accession}.jpg")
    build_accession_page(annotated, page_path, magick_path)

    patient_name_safe = _filename_safe(patient_name)
    pdf_path = os.path.join(tmp_dir, f"CR_MANUAL_{patient_name_safe}_{accession}.pdf")
    subprocess.run([magick_path, "-density", "150", page_path, pdf_path], check=True, capture_output=True)

    effective_phone = phone or core.fetch_phone_from_labit(
        accession, cfg["labit"]["base_url"], cfg["labit"]["dispatch_user"], cfg["labit"]["dispatch_password"]
    ) or cfg["whatsapp"]["default_phone"]

    public_url = core.upload_file_to_ftp(pdf_path, study_id, cfg["ftp"], dry_run=dry_run)

    try:
        core.push_report_link_to_labit(
            accession, [public_url], cfg["labit"]["base_url"], cfg["labit"]["internal_token"], dry_run=dry_run,
        )
    except Exception as exc:
        log.warning(log_prefix + f"push_report_link_to_labit failed: {exc}")

    core.send_whatsapp_document(
        effective_phone, patient_name, public_url, os.path.basename(pdf_path), cfg["whatsapp"], dry_run=dry_run
    )

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    orthanc.put_metadata(study_id, "WhatsappStatus", "SENT", dry_run=dry_run)
    orthanc.put_metadata(study_id, "WhatsappTimestamp", ts, dry_run=dry_run)
    if public_url:
        orthanc.put_metadata(study_id, "WhatsappPdfUrl", public_url, dry_run=dry_run)

    log.info(log_prefix + f"Manual send complete. PDF={pdf_path} URL={public_url}")
    return {"ok": True, "studyId": study_id, "publicUrl": public_url, "phone": effective_phone}
