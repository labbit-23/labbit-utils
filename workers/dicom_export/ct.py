"""
CT operator-selected film consolidation.

Unlike cr.py (which auto-includes every CR instance in a study), CT
studies are large -- often 100+ instances -- so an operator picks which
images actually go in the report via the dicom-v2 dashboard's CT tab.
Selection is tracked per-INSTANCE (not per-study like WhatsappStatus)
via a new Orthanc metadata key, SelectedForReport, storing the 1-based
order the operator chose (unset/0 = not selected). Pagination is one
page per SERIES (not per accession like CR), using only the selected
instances in that order.

If nobody selects anything within SELECTION_FALLBACK_HOURS of the study
settling (see auto_select_all_if_stale's docstring for exactly what
"settling" means and why), every instance gets auto-selected in
series+instance order -- falling back to today's send-everything
behavior rather than leaving a study stuck forever.

Reuses cr.py's generic helpers directly (group_by_patient_and_date,
lock_group, mark_group_sent, mark_group_error, group_status,
download_and_annotate, build_accession_page, _filename_safe,
MAX_ATTEMPTS) since none of that logic is actually CR-specific despite
living in cr.py -- duplicating it here would just be drift risk.
"""

import logging
import os
import subprocess
import time
from collections import defaultdict
from datetime import datetime, timedelta

import core
import cr

log = logging.getLogger("dicom_export")

# How long a CT study can sit with zero operator selection before we give
# up waiting and send everything, matching today's Mirth behavior instead
# of leaving it stuck forever. Pavan's own spec: "3-4 hour fallback."
SELECTION_FALLBACK_HOURS = 3.5


def find_todays_ct_studies(orthanc):
    today = datetime.now().strftime("%Y%m%d")
    study_ids = orthanc.find_studies({"StudyDate": today, "ModalitiesInStudy": "CT"})
    return study_ids or []


def _study_series_instances(orthanc, study):
    """Returns {series_id: [instance_id, ...]} for every CT series in a
    study, instances in Orthanc's own (acquisition) order."""
    result = {}
    for series_id in study.get("Series", []):
        series = orthanc.get_series(series_id)
        if (series.get("MainDicomTags") or {}).get("Modality") != "CT":
            continue
        result[series_id] = series.get("Instances", [])
    return result

# --- selected an instance --------------------------------------------------
def _selected_order(orthanc, instance_id):
    """Returns the operator-assigned order (int, >=1) for one instance, or
    0 if not selected. SelectedForReport is a plain string metadata value;
    a missing/blank/non-numeric value all mean "not selected"."""
    raw = orthanc.get_metadata(instance_id, "SelectedForReport", "", resource_type="instances")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def has_any_selection(orthanc, study):
    """True as soon as ANY instance in the study carries a SelectedForReport
    value. This alone is treated as "selection is final" -- see
    process_once()'s docstring for why no additional quiet-period timing
    heuristic is layered on top of this."""
    for series_id, instance_ids in _study_series_instances(orthanc, study).items():
        for iid in instance_ids:
            if _selected_order(orthanc, iid) > 0:
                return True
    return False


def get_selected_instances_by_series(orthanc, study):
    """Returns {series_id: [instance_id, ...]} containing only the SELECTED
    instances of each series, ordered by their SelectedForReport value.
    Series with zero selected instances are omitted entirely."""
    result = {}
    for series_id, instance_ids in _study_series_instances(orthanc, study).items():
        ordered = sorted(
            ((_selected_order(orthanc, iid), iid) for iid in instance_ids),
            key=lambda pair: pair[0],
        )
        selected = [iid for order, iid in ordered if order > 0]
        if selected:
            result[series_id] = selected
    return result


def auto_select_all_if_stale(orthanc, study, study_id, dry_run):
    """
    The fallback path: if this study has NO operator selection at all and
    has been sitting untouched (Orthanc's own LastUpdate, which we
    confirmed empirically does NOT change on instance-metadata writes, so
    it reflects "when new instances stopped arriving" -- not touched by
    selection activity itself) for more than SELECTION_FALLBACK_HOURS,
    auto-select every CT instance in series+instance order. This is the
    ONLY place SelectedForReport gets written by the worker itself rather
    than by an operator's explicit Save -- everywhere else, "has a
    selection" means "a human saved one."

    Returns True if it just performed an auto-selection (caller should
    treat the study as newly ready), False otherwise (either already
    selected, or still within the waiting window).
    """
    if has_any_selection(orthanc, study):
        return False

    last_update_raw = study.get("LastUpdate", "")
    try:
        last_update = datetime.strptime(last_update_raw, "%Y%m%dT%H%M%S")
    except ValueError:
        log.warning("[StudyId=%s] Unparseable LastUpdate=%r, cannot evaluate fallback.", study_id, last_update_raw)
        return False

    age = datetime.now() - last_update
    if age < timedelta(hours=SELECTION_FALLBACK_HOURS):
        return False

    log.info(
        "[StudyId=%s] No operator selection after %.1fh (LastUpdate=%s). Auto-selecting all instances.",
        study_id, age.total_seconds() / 3600, last_update_raw,
    )
    order = 1
    for series_id, instance_ids in sorted(_study_series_instances(orthanc, study).items()):
        for iid in instance_ids:
            orthanc.put_metadata(iid, "SelectedForReport", order, dry_run=dry_run, resource_type="instances")
            order += 1
    return True


# --- PDF building ------------------------------------------------------------
def build_group_pdf(orthanc, group, tmp_dir, magick_path, institution_name=""):
    """One PDF, one page per SERIES that has at least one selected
    instance in this patient-day group (not one page per accession, since
    a single CT study's selected images across all its series make up the
    report here -- pagination is a presentation choice per series, not
    per order). Images placed within a page in the operator's chosen
    order."""
    page_paths = []
    # Sort studies then series so page order is stable across runs.
    for member in sorted(group, key=lambda m: m["accession"]):
        study = member["study"]
        selected_by_series = get_selected_instances_by_series(orthanc, study)
        for series_id, instance_ids in sorted(selected_by_series.items()):
            annotated = []
            for instance_id in instance_ids:
                tags = orthanc.get_simplified_tags(instance_id)
                annotated.append(
                    cr.download_and_annotate(orthanc, instance_id, tags, tmp_dir, magick_path, institution_name)
                )
            if not annotated:
                continue
            page_path = os.path.join(tmp_dir, f"page_{series_id}.jpg")
            page_paths.append(cr.build_accession_page(annotated, page_path, magick_path))

    if not page_paths:
        return None

    patient_id = group[0]["patient_id"]
    patient_name_safe = cr._filename_safe(group[0]["patient_name"])
    study_date = group[0]["study"].get("MainDicomTags", {}).get("StudyDate", "")
    pdf_path = os.path.join(tmp_dir, f"CT_{patient_name_safe}_{patient_id}_{study_date}.pdf")
    cmd = [magick_path, "-density", "150", *page_paths, pdf_path]
    subprocess.run(cmd, check=True, capture_output=True)
    return pdf_path


def process_once(cfg, orthanc):
    """
    Mirrors cr.process_once()'s overall shape (group by patient-day, lock,
    build, send, mark) with one key gating difference: a CT group is only
    eligible for processing once EVERY member study in it either (a) has
    at least one SelectedForReport value, or (b) has been auto-selected by
    the stale-study fallback above.

    Why "has any SelectedForReport value" alone means "selection is
    final," with no extra quiet-period/debounce logic: the ONLY code path
    that writes SelectedForReport (other than the fallback) is the
    SAVE_SELECTION HTTP endpoint, which only fires when an operator
    explicitly clicks "Save selection" in the UI -- not on every click as
    they're still arranging thumbnails. The save action itself already IS
    the human's "I'm done" signal; layering a second timing heuristic on
    top would just add complexity without a real problem it solves.
    (Trade-off, deliberately accepted: if an operator saves and then
    immediately wants to revise before the next ~60s poll picks it up,
    they need to re-save before that poll fires. Same finality trade-off
    Pavan already chose for CR's fully-automatic, no-confirm-step send.)
    """
    dry_run = cfg["dry_run"]
    tmp_dir = os.path.abspath(cfg["worker"]["tmp_dir"])
    os.makedirs(tmp_dir, exist_ok=True)
    magick_path = cfg["worker"]["magick_path"]
    institution_name = cfg.get("institution", {}).get("name", "")

    study_ids = find_todays_ct_studies(orthanc)
    if not study_ids:
        log.info("No CT studies found today.")
        return 0

    # Evaluate the stale-study fallback for every study before grouping,
    # so a group's readiness check below sees up-to-date selection state.
    for study_id in study_ids:
        try:
            study = orthanc.get_study(study_id)
        except Exception as exc:
            log.warning("Could not fetch study %s for fallback check: %s", study_id, exc)
            continue
        auto_select_all_if_stale(orthanc, study, study_id, dry_run)

    groups = cr.group_by_patient_and_date(orthanc, study_ids)
    log.info("Found %d CT studies in %d patient-day groups.", len(study_ids), len(groups))

    sent_count = 0
    for key, group in groups.items():
        patient_id, study_date = key
        log_prefix = f"[CT Patient={patient_id} | Date={study_date}] "

        status, attempts = cr.group_status(orthanc, group)
        if status == "SENT":
            log.info(log_prefix + "Already SENT. Skipping.")
            continue
        if status == "PROCESSING":
            log.info(log_prefix + "Currently PROCESSING. Skipping.")
            continue
        if attempts >= cr.MAX_ATTEMPTS:
            log.info(log_prefix + "Max attempts reached. Skipping.")
            continue

        not_ready = [m for m in group if not has_any_selection(orthanc, m["study"])]
        if not_ready:
            log.info(log_prefix + f"Waiting on operator selection for {len(not_ready)} study(s). Skipping for now.")
            continue

        accessions = sorted({m["accession"] for m in group})
        log.info(log_prefix + f"Locking for processing. Accessions={accessions}")
        cr.lock_group(orthanc, group, attempts, dry_run)

        try:
            pdf_path = build_group_pdf(orthanc, group, tmp_dir, magick_path, institution_name)
            if not pdf_path:
                raise RuntimeError("No pages generated for this group (no selected instances found).")

            reqno = group[0]["accession"]
            phone = core.fetch_phone_from_labit(
                reqno, cfg["labit"]["base_url"], cfg["labit"]["dispatch_user"], cfg["labit"]["dispatch_password"]
            )
            if not phone:
                phone = cfg["whatsapp"]["default_phone"]
                log.warning(log_prefix + f"No phone from Labit, using default: {phone}")

            remote_folder = group[0]["study_id"]
            public_url = core.upload_file_to_ftp(pdf_path, remote_folder, cfg["ftp"], dry_run=dry_run)

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

            cr.mark_group_sent(orthanc, group, dry_run, public_url)
            log.info(log_prefix + f"Sent. PDF={pdf_path} URL={public_url}")
            sent_count += 1
            time.sleep(1)

        except Exception as exc:
            log.exception(log_prefix + f"Failed: {exc}")
            cr.mark_group_error(orthanc, group, dry_run)

    return sent_count
