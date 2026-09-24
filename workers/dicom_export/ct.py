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

Reuses cr.py's generic helpers directly (group_by_accession,
lock_group, mark_group_sent, mark_group_error, download_and_annotate,
build_accession_page, _filename_safe, MAX_ATTEMPTS) since none of that
logic is actually CR-specific despite living in cr.py -- duplicating it
here would just be drift risk. CT delivery status is handled per study
so one StudyInstanceUID cannot suppress another within the accession.
"""

import io
import logging
import os
import subprocess
import shutil
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import core
import cr

log = logging.getLogger("dicom_export")

# How long a CT study can sit with zero operator selection before we give
# up waiting and send everything, matching today's Mirth behavior instead
# of leaving it stuck forever. Pavan's own spec: "3-4 hour fallback."
SELECTION_FALLBACK_HOURS = 3.5

FILM_LAYOUTS = {
    "4x4": (4, 4), "5x5": (5, 5), "5x6": (5, 6),
    "6x4": (6, 4), "6x6": (6, 6), "7x5": (7, 5),
}

# Keep remote CT rendering bounded while avoiding one serial Orthanc round-trip
# per selected image. executor.map preserves the operator's image order.
RENDER_WORKERS = 4

# Portrait 14x17 film at 150 DPI. CT pages use the existing 6x4
# convention: six rows by four columns, with a fixed canvas regardless of
# image count or source aspect ratio. Geometry preserves aspect ratio and
# centers each tile.
CT_PAGE_WIDTH = 2100
CT_PAGE_HEIGHT = 2550
CT_GRID_ROWS = 6
CT_GRID_COLS = 4
CT_GRID_GAP = 8
CT_FOOTER_RESERVE = 72


def _annotate_one(orthanc, instance_id, tmp_dir, magick_path, institution_name):
    tags = orthanc.get_simplified_tags(instance_id)
    return cr.download_and_annotate_ct(
        orthanc, instance_id, tags, tmp_dir, magick_path, institution_name
    )


def validate_film_layout(layout):
    """Return (rows, cols) for a supported composer layout."""
    try:
        rows, cols = FILM_LAYOUTS[str(layout)]
    except (KeyError, TypeError):
        raise ValueError("unsupported film layout")
    return rows, cols



def _ct_tile_geometry(rows, cols):
    usable_width = CT_PAGE_WIDTH - 40 - (cols - 1) * CT_GRID_GAP
    usable_height = CT_PAGE_HEIGHT - CT_FOOTER_RESERVE - 40 - (rows - 1) * CT_GRID_GAP
    return f"{max(1, usable_width // cols)}x{max(1, usable_height // rows)}+{CT_GRID_GAP}+{CT_GRID_GAP}"


def build_ct_page(annotated_paths, out_path, magick_path, footer_text="SDRC Diagnostics | sdrc.in",
                  rows=CT_GRID_ROWS, cols=CT_GRID_COLS):
    """Place CT images on a fixed portrait 14x17 page using the selected grid.

    This is CT-only. CR continues to use cr.build_accession_page and its
    existing variable-size montage behavior. ImageMagick geometry fits each
    image inside its tile without stretching or cropping.
    """
    if not annotated_paths:
        raise ValueError("at least one CT image is required")
    tile_geometry = _ct_tile_geometry(rows, cols)
    montage = magick_path.replace("magick", "montage") if "magick" in magick_path else "montage"
    montage_path = out_path + ".montage.png"
    cmd = [montage, *annotated_paths, "-tile", f"{cols}x{rows}",
           "-geometry", tile_geometry, "-background", "black", montage_path]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        subprocess.run([magick_path, "montage", *annotated_paths,
                        "-tile", f"{cols}x{rows}", "-geometry", tile_geometry,
                        "-background", "black", montage_path],
                       check=True, capture_output=True)
    # montage does not reliably honor a final canvas extent on all
    # ImageMagick builds, so normalize it in a second step.
    final_cmd = [magick_path, montage_path, "-background", "black", "-gravity", "center",
                 "-extent", f"{CT_PAGE_WIDTH}x{CT_PAGE_HEIGHT}"]
    # Keep the final page compact, but only compress once after all fitting
    # and annotation work is complete. Composer PNG output remains lossless.
    if out_path.lower().endswith((".jpg", ".jpeg")):
        final_cmd.extend(["-quality", "94"])
    final_cmd.append(out_path)
    subprocess.run(final_cmd, check=True, capture_output=True)
    try:
        os.unlink(montage_path)
    except FileNotFoundError:
        pass
    if footer_text:
        subprocess.run([magick_path, out_path, "-gravity", "South", "-fill", "white",
                        "-pointsize", "18", "-annotate", "+0+16", footer_text, out_path],
                       check=True, capture_output=True)
    return out_path


def _render_ct_pages(annotated, output_dir, magick_path, footer_text, rows, cols, prefix):
    page_paths = []
    page_size = rows * cols
    for index in range(0, len(annotated), page_size):
        page_no = index // page_size + 1
        page_path = os.path.join(output_dir, f"{prefix}_{page_no:02d}.jpg")
        build_ct_page(annotated[index:index + page_size], page_path, magick_path,
                      footer_text, rows, cols)
        page_paths.append(page_path)
    return page_paths


def build_composer_raster(orthanc, study, selected_instance_ids, layout,
                          tmp_dir, magick_path, institution_name=""):
    """Render the first fixed portrait 14x17 CT film page as PNG bytes."""
    rows, cols = validate_film_layout(layout)
    selected = list(dict.fromkeys(selected_instance_ids or []))
    if not selected:
        raise ValueError("at least one image must be selected")
    known = {iid for sid in study.get("Series", [])
             for iid in orthanc.get_series(sid).get("Instances", [])}
    if any(iid not in known for iid in selected):
        raise ValueError("selection contains an instance outside this study")
    import shutil
    render_dir = os.path.join(os.path.abspath(tmp_dir), "composer-raster", uuid.uuid4().hex)
    os.makedirs(render_dir, exist_ok=True)
    try:
        with ThreadPoolExecutor(max_workers=RENDER_WORKERS) as pool:
            annotated = list(pool.map(
                lambda iid: _annotate_one(orthanc, iid, render_dir, magick_path, institution_name),
                selected,
            ))
        output = os.path.join(render_dir, "film.png")
        build_ct_page(annotated[:rows * cols], output, magick_path,
                      "SDRC Diagnostics | sdrc.in", rows, cols)
        with open(output, "rb") as handle:
            return handle.read()
    finally:
        shutil.rmtree(render_dir, ignore_errors=True)


def build_composer_preview(orthanc, study, selected_instance_ids, layout,
                           tmp_dir, magick_path, institution_name=""):
    """Render fixed portrait 14x17 CT preview pages without changing send state."""
    rows, cols = validate_film_layout(layout)
    selected = list(dict.fromkeys(selected_instance_ids or []))
    if not selected:
        raise ValueError("at least one image must be selected")
    if len(selected) > rows * cols * 20:
        raise ValueError("selection is too large for one preview request")

    known = {iid for sid in study.get("Series", [])
             for iid in orthanc.get_series(sid).get("Instances", [])}
    if any(iid not in known for iid in selected):
        raise ValueError("selection contains an instance outside this study")

    import shutil
    preview_dir = os.path.join(os.path.abspath(tmp_dir), "composer-preview", uuid.uuid4().hex)
    os.makedirs(preview_dir, exist_ok=True)
    try:
        with ThreadPoolExecutor(max_workers=RENDER_WORKERS) as pool:
            annotated = list(pool.map(
                lambda iid: _annotate_one(orthanc, iid, preview_dir, magick_path, institution_name),
                selected,
            ))
        page_paths = _render_ct_pages(annotated, preview_dir, magick_path,
                                      "SDRC Diagnostics | sdrc.in", rows, cols, "film")
        pdf_path = os.path.join(preview_dir, "film-preview.pdf")
        subprocess.run([magick_path, "-density", "150", *page_paths, pdf_path],
                       check=True, capture_output=True)
        with open(pdf_path, "rb") as handle:
            return handle.read()
    finally:
        shutil.rmtree(preview_dir, ignore_errors=True)


def build_vector_preview_pdf(orthanc, study, selected_instance_ids, layout, output_path):
    """Render a trial CT PDF with raster images and selectable vector labels.

    This intentionally remains separate from the live ImageMagick path until
    the output is approved. Images are JPEG-compressed in memory for size;
    patient/series/instance labels are drawn as real PDF text by ReportLab.
    """
    from PIL import Image
    from reportlab.lib.utils import ImageReader
    from reportlab.pdfgen import canvas

    rows, cols = validate_film_layout(layout)
    selected = list(dict.fromkeys(selected_instance_ids or []))
    if not selected:
        raise ValueError("at least one image must be selected")
    page_width, page_height = 14 * 72, 17 * 72
    margin, gap, footer = 18, 6, 34
    cell_width = (page_width - 2 * margin - (cols - 1) * gap) / cols
    cell_height = (page_height - 2 * margin - footer - (rows - 1) * gap) / rows

    def fetch(iid):
        tags = orthanc.get_simplified_tags(iid)
        return iid, tags, orthanc.get_rendered_png(iid)

    fetched = []
    with ThreadPoolExecutor(max_workers=RENDER_WORKERS) as pool:
        fetched = list(pool.map(fetch, selected))

    pdf = canvas.Canvas(output_path, pagesize=(page_width, page_height), pageCompression=1)
    for page_start in range(0, len(fetched), rows * cols):
        pdf.setFillColorRGB(0, 0, 0)
        pdf.rect(0, 0, page_width, page_height, stroke=0, fill=1)
        for pos, (_iid, tags, png_bytes) in enumerate(fetched[page_start:page_start + rows * cols]):
            row, col = divmod(pos, cols)
            x = margin + col * (cell_width + gap)
            y = page_height - margin - footer - (row + 1) * cell_height - row * gap
            with Image.open(io.BytesIO(png_bytes)) as image:
                image = image.convert("RGB")
                source_w, source_h = image.size
                image.thumbnail((int(cell_width), int(cell_height)), Image.Resampling.LANCZOS)
                image_buffer = io.BytesIO()
                image.save(image_buffer, format="JPEG", quality=90, optimize=True)
                image_buffer.seek(0)
                draw_w, draw_h = image.size
            draw_x = x + (cell_width - draw_w) / 2
            draw_y = y + (cell_height - draw_h) / 2
            pdf.drawImage(ImageReader(image_buffer), draw_x, draw_y, draw_w, draw_h, mask="auto")

            patient_name = (tags.get("PatientName") or "").replace("^", " ").strip()
            patient_id = tags.get("PatientID") or ""
            accession = tags.get("AccessionNumber") or ""
            timestamp = f"{tags.get('StudyDate', '')} {tags.get('StudyTime', '')}".strip()
            sex = tags.get("PatientSex") or "—"
            series_no = tags.get("SeriesNumber") or "—"
            instance_no = tags.get("InstanceNumber") or "—"
            pdf.setFillColorRGB(1, 1, 1)
            pdf.setFont("Helvetica", 8.5)
            pdf.drawString(x + 5, y + cell_height - 12, patient_name[:32])
            pdf.drawString(x + 5, y + cell_height - 22, f"Patient ID: {patient_id}"[:32])
            pdf.drawString(x + 5, y + cell_height - 32, f"Acc: {accession}"[:32])
            pdf.drawRightString(x + cell_width - 5, y + cell_height - 12, timestamp[:24])
            pdf.drawRightString(x + cell_width - 5, y + cell_height - 22, f"Sex: {sex}")
            pdf.drawRightString(x + cell_width - 5, y + cell_height - 32, f"Series {series_no}")
            pdf.setFont("Helvetica", 8)
            pdf.drawString(x + 5, y + 6, f"Instance {instance_no}")

        pdf.setFillColorRGB(1, 1, 1)
        pdf.setFont("Helvetica", 9)
        pdf.drawCentredString(page_width / 2, 12, "SDRC Diagnostics | sdrc.in")
        pdf.showPage()
    pdf.save()
    return output_path

def find_todays_ct_studies(orthanc):
    today = datetime.now().strftime("%Y%m%d")
    study_ids = orthanc.find_studies({"StudyDate": today, "ModalitiesInStudy": "CT"})
    return study_ids or []


def _dicom_int(value, default=10**9):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _study_series_instances(orthanc, study):
    """Return CT series in SeriesNumber order and instances in InstanceNumber order."""
    series_rows = []
    for series_id in study.get("Series", []):
        series = orthanc.get_series(series_id)
        tags = series.get("MainDicomTags") or {}
        if tags.get("Modality") != "CT":
            continue
        instance_rows = []
        for instance_id in series.get("Instances", []):
            instance_tags = orthanc.get_simplified_tags(instance_id)
            instance_rows.append((
                _dicom_int(instance_tags.get("InstanceNumber")), instance_id
            ))
        instance_rows.sort(key=lambda pair: (pair[0], pair[1]))
        series_rows.append((
            _dicom_int(tags.get("SeriesNumber")), series_id,
            [instance_id for _, instance_id in instance_rows]
        ))
    series_rows.sort(key=lambda row: (row[0], row[1]))
    return {series_id: instance_ids for _, series_id, instance_ids in series_rows}

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
    """Return selected images in SeriesNumber/InstanceNumber ascending order.
    SelectedForReport controls inclusion; DICOM numbering controls output order."""
    result = {}
    for series_id, instance_ids in _study_series_instances(orthanc, study).items():
        selected = [iid for iid in instance_ids if _selected_order(orthanc, iid) > 0]
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
def _is_scout_series(series):
    tags = series.get("MainDicomTags") or {}
    text = " ".join(str(tags.get(key) or "") for key in ("SeriesDescription", "ProtocolName", "SequenceName")).upper()
    return any(token in text for token in ("SCOUT", "LOCALIZER", "TOPOGRAM"))


def _write_pdf(magick_path, image_paths, pdf_path):
    subprocess.run([magick_path, "-density", "150", *image_paths, pdf_path], check=True, capture_output=True)
    return os.path.getsize(pdf_path)


def _split_series_pdf(annotated, series_id, base_path, max_bytes, magick_path, footer_text="SDRC Diagnostics | sdrc.in"):
    """Return ordered PDFs for one series, subdividing only if necessary."""
    if not annotated:
        return []
    candidate = base_path + ".pdf"
    page = base_path + ".jpg"
    # Keep the same fixed portrait 14x17 CT page in the oversized-PDF fallback.
    build_ct_page(annotated, page, magick_path, footer_text)
    size = _write_pdf(magick_path, [page], candidate)
    if size <= max_bytes:
        return [candidate]
    if len(annotated) == 1:
        raise RuntimeError(
            f"CT image in series {series_id} remains {size / (1024 * 1024):.2f} MB; refusing oversized WhatsApp document"
        )
    midpoint = len(annotated) // 2
    left = _split_series_pdf(annotated[:midpoint], series_id, base_path + "_a", max_bytes, magick_path, footer_text)
    right = _split_series_pdf(annotated[midpoint:], series_id, base_path + "_b", max_bytes, magick_path, footer_text)
    return left + right


def build_group_pdfs(orthanc, group, tmp_dir, magick_path, institution_name="", footer_text="SDRC Diagnostics | sdrc.in"):
    """Build a CT PDF, splitting by series and then by image chunks if needed.

    The relay bridge limit is 15 MB; keep a 14.8 MB safety cap per PDF.
    """
    max_bytes = int(14.8 * 1024 * 1024)
    page_paths = []
    series_images = []
    for member in sorted(group, key=lambda m: m["accession"]):
        study = member["study"]
        selected_by_series = get_selected_instances_by_series(orthanc, study)
        for series_id, instance_ids in selected_by_series.items():
            series = orthanc.get_series(series_id)
            # Scout/localizer/topogram has a different aspect ratio and is a
            # planning image. Keep it as its own page, in series order, so it
            # is always page 1 when the scanner sends it first.
            series_images.append((series_id, list(instance_ids)))

    for series_index, (series_id, instance_ids) in enumerate(series_images, start=1):
        annotated = []
        for instance_id in instance_ids:
            tags = orthanc.get_simplified_tags(instance_id)
            annotated.append(cr.download_and_annotate_ct(
                orthanc, instance_id, tags, tmp_dir, magick_path, institution_name
            ))
        if annotated:
            for page_no, offset in enumerate(range(0, len(annotated), CT_GRID_ROWS * CT_GRID_COLS), start=1):
                page_annotated = annotated[offset:offset + CT_GRID_ROWS * CT_GRID_COLS]
                page_path = os.path.join(tmp_dir, f"ct_series_{series_index:02d}_{page_no:02d}_{series_id}.jpg")
                page_paths.append((series_id, page_path, page_annotated))

    if not page_paths:
        return []

    accession = group[0]["accession"]
    study = group[0]["study"]
    study_desc = cr._filename_safe((study.get("MainDicomTags") or {}).get("StudyDescription", "STUDY"))
    study_token = cr._filename_safe(group[0]["study_id"], max_len=16)
    study_date = (study.get("MainDicomTags") or {}).get("StudyDate", "")
    # One CT PDF is built per StudyInstanceUID. Accession is the outer
    # transaction boundary; PatientID is never used as a grouping key.
    full_path = os.path.join(tmp_dir, f"CT_{accession}_{study_desc}_{study_token}_{study_date}.pdf")
    for _, page_path, annotated in page_paths:
        build_ct_page(annotated, page_path, magick_path, footer_text)
    full_size = _write_pdf(magick_path, [p for _, p, _ in page_paths], full_path)
    if full_size <= max_bytes:
        log.info("CT PDF is %.2f MB; sending as one document.", full_size / (1024 * 1024))
        return [full_path]

    log.warning("CT PDF is %.2f MB; splitting into series/chunk documents.", full_size / (1024 * 1024))
    split_paths = []
    for part, (series_id, page_path, annotated) in enumerate(page_paths, start=1):
        base = os.path.join(tmp_dir, f"CT_{accession}_{study_desc}_{study_token}_{study_date}_series_{part:02d}")
        split_paths.extend(_split_series_pdf(annotated, series_id, base, max_bytes, magick_path, footer_text))
    return split_paths

def process_once(cfg, orthanc):
    """
    Mirrors cr.process_once()'s overall shape (group by accession, lock,
    build, send, mark) with one key gating difference: a CT study is only
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

    groups = cr.group_by_accession(orthanc, study_ids)
    log.info("Found %d CT studies in %d accession groups.", len(study_ids), len(groups))

    sent_count = 0
    for accession, group in groups.items():
        log_prefix = f"[CT Accession={accession}] "

        # Accession remains the only outer boundary. Each StudyInstanceUID
        # inside that accession has its own status and PDF, so one completed
        # study can never suppress a sibling study.
        pending = []
        for member in group:
            status = orthanc.get_metadata(
                member["study_id"], "WhatsappStatus", "", raise_on_error=True
            )
            attempts_raw = orthanc.get_metadata(
                member["study_id"], "WhatsappAttempts", "0", raise_on_error=True
            )
            try:
                attempts = int(attempts_raw or 0)
            except (TypeError, ValueError):
                attempts = 0
            member["delivery_status"] = status
            member["delivery_attempts"] = attempts
            if status == "SENT":
                log.info(
                    "%sStudy=%s already SENT. Skipping.",
                    log_prefix, member["study_id"],
                )
            elif status == "PROCESSING":
                log.info(
                    "%sStudy=%s currently PROCESSING. Skipping.",
                    log_prefix, member["study_id"],
                )
            elif attempts >= cr.MAX_ATTEMPTS:
                log.info(
                    "%sStudy=%s max attempts reached. Skipping.",
                    log_prefix, member["study_id"],
                )
            else:
                pending.append(member)

        if not pending:
            continue

        pending = [m for m in pending if has_any_selection(orthanc, m["study"])]
        if not pending:
            log.info(log_prefix + "Waiting on operator selection for pending study(s). Skipping for now.")
            continue

        for member in pending:
            study_desc = member.get("study_description") or (member["study"].get("MainDicomTags") or {}).get("StudyDescription", "")
            study_prefix = f"{log_prefix}[Study={study_desc or member['study_id']}] "
            cr.lock_group(orthanc, [member], member["delivery_attempts"], dry_run)
            try:
                pdf_paths = build_group_pdfs(
                    orthanc, [member], tmp_dir, magick_path, institution_name,
                    cfg.get("institution", {}).get("footer_text", "SDRC Diagnostics | sdrc.in"),
                )
                if not pdf_paths:
                    raise RuntimeError("No pages generated for this study (no selected instances found).")

                reqno = member["accession"]
                phone = core.fetch_phone_from_labit(
                    reqno, cfg["labit"]["base_url"], cfg["labit"]["dispatch_user"], cfg["labit"]["dispatch_password"]
                )
                if not phone:
                    phone = cfg["whatsapp"]["default_phone"]
                    log.warning(study_prefix + f"No phone from Labit, using default: {phone}")

                public_urls = [
                    core.upload_file_to_ftp(pdf_path, member["study_id"], cfg["ftp"], dry_run=dry_run)
                    for pdf_path in pdf_paths
                ]
                try:
                    core.push_report_link_to_labit(
                        reqno, public_urls, cfg["labit"]["base_url"], cfg["labit"]["internal_token"], dry_run=dry_run,
                    )
                except Exception as exc:
                    log.warning(study_prefix + f"push_report_link_to_labit failed: {exc}")

                patient_name = member["patient_name"] or "Patient"
                for pdf_path, public_url in zip(pdf_paths, public_urls):
                    core.send_whatsapp_document(
                        phone, patient_name, public_url, os.path.basename(pdf_path), cfg["whatsapp"], dry_run=dry_run
                    )

                cr.mark_group_sent(orthanc, [member], dry_run, public_urls, phone)
                log.info(study_prefix + f"Sent {len(pdf_paths)} CT document(s). First PDF={pdf_paths[0]}")
                sent_count += 1
                time.sleep(1)
            except Exception as exc:
                log.exception(study_prefix + f"Failed: {exc}")
                cr.mark_group_error(orthanc, [member], dry_run)

    return sent_count
