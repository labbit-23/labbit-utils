#!/usr/bin/env python3
import argparse
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import core
import cr
import ct

# Caps concurrent Orthanc calls from the LIST endpoint. Each per-study
# lookup is ~500ms of mostly Orthanc-side work (confirmed live -- not our
# round-trip count, Orthanc itself is slow to compute per-study), so
# running them one at a time made LIST take ~15s for one day's studies.
# Bounded, not unlimited, so this can't repeat tonight's other incident
# (the CT thumbnail grid firing 150+ concurrent Orthanc requests with no
# cap and getting connections refused) -- 8 is enough to get most of the
# parallelism benefit without hammering the same box.
LIST_CONCURRENCY = 8

# Caps concurrent thumbnail-proxy fetches to Orthanc. Found live tonight:
# opening a large CT study's selection grid fired 150+ near-simultaneous
# thumbnail requests (ThreadingHTTPServer gives each incoming request its
# own thread, so nothing bounded this), and Orthanc's DICOM box actually
# refused connections under that burst. This semaphore queues extra
# requests instead of letting them all hit Orthanc at once -- browser
# requests wait briefly rather than the server being overwhelmed.
_thumbnail_semaphore = threading.Semaphore(6)

log = logging.getLogger("dicom_export")

# Shared between the poll-loop thread and the HTTP server thread, read by
# GET /health for external monitoring (labbit-py's services.local.ini).
# Plain dict + lock rather than anything fancier -- one writer (the poll
# loop), occasional readers, low contention.
_state_lock = threading.Lock()
_worker_state = {
    "started_at": None,
    "last_poll_at": None,
    "last_poll_ok": None,
    "last_poll_error": None,
    "cr_sent_total": 0,
    "ct_sent_total": 0,
}


def load_config(path):
    with open(path) as f:
        return json.load(f)


def build_orthanc(cfg):
    return core.OrthancClient(cfg["orthanc"]["base_url"], cfg["orthanc"]["user"], cfg["orthanc"]["password"])


def _fetch_study_row(orthanc, study_id):
    """One study's worth of work for list_studies_for_date, split out so
    it can run in a thread pool -- each call is mostly Orthanc-side wait
    time (I/O), not CPU work here, so threads (not async) are a fine fit.
    Returns None if the study couldn't be fetched (logged, skipped)."""
    try:
        # requestedTags=ModalitiesInStudy gets modality in this same call
        # -- replaces a per-series lookup loop that was the single
        # biggest cost per study (confirmed live: ~600ms for a 2-series
        # study, since /series/{id} returns each series' full Instances
        # array just to read one Modality field).
        study = orthanc.get_study(study_id, requested_tags=["ModalitiesInStudy"])
    except Exception as exc:
        log.warning("LIST: could not fetch study %s: %s", study_id, exc)
        return None
    main_tags = study.get("MainDicomTags") or {}
    patient_tags = study.get("PatientMainDicomTags") or {}
    # ModalitiesInStudy can list more than one modality (space-separated)
    # if a study genuinely mixes them; take the first -- every real study
    # seen in this system so far is single-modality.
    modality = (study.get("RequestedTags") or {}).get("ModalitiesInStudy", "").split()
    modality = modality[0] if modality else ""

    # One call for all 5 keys instead of 5 separate calls -- see
    # get_all_metadata's docstring.
    meta = orthanc.get_all_metadata(study_id)
    return {
        "studyId": study_id,
        "accession": main_tags.get("AccessionNumber", ""),
        "patientName": (patient_tags.get("PatientName") or "").replace("^", " ").strip(),
        "phone": meta.get("WhatsappPhone", ""),
        "status": meta.get("WhatsappStatus", ""),
        "attempts": meta.get("WhatsappAttempts", "0"),
        "timestamp": meta.get("WhatsappTimestamp", ""),
        "pdfUrl": meta.get("WhatsappPdfUrl", ""),
        "modality": modality,
    }


def list_studies_for_date(orthanc, date_str):
    """Matches the dashboard's LIST contract: one row per study, with
    WhatsApp status/attempts/timestamp read from Orthanc metadata.

    Fetches studies concurrently (bounded, see LIST_CONCURRENCY) -- each
    per-study lookup is dominated by Orthanc's own response time (~500ms,
    confirmed live, not our round-trip count), so running them one at a
    time made a single day's LIST take ~15s. requests.Session (used by
    OrthancClient) is documented thread-safe for concurrent calls, so one
    shared client across the pool is the correct/standard approach here,
    not a separate client per thread."""
    study_ids = orthanc.find_studies({"StudyDate": date_str}) or []
    if not study_ids:
        return []
    with ThreadPoolExecutor(max_workers=LIST_CONCURRENCY) as pool:
        results = list(pool.map(lambda sid: _fetch_study_row(orthanc, sid), study_ids))
    return [row for row in results if row is not None]


def get_study_instances_payload(orthanc, study_id):
    """CT selection UI's data source: every series/instance in a study,
    each instance's current SelectedForReport order (0 = unselected)."""
    study = orthanc.get_study(study_id)
    series_out = []
    for series_id in study.get("Series", []):
        series = orthanc.get_series(series_id)
        tags = series.get("MainDicomTags") or {}
        instances = []
        for instance_id in series.get("Instances", []):
            order = ct._selected_order(orthanc, instance_id)
            instances.append({"instanceId": instance_id, "selectedOrder": order})
        series_out.append(
            {
                "seriesId": series_id,
                "seriesNumber": tags.get("SeriesNumber", ""),
                "seriesDescription": tags.get("SeriesDescription", ""),
                "modality": tags.get("Modality", ""),
                "instances": instances,
            }
        )
    return {"studyId": study_id, "series": series_out}


def save_selection(orthanc, cfg, study_id, selected_instance_ids):
    """Writes SelectedForReport = 1-based position for every instance in
    selected_instance_ids (the operator's chosen order), and clears it
    from any instance in this study that was previously selected but is
    no longer in the list. dry_run always respected."""
    dry_run = cfg["dry_run"]
    study = orthanc.get_study(study_id)
    all_instance_ids = set()
    for series_id in study.get("Series", []):
        series = orthanc.get_series(series_id)
        all_instance_ids.update(series.get("Instances", []))

    selected_set = set(selected_instance_ids)
    for iid in all_instance_ids - selected_set:
        if ct._selected_order(orthanc, iid) > 0:
            orthanc.delete_metadata(iid, "SelectedForReport", dry_run=dry_run, resource_type="instances")

    for position, iid in enumerate(selected_instance_ids, start=1):
        orthanc.put_metadata(iid, "SelectedForReport", position, dry_run=dry_run, resource_type="instances")

    return {"ok": True, "studyId": study_id, "selectedCount": len(selected_instance_ids)}


def make_handler(cfg, orthanc):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            log.info("HTTP %s - %s", self.address_string(), fmt % args)

        def _send_json(self, payload, status=200):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if self.path != "/api/dicom":
                self._send_json({"error": "not found"}, 404)
                return
            length = int(self.headers.get("Content-Length", 0))
            try:
                data = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError:
                self._send_json({"error": "invalid JSON"}, 400)
                return

            try:
                if data.get("action") == "LIST":
                    date_str = data.get("date") or datetime.now().strftime("%Y%m%d")
                    rows = list_studies_for_date(orthanc, date_str)
                    self._send_json(rows)
                    return

                if data.get("sendMode") == "MANUAL":
                    accession = data.get("accession")
                    phone = data.get("phone")
                    if not accession:
                        self._send_json({"error": "accession is required"}, 400)
                        return
                    result = cr.manual_send(cfg, orthanc, accession, phone)
                    self._send_json(result)
                    return

                if data.get("action") == "GET_STUDY_INSTANCES":
                    study_id = data.get("studyId")
                    if not study_id:
                        self._send_json({"error": "studyId is required"}, 400)
                        return
                    self._send_json(get_study_instances_payload(orthanc, study_id))
                    return

                if data.get("action") == "SAVE_SELECTION":
                    study_id = data.get("studyId")
                    selected = data.get("selectedInstanceIds")
                    if not study_id or not isinstance(selected, list):
                        self._send_json({"error": "studyId and selectedInstanceIds[] are required"}, 400)
                        return
                    self._send_json(save_selection(orthanc, cfg, study_id, selected))
                    return

                self._send_json({"error": "unrecognized request shape"}, 400)
            except Exception as exc:
                log.exception("HTTP handler error: %s", exc)
                self._send_json({"error": str(exc)}, 500)

        def do_GET(self):
            if self.path == "/health":
                # For labbit-py's monitoring (services.local.ini, type=http_json)
                # -- mirrors what the Mirth channel checks already surface
                # (state/received/sent/errors), scoped to what's meaningful
                # for a poll-loop worker: is it alive, when did it last run
                # a cycle, did that cycle succeed, how many real sends so
                # far since this process started.
                with _state_lock:
                    state = dict(_worker_state)
                state["dry_run"] = cfg["dry_run"]
                state["ct_enabled"] = cfg.get("ct", {}).get("enabled", False)
                self._send_json(state)
                return
            # Thumbnail proxy: the browser can't reach Orthanc directly
            # (different network, and doing so would mean shipping Orthanc
            # credentials client-side) -- so the CT selection grid's <img>
            # tags point here, and this fetches the bytes from Orthanc
            # server-side using the worker's own credentials.
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/dicom-thumbnail/"):
                instance_id = parsed.path.rsplit("/", 1)[-1]
                with _thumbnail_semaphore:
                    try:
                        content, content_type = orthanc.get_preview_png(instance_id)
                    except Exception as exc:
                        log.warning("Thumbnail proxy failed for instance=%s: %s", instance_id, exc)
                        self.send_response(502)
                        self.end_headers()
                        return
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "public, max-age=3600")
                self.end_headers()
                self.wfile.write(content)
                return
            self.send_response(404)
            self.end_headers()

    return Handler


def run_http_server(cfg, orthanc):
    port = cfg["http"]["port"]
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(cfg, orthanc))
    log.info("HTTP API listening on 0.0.0.0:%d (LIST / manualsend, dry_run=%s)", port, cfg["dry_run"])
    server.serve_forever()


def run_poll_loop(cfg, orthanc):
    poll_seconds = cfg["worker"]["poll_seconds"]
    # CT has its OWN activation flag (cfg["ct"]["enabled"], default False),
    # deliberately separate from cfg["dry_run"]. Found live on 2026-09-17:
    # ct.process_once() was committed and wired into this loop unconditionally,
    # sharing CR's dry_run -- the moment CR's own approved go-live set
    # dry_run=False, a later unrelated restart (to deploy an unrelated fix)
    # silently activated real CT sends too, with no explicit approval step of
    # its own. CT must never again be able to piggyback on CR's dry_run state.
    ct_enabled = cfg.get("ct", {}).get("enabled", False)
    log.info(
        "Starting export poll loop. poll_seconds=%s dry_run=%s ct_enabled=%s",
        poll_seconds, cfg["dry_run"], ct_enabled,
    )
    with _state_lock:
        _worker_state["started_at"] = datetime.now().isoformat()

    while True:
        cycle_ok = True
        cycle_error = None
        try:
            sent = cr.process_once(cfg, orthanc)
            if sent:
                log.info("Processed %d CR group(s) this cycle.", sent)
                with _state_lock:
                    _worker_state["cr_sent_total"] += sent
        except Exception as exc:
            log.exception("CR poll loop error: %s", exc)
            cycle_ok = False
            cycle_error = f"CR: {exc}"
        if ct_enabled:
            try:
                sent = ct.process_once(cfg, orthanc)
                if sent:
                    log.info("Processed %d CT group(s) this cycle.", sent)
                    with _state_lock:
                        _worker_state["ct_sent_total"] += sent
            except Exception as exc:
                log.exception("CT poll loop error: %s", exc)
                cycle_ok = False
                cycle_error = (cycle_error + " | " if cycle_error else "") + f"CT: {exc}"

        with _state_lock:
            _worker_state["last_poll_at"] = datetime.now().isoformat()
            _worker_state["last_poll_ok"] = cycle_ok
            _worker_state["last_poll_error"] = cycle_error

        time.sleep(poll_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="./config/dicom_export.json")
    args = parser.parse_args()

    cfg = load_config(args.config)
    core.setup_logging(cfg["worker"].get("log_dir", "./logs"), cfg["worker"].get("log_level", "INFO"))
    log.info("Loaded config from %s. dry_run=%s", args.config, cfg["dry_run"])

    orthanc = build_orthanc(cfg)

    http_thread = threading.Thread(target=run_http_server, args=(cfg, orthanc), daemon=True)
    http_thread.start()

    run_poll_loop(cfg, orthanc)
