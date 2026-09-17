#!/usr/bin/env python3
import argparse
import json
import logging
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import core
import cr

log = logging.getLogger("dicom_export")


def load_config(path):
    with open(path) as f:
        return json.load(f)


def build_orthanc(cfg):
    return core.OrthancClient(cfg["orthanc"]["base_url"], cfg["orthanc"]["user"], cfg["orthanc"]["password"])


def list_studies_for_date(orthanc, date_str):
    """Matches the dashboard's LIST contract: one row per study, with
    WhatsApp status/attempts/timestamp read from Orthanc metadata."""
    study_ids = orthanc.find_studies({"StudyDate": date_str}) or []
    rows = []
    for study_id in study_ids:
        try:
            study = orthanc.get_study(study_id)
        except Exception as exc:
            log.warning("LIST: could not fetch study %s: %s", study_id, exc)
            continue
        main_tags = study.get("MainDicomTags") or {}
        patient_tags = study.get("PatientMainDicomTags") or {}

        modality = None
        for series_id in study.get("Series", []):
            try:
                series = orthanc.get_series(series_id)
                modality = (series.get("MainDicomTags") or {}).get("Modality")
                if modality:
                    break
            except Exception:
                continue

        rows.append(
            {
                "studyId": study_id,
                "accession": main_tags.get("AccessionNumber", ""),
                "patientName": (patient_tags.get("PatientName") or "").replace("^", " ").strip(),
                "phone": orthanc.get_metadata(study_id, "WhatsappPhone", ""),
                "status": orthanc.get_metadata(study_id, "WhatsappStatus", ""),
                "attempts": orthanc.get_metadata(study_id, "WhatsappAttempts", "0"),
                "timestamp": orthanc.get_metadata(study_id, "WhatsappTimestamp", ""),
                "pdfUrl": orthanc.get_metadata(study_id, "WhatsappPdfUrl", ""),
                "modality": modality or "",
            }
        )
    return rows


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

                self._send_json({"error": "unrecognized request shape"}, 400)
            except Exception as exc:
                log.exception("HTTP handler error: %s", exc)
                self._send_json({"error": str(exc)}, 500)

    return Handler


def run_http_server(cfg, orthanc):
    port = cfg["http"]["port"]
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(cfg, orthanc))
    log.info("HTTP API listening on 0.0.0.0:%d (LIST / manualsend, dry_run=%s)", port, cfg["dry_run"])
    server.serve_forever()


def run_poll_loop(cfg, orthanc):
    poll_seconds = cfg["worker"]["poll_seconds"]
    log.info("Starting CR export poll loop. poll_seconds=%s dry_run=%s", poll_seconds, cfg["dry_run"])
    while True:
        try:
            sent = cr.process_once(cfg, orthanc)
            if sent:
                log.info("Processed %d CR group(s) this cycle.", sent)
        except Exception as exc:
            log.exception("Poll loop error: %s", exc)
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
