#!/usr/bin/env python3
import argparse
import base64
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def dicom_date_time_pair(raw_value: str) -> Tuple[str, str]:
    text = str(raw_value or "").strip()
    if not text:
        now = datetime.now()
        return now.strftime("%Y%m%d"), now.strftime("%H%M%S")

    # Supports: YYYY-MM-DDTHH:MM[:SS], YYYY-MM-DD HH:MM[:SS], YYYYMMDDHHMMSS
    try:
        if "T" in text or " " in text:
            norm = text.replace(" ", "T")
            if len(norm) == 16:
                norm = norm + ":00"
            dt = datetime.fromisoformat(norm)
            return dt.strftime("%Y%m%d"), dt.strftime("%H%M%S")
        if len(text) >= 14 and text.isdigit():
            return text[:8], text[8:14]
    except Exception:
        pass

    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H%M%S")


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_path(data: Any, path: str, default: Any = None) -> Any:
    if not path:
        return data
    cur = data
    for token in str(path).split("."):
        if isinstance(cur, list):
            try:
                idx = int(token)
            except ValueError:
                return default
            if idx < 0 or idx >= len(cur):
                return default
            cur = cur[idx]
        elif isinstance(cur, dict):
            if token not in cur:
                return default
            cur = cur[token]
        else:
            return default
    return cur


class StateStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            create table if not exists mwl_dispatch_state (
              id integer primary key autoincrement,
              accession_number text not null,
              modality text not null,
              source_row_id text,
              status text not null,
              attempts integer not null default 0,
              last_error text,
              response_json text,
              mwl_file_path text,
              first_seen_at text not null,
              sent_at text,
              updated_at text not null,
              unique(accession_number, modality)
            );
            """
        )
        self.conn.commit()

    def was_sent(self, accession_number: str, modality: str) -> bool:
        row = self.conn.execute(
            """
            select status from mwl_dispatch_state
            where accession_number = ? and modality = ?
            """,
            (accession_number, modality),
        ).fetchone()
        return bool(row and row["status"] == "sent")

    def upsert_attempt(
        self,
        accession_number: str,
        modality: str,
        source_row_id: Optional[str],
        status: str,
        mwl_file_path: str,
        response_obj: Optional[Dict[str, Any]] = None,
        error_text: str = "",
    ) -> None:
        now = utc_now_iso()
        existing = self.conn.execute(
            """
            select id, attempts from mwl_dispatch_state
            where accession_number = ? and modality = ?
            """,
            (accession_number, modality),
        ).fetchone()

        response_json = json.dumps(response_obj or {}, ensure_ascii=False)

        if existing:
            attempts = int(existing["attempts"] or 0) + 1
            sent_at = now if status == "sent" else None
            self.conn.execute(
                """
                update mwl_dispatch_state
                set source_row_id = ?,
                    status = ?,
                    attempts = ?,
                    last_error = ?,
                    response_json = ?,
                    mwl_file_path = ?,
                    sent_at = coalesce(?, sent_at),
                    updated_at = ?
                where id = ?
                """,
                (
                    source_row_id,
                    status,
                    attempts,
                    error_text,
                    response_json,
                    mwl_file_path,
                    sent_at,
                    now,
                    int(existing["id"]),
                ),
            )
        else:
            sent_at = now if status == "sent" else None
            self.conn.execute(
                """
                insert into mwl_dispatch_state (
                  accession_number, modality, source_row_id, status,
                  attempts, last_error, response_json, mwl_file_path,
                  first_seen_at, sent_at, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    accession_number,
                    modality,
                    source_row_id,
                    status,
                    1,
                    error_text,
                    response_json,
                    mwl_file_path,
                    now,
                    sent_at,
                    now,
                ),
            )
        self.conn.commit()


class MWLWorker:
    def __init__(self, config: Dict[str, Any], dry_run: bool = False) -> None:
        self.cfg = config
        self.dry_run = dry_run
        self.session = requests.Session()

        worker = self.cfg.get("worker", {})
        log_level = str(worker.get("log_level", "INFO")).upper()
        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format="%(asctime)s %(levelname)s %(message)s",
        )
        self.log = logging.getLogger("radiology_mwl_worker")

        state_db_path = str(worker.get("state_db_path", "./state/radiology_mwl_state.sqlite3")).strip()
        self.store = StateStore(state_db_path)

    def poll_items(self) -> List[Dict[str, Any]]:
        source = self.cfg["source"]
        method = str(source.get("poll_method", "GET")).upper()
        url = source["poll_url"]
        headers = source.get("poll_headers", {}) or {}
        timeout = int(source.get("poll_timeout_seconds", 20))
        items_path = source.get("items_path", "items")

        resp = self.session.request(method=method, url=url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()

        if isinstance(body, list):
            return body

        items = get_path(body, items_path, [])
        if not isinstance(items, list):
            raise ValueError(f"Source response path '{items_path}' is not a list")
        return items

    def pending_items(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        source = self.cfg["source"]
        performed_field = source.get("performed_field", "performed")
        pending_value = source.get("pending_value", 0)
        batch_size = int(self.cfg.get("worker", {}).get("batch_size", 50))

        pending = []
        for row in rows:
            if get_path(row, performed_field) == pending_value:
                pending.append(row)
            if len(pending) >= batch_size:
                break
        return pending

    def extract_keys(self, row: Dict[str, Any]) -> Tuple[Optional[str], str, str]:
        source = self.cfg["source"]
        id_field = source.get("id_field", "id")
        row_id = get_path(row, id_field)

        mapping = self.cfg.get("mwl", {}).get("payload_fields", {}) or {}
        accession = str(get_path(row, mapping.get("accession_number", "reqno"), "")).strip()
        modality = str(
            get_path(row, mapping.get("modality", "modality"), "")
            or self.cfg.get("mwl", {}).get("defaults", {}).get("modality", "US")
        ).strip()

        return (None if row_id in (None, "") else str(row_id), accession, modality)

    def build_mwl_payload(self, row: Dict[str, Any]) -> Dict[str, Any]:
        mwl_cfg = self.cfg["mwl"]
        dest = self.cfg["destination"]
        defaults = dict(mwl_cfg.get("defaults", {}) or {})
        mapping = mwl_cfg.get("payload_fields", {}) or {}

        payload: Dict[str, Any] = {
            "destination": {
                "aet": dest["aet"],
                "host": dest["host"],
                "port": int(dest["port"]),
            }
        }
        payload.update(defaults)

        for target_key, row_field_path in mapping.items():
            payload[target_key] = get_path(row, str(row_field_path), payload.get(target_key))

        payload["raw"] = row
        return payload

    def build_mwl_dataset(self, payload: Dict[str, Any]) -> FileDataset:
        accession = str(payload.get("accession_number") or "").strip()
        patient_id = str(payload.get("patient_id") or "").strip()
        patient_name = str(payload.get("patient_name") or "UNKNOWN").strip() or "UNKNOWN"
        modality = str(payload.get("modality") or "US").strip() or "US"
        sched_raw = str(payload.get("scheduled_datetime") or "").strip()
        sched_date, sched_time = dicom_date_time_pair(sched_raw)

        file_meta = FileMetaDataset()
        file_meta.FileMetaInformationVersion = b"\x00\x01"
        file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.31"  # MWL Information Model - FIND
        file_meta.MediaStorageSOPInstanceUID = generate_uid()
        file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
        file_meta.ImplementationClassUID = generate_uid()

        ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\x00" * 128)
        ds.is_little_endian = True
        ds.is_implicit_VR = False

        now = datetime.now()
        ds.SpecificCharacterSet = "ISO_IR 100"
        ds.AccessionNumber = accession
        ds.PatientName = patient_name
        ds.PatientID = patient_id
        ds.PatientSex = ""
        ds.PatientBirthDate = ""
        ds.StudyInstanceUID = generate_uid()
        ds.RequestedProcedureID = accession or generate_uid()
        ds.RequestedProcedureDescription = str(payload.get("requested_procedure_description") or "RADIOLOGY")
        ds.ReferringPhysicianName = str(payload.get("referring_physician_name") or "")
        ds.InstitutionName = str(payload.get("institution_name") or "SDRC")
        ds.StationAETitle = str(self.cfg.get("destination", {}).get("aet") or "")

        sps_item = Dataset()
        sps_item.Modality = modality
        sps_item.ScheduledStationAETitle = str(self.cfg.get("destination", {}).get("aet") or "")
        sps_item.ScheduledProcedureStepStartDate = sched_date
        sps_item.ScheduledProcedureStepStartTime = sched_time
        sps_item.ScheduledProcedureStepDescription = str(payload.get("scheduled_step_description") or "RADIOLOGY")
        sps_item.ScheduledProcedureStepID = accession or generate_uid()
        sps_item.ScheduledPerformingPhysicianName = str(payload.get("performing_physician_name") or "")

        ds.ScheduledProcedureStepSequence = [sps_item]
        ds.ContentDate = now.strftime("%Y%m%d")
        ds.ContentTime = now.strftime("%H%M%S")

        return ds

    def write_mwl_file(self, ds: FileDataset, accession: str, modality: str) -> Tuple[str, bytes]:
        outbox = str(self.cfg.get("mwl", {}).get("outbox_dir", "./outbox")).strip() or "./outbox"
        os.makedirs(outbox, exist_ok=True)

        safe_accession = "".join(ch for ch in accession if ch.isalnum() or ch in ("-", "_")) or "NOACC"
        safe_modality = "".join(ch for ch in modality if ch.isalnum() or ch in ("-", "_")) or "MOD"
        filename = f"MWL_{safe_accession}_{safe_modality}.dcm"
        full_path = os.path.join(outbox, filename)

        ds.save_as(full_path, write_like_original=False)
        with open(full_path, "rb") as f:
            data = f.read()
        return full_path, data

    def send_to_plugin(self, payload: Dict[str, Any], mwl_file_path: str, mwl_bytes: bytes) -> Dict[str, Any]:
        mwl_cfg = self.cfg["mwl"]
        method = str(mwl_cfg.get("create_method", "POST")).upper()
        url = str(mwl_cfg.get("create_url") or "").strip()
        headers = mwl_cfg.get("create_headers", {}) or {}
        timeout = int(mwl_cfg.get("timeout_seconds", 20))
        transport = str(mwl_cfg.get("transport", "legacy_json")).strip().lower()

        if not url:
            return {"ok": True, "local_only": True, "mwl_file_path": mwl_file_path}

        if self.dry_run:
            self.log.info("[dry-run] transport=%s url=%s payload=%s", transport, url, json.dumps(payload, ensure_ascii=False))
            return {"ok": True, "dry_run": True, "transport": transport, "mwl_file_path": mwl_file_path}

        if transport == "multipart_file":
            with open(mwl_file_path, "rb") as f:
                files = {
                    "file": (os.path.basename(mwl_file_path), f, "application/dicom")
                }
                data = {
                    "accession_number": str(payload.get("accession_number") or ""),
                    "patient_id": str(payload.get("patient_id") or ""),
                    "patient_name": str(payload.get("patient_name") or ""),
                    "modality": str(payload.get("modality") or ""),
                }
                resp = self.session.request(method=method, url=url, headers=headers, data=data, files=files, timeout=timeout)
        elif transport == "json_base64":
            body = dict(payload)
            body["mwl_file_name"] = os.path.basename(mwl_file_path)
            body["mwl_dicom_base64"] = base64.b64encode(mwl_bytes).decode("ascii")
            resp = self.session.request(method=method, url=url, headers=headers, json=body, timeout=timeout)
        else:
            # legacy_json -> existing endpoint contract
            body = dict(payload)
            if bool(mwl_cfg.get("include_local_file_path", True)):
                body["mwl_file_path"] = mwl_file_path
            if bool(mwl_cfg.get("include_dicom_base64", False)):
                body["mwl_dicom_base64"] = base64.b64encode(mwl_bytes).decode("ascii")
            resp = self.session.request(method=method, url=url, headers=headers, json=body, timeout=timeout)

        resp.raise_for_status()
        if not resp.content:
            return {"ok": True, "status_code": resp.status_code}
        try:
            return resp.json()
        except Exception:
            return {"ok": True, "status_code": resp.status_code, "raw_response": resp.text[:2000]}

    def process_once(self) -> int:
        rows = self.poll_items()
        pending = self.pending_items(rows)

        if not pending:
            self.log.info("No pending rows.")
            return 0

        self.log.info("Pending rows from source: %s", len(pending))
        success_count = 0

        for row in pending:
            row_id, accession, modality = self.extract_keys(row)
            if not accession:
                self.log.warning("Skipping row id=%s due to missing accession_number", row_id)
                continue

            if self.store.was_sent(accession, modality):
                self.log.info("Already sent earlier, skipping accession=%s modality=%s", accession, modality)
                continue

            try:
                payload = self.build_mwl_payload(row)
                ds = self.build_mwl_dataset(payload)
                mwl_file_path, mwl_bytes = self.write_mwl_file(ds, accession, modality)
                result = self.send_to_plugin(payload, mwl_file_path, mwl_bytes)

                self.store.upsert_attempt(
                    accession_number=accession,
                    modality=modality,
                    source_row_id=row_id,
                    status="sent",
                    mwl_file_path=mwl_file_path,
                    response_obj=result,
                )
                success_count += 1
                self.log.info("Sent MWL accession=%s modality=%s source_row_id=%s file=%s", accession, modality, row_id, mwl_file_path)
            except Exception as exc:
                err = str(exc)
                self.log.exception("Failed accession=%s modality=%s source_row_id=%s: %s", accession, modality, row_id, err)
                self.store.upsert_attempt(
                    accession_number=accession,
                    modality=modality,
                    source_row_id=row_id,
                    status="failed",
                    mwl_file_path="",
                    response_obj={},
                    error_text=err,
                )

        return success_count

    def run_forever(self) -> None:
        poll_seconds = int(self.cfg.get("worker", {}).get("poll_seconds", 15))
        self.log.info("Starting radiology MWL worker. poll_seconds=%s dry_run=%s", poll_seconds, self.dry_run)
        while True:
            try:
                self.process_once()
            except Exception as exc:
                self.log.exception("Loop error: %s", exc)
            time.sleep(poll_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Radiology MWL worker")
    parser.add_argument("--config", required=True, help="Path to worker config JSON")
    parser.add_argument("--once", action="store_true", help="Run one polling cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="Do not call plugin endpoint")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_json(args.config)
    worker = MWLWorker(cfg, dry_run=args.dry_run)
    if args.once:
        worker.process_once()
        return 0
    worker.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
