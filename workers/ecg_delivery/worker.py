#!/usr/bin/env python3
"""Parallel Tricog ECG worker.

Shadow mode is the default. It authenticates, switches through all configured
branches, discovers the same records as Mirth, and records them in a separate
state file without downloading or delivering anything. Delivery is enabled
only with --live after comparison.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from manual_send import detect_quality_flag, manual_reattach

log = logging.getLogger("ecg_delivery")
TRICOG_BASE = "https://customer.tricog.com"
VCARDIA_BASE = "https://vcardia.tricog.com"
def _request(method, url, *, headers=None, body=None, timeout=(15, 45)):
    response = requests.request(method, url, headers=headers, json=body, timeout=timeout)
    response.raise_for_status()
    return response.json() if response.content else {}


def login(username, password):
    result = _request("POST", f"{TRICOG_BASE}/api/login", headers={"appname": "CUSTOMER_PORTAL", "accept": "application/json"}, body={"username": username, "password": password})
    return result["token"]


def switch_branch(token, branch):
    result = _request("PUT", f"{TRICOG_BASE}/api/users/clinics/change", headers={"appname": "CUSTOMER_PORTAL", "token": token}, body={"centerId": branch["centerId"], "doctorId": branch["doctorId"]})
    return result.get("newToken") or result.get("token")

def _clinic_items(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("clinics", "centers", "data", "rows", "items", "result", "list"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _clinic_items(value)
            if nested:
                return nested
    return []


def discover_branches(token):
    result = _request(
        "GET",
        f"{TRICOG_BASE}/api/users/clinics",
        headers={"appname": "CUSTOMER_PORTAL", "accept": "application/json", "token": token},
    )
    branches = []
    for item in _clinic_items(result):
        if not isinstance(item, dict):
            continue
        center_id = item.get("centerId") or item.get("center_id") or item.get("centerID")
        doctor_id = item.get("doctorId") or item.get("doctor_id") or item.get("doctorID")
        if isinstance(item.get("doctor"), dict):
            doctor_id = doctor_id or item["doctor"].get("id") or item["doctor"].get("doctorId")
        if isinstance(item.get("center"), dict):
            center_id = center_id or item["center"].get("id") or item["center"].get("centerId")
        if center_id is None or doctor_id is None:
            continue
        name = (
            item.get("centerName")
            or item.get("center_name")
            or item.get("clinicName")
            or item.get("clinic_name")
            or item.get("name")
            or item.get("title")
            or str(center_id)
        )
        branches.append({
            "centerId": str(center_id),
            "doctorId": int(doctor_id),
            "centerName": str(name),
        })
    unique = {(branch["centerId"], branch["doctorId"]): branch for branch in branches}
    if not unique:
        raise RuntimeError("Tricog clinic list returned no usable centerId/doctorId pairs")
    return list(unique.values())



def _branch_key(branch):
    return f"{branch['centerId']}:{branch['doctorId']}"


def list_recent(token, limit=25):
    filters = {"ecgFilter": {"ecgType": "RESTING", "patientId": "", "ecgStatus": [], "diagnosisStatus": [], "gender": [], "reportingType": [], "startDate": "", "endDate": ""}}
    url = f"{TRICOG_BASE}/api/ecg/all?limit={limit}&offset=0&timestamp={int(time.time() * 1000)}&filters={requests.utils.quote(json.dumps(filters))}&isSrEnabled=false"
    result = _request("GET", url, headers={"appname": "CUSTOMER_PORTAL", "accept": "application/json", "token": token})
    if isinstance(result, list):
        return result
    for key in ("data", "rows", "records", "ecgs", "result", "items"):
        if isinstance(result.get(key), list):
            return result[key]
    return []


def _timestamp(record):
    raw = record.get("acquiredon") or record.get("timeRead") or record.get("acquired") or record.get("acquiredDate")
    if not raw:
        return 0
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0


def load_state(path):
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {"seen": {}}


def save_state(path, state):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(target)


def discover(username, password, state, initial_lookback_hours=24):
    token = login(username, password)
    branches = discover_branches(token)
    found = []
    now = time.time()
    branches_state = state.setdefault("branches", {})
    for branch in branches:
        token = switch_branch(token, branch)
        records = list_recent(token)
        branch_key = _branch_key(branch)
        legacy_state = branches_state.get(branch["centerId"])
        branch_state = branches_state.setdefault(branch_key, legacy_state or {})
        seen = set(branch_state.setdefault("seen", []))
        cutoff = branch_state.get("last_synced_at")
        if cutoff:
            cutoff = datetime.fromisoformat(cutoff.replace("Z", "+00:00")).timestamp()
        else:
            cutoff = now - (initial_lookback_hours * 3600)
        for record in records:
            record_ts = _timestamp(record)
            ecg_id = record.get("ecgId") or record.get("ecgid")
            if not ecg_id or ecg_id in seen or (record_ts and record_ts < cutoff):
                continue
            # Match Mirth: do not even queue an ECG until Tricog has a
            # real diagnosis. It must remain eligible for a later poll.
            diagnosis = record.get("diagnosis")
            if diagnosis is None or (isinstance(diagnosis, str) and not diagnosis.strip()):
                continue
            if isinstance(diagnosis, (list, dict)) and not diagnosis:
                continue
            accession = str(record.get("patientId") or record.get("patientid") or "").strip()
            if not accession:
                continue
            quality_flag = detect_quality_flag(
                record.get("qualityFlag"),
                record.get("quality_flag"),
                diagnosis,
                record.get("finalclassification"),
            )
            found.append({
                "accession_no": accession,
                "tricog_ecg_id": str(ecg_id),
                "patient_name": record.get("patientName") or record.get("patientname"),
                "age": record.get("age"),
                "sex": record.get("sex"),
                "branch_center_id": branch["centerId"],
                "branch_scope_key": branch_key,
                "branch_center_name": branch["centerName"],
                "diagnosis": record.get("diagnosis"),
                "final_classification": record.get("finalclassification"),
                "status": record.get("status"),
                "quality_flag": quality_flag,
                "acquired_at": datetime.fromtimestamp(_timestamp(record), tz=timezone.utc).isoformat() if _timestamp(record) else None,
                "raw_json": {
                    **record,
                    "pdfUrl": f"{VCARDIA_BASE}/api/v2/ecg/{ecg_id}/report?resType=pdf",
                    "tricogToken": token,
                    "tricogCenterId": branch["centerId"],
                    "tricogCenterName": branch["centerName"],
                    "tricogEcgId": str(ecg_id),
                    **({"qualityFlag": quality_flag} if quality_flag else {}),
                },
            })
        # The watermark is advanced only after a row is successfully handled
        # in run_once(). This prevents transient delivery failures or missing
        # Tricog diagnoses from being skipped permanently.
    found.sort(key=lambda row: row.get("acquired_at") or "")
    return found


def run_once(args, cfg):
    state = load_state(args.state_file)
    rows = discover(args.username, args.password, state, args.initial_lookback_hours)
    log.info("Discovered %d new ECG record(s); mode=%s", len(rows), "live" if args.live else "shadow")
    for row in rows:
        log.info("ECG candidate accession=%s ecg_id=%s diagnosis=%s", row["accession_no"], row["tricog_ecg_id"], bool(row.get("diagnosis")))
        if args.live:
            try:
                result = manual_reattach(cfg, row, send_whatsapp=True)
                log.info("ECG delivered accession=%s stages=%s", row["accession_no"], result["stages"])
            except Exception:
                log.exception("ECG delivery failed accession=%s; leaving it unmarked for retry", row["accession_no"])
                continue
        branch_state = state.setdefault("branches", {}).setdefault(row.get("branch_scope_key", row["branch_center_id"]), {})
        branch_state.setdefault("seen", []).append(row["tricog_ecg_id"])
        branch_state["seen"] = branch_state["seen"][-300:]
        record_ts = _timestamp(row.get("raw_json") or {})
        if record_ts:
            current = branch_state.get("last_synced_at")
            current_ts = datetime.fromisoformat(current.replace("Z", "+00:00")).timestamp() if current else 0
            if record_ts > current_ts:
                branch_state["last_synced_at"] = datetime.fromtimestamp(record_ts, tz=timezone.utc).isoformat()
    save_state(args.state_file, state)
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delivery-config", required=True)
    parser.add_argument("--state-file", default="/opt/labbit-utils/workers/ecg_delivery/state/python_tricog_state.json")
    parser.add_argument("--username", default=os.environ.get("TRICOG_USERNAME", ""))
    parser.add_argument("--password", default=os.environ.get("TRICOG_PASSWORD", ""))
    parser.add_argument("--initial-lookback-hours", type=int, default=24)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if not args.username or not args.password:
        raise SystemExit("TRICOG_USERNAME and TRICOG_PASSWORD are required")
    cfg = json.loads(Path(args.delivery_config).read_text())
    cfg["supabase_url"] = os.environ.get("SUPABASE_URL", "")
    cfg["supabase_service_key"] = os.environ.get("SUPABASE_SERVICE_KEY", "")
    cfg["ecg"] = cfg.get("ecg") or {}
    logging.basicConfig(level=cfg.get("worker", {}).get("log_level", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    while True:
        try:
            run_once(args, cfg)
        except Exception:
            log.exception("ECG worker cycle failed")
        if args.once:
            break
        time.sleep(int(cfg.get("ecg", {}).get("poll_seconds", 120)))


if __name__ == "__main__":
    main()
