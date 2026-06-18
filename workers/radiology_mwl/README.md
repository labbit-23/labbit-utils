# radiology_mwl worker

Polls the labbit-py department worklist for pending radiology items, generates DICOM MWL `.wl` files, sends them to Orthanc's worklist directory via HTTP, and stores local send-state in SQLite.

## Install

```bash
cd py_utils/workers/radiology_mwl
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Config

```bash
cp config/mwl_worker.example.json config/mwl_worker.json
```

Required:

- `source.poll_url` — labbit-py department-worklist endpoint (see below)
- `destination.aet`, `destination.host`, `destination.port`
- `mwl.create_url` — HTTP endpoint that writes the `.wl` file to Orthanc's worklist directory

## Source URL

Point `source.poll_url` at the labbit-py **department-worklist** endpoint for a specific department:

```
https://api.sdrc.in/py/delivery/department-worklist?department_name=radiology
https://api.sdrc.in/py/delivery/department-worklist?department_name=ct_scan
https://api.sdrc.in/py/delivery/department-worklist?department_name=sonology
```

For date-specific polling add `&fromreqdate=YYYY-MM-DD&toreqdate=YYYY-MM-DD`.

The endpoint returns `{ items: [...] }` where each item has `reqno`, `patient_id`, `patient_name`, `patient_sex`, `patient_dob`, `reqdt`, `reqtm`, `procedure_name`, `performed`.

## Modality per department (multi-instance pattern)

Run one worker instance per department+modality. Each instance has its own config and SQLite state:

| Config file             | department_name | defaults.modality |
|-------------------------|-----------------|-------------------|
| mwl_worker_xray.json    | radiology       | CR                |
| mwl_worker_ct.json      | ct_scan         | CT                |
| mwl_worker_usg.json     | sonology        | US                |

Set `mwl.defaults.modality` to the DICOM modality code for that department.

## Source fields → DICOM mapping

| Source field    | DICOM tag                       |
|-----------------|---------------------------------|
| reqno           | AccessionNumber, RequestedProcedureID |
| patient_id      | PatientID                       |
| patient_name    | PatientName                     |
| patient_sex     | PatientSex (M/F/O)              |
| patient_dob     | PatientBirthDate (YYYYMMDD)     |
| reqdt + reqtm   | ScheduledProcedureStepStart     |
| procedure_name  | RequestedProcedureDescription   |
| modality        | Modality (from defaults)        |

Set `source.scheduled_date_field = "reqdt"` and `source.scheduled_time_field = "reqtm"` so the worker combines them automatically when `scheduled_datetime` is absent.

## Orthanc integration

The worker sends `.wl` files to Orthanc's worklist directory via an HTTP bridge (configure `mwl.create_url`). The Orthanc worklist plugin reads `.wl` files from a local directory and serves them to CR/CT modalities via DICOM C-FIND.

The bridge endpoint needs to write the received file into the Orthanc worklist folder. Any small HTTP handler (nginx + lua, a FastAPI route on the Orthanc host, etc.) that receives a multipart file upload and saves it to the folder works.

Transport modes:

- `multipart_file` — posts the `.wl` file as a multipart upload (recommended for remote Orthanc)
- `json_base64` — embeds the file as base64 in JSON
- `legacy_json` — sends JSON payload only (use when the bridge is on the same host and uses `mwl_file_path`)

## Run

```bash
python radiology_mwl_worker.py --config config/mwl_worker.json
```

Dry-run (generates files, skips HTTP send):

```bash
python radiology_mwl_worker.py --config config/mwl_worker.json --dry-run
```

Single cycle:

```bash
python radiology_mwl_worker.py --config config/mwl_worker.json --once
```

## Behavior

1. Poll source endpoint every `worker.poll_seconds` (default 15 s)
2. Filter rows where `performed == pending_value` (default `"0"`)
3. Build DICOM MWL FileDataset — patient demographics, accession, modality, scheduled step
4. Write `.wl` file to `mwl.outbox_dir`
5. Send to Orthanc worklist bridge via configured transport
6. Track state in SQLite (`worker.state_db_path`) — prevents duplicate sends

## Local state (SQLite)

Each `(accession_number, modality)` pair is tracked with:

- status (`sent` / `failed`)
- attempt count + last error
- Orthanc bridge response JSON
- generated `.wl` file path
- timestamps
