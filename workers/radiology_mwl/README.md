# radiology_mwl worker

Polls Shivam/labbit-py queue rows where `performed = 0`, generates MWL DICOM files, sends to MWL plugin endpoint, and stores local send-state in SQLite.

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

- `source.poll_url` (labbit-py Shivam API)
- `destination.aet`, `destination.host`, `destination.port`
- `mwl.create_url` (MWL plugin endpoint)

## Run

```bash
python radiology_mwl_worker.py --config config/mwl_worker.json
```

Dry-run:

```bash
python radiology_mwl_worker.py --config config/mwl_worker.json --dry-run
```

## Behavior

1. Poll source endpoint
2. Filter rows where `performed == pending_value` (default `0`)
3. Build MWL payload and generate DICOM file (`.dcm`) in `mwl.outbox_dir`
4. Send to plugin using configured transport
5. Write local state in SQLite (`worker.state_db_path`)

## Transport modes

`mwl.transport` supports:

- `legacy_json` (default): sends normal JSON payload (can include local file path and optional base64)
- `json_base64`: sends payload + `mwl_dicom_base64`
- `multipart_file`: uploads DICOM file as multipart (`file` field)

## Local tracking (no upstream performed update)

The worker does **not** mark `performed` upstream.

Local SQLite table tracks:

- accession number
- modality
- source row id
- status (`sent` / `failed`)
- attempts
- last error
- response JSON
- generated MWL file path
- timestamps
