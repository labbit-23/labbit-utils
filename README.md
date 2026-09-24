# labbit-utils

Operational Python workers used by the SDRC/Labit integration layer. This repository contains code and safe example configuration; live secrets, generated runtime state, and PHI outputs remain on the machine and are not part of git.

## Start here

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — current live topology and process map.
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — deployment, validation, and recovery notes.
- [deploy/README.md](deploy/README.md) — selectable lab profiles and recovery tooling.
- [workers/dicom_export/README.md](workers/dicom_export/README.md) — CR/CT export worker.
- [workers/radiology_mwl/README.md](workers/radiology_mwl/README.md) — MWL worker.
- [workers/report_sender/README.md](workers/report_sender/README.md) — report sender workers.

## Structure

Each utility lives in its own folder with its own README, requirements, example configuration, and entry scripts. This allows independent testing and deployment per worker. The live PM2 process map is documented separately because PM2 state is machine-level runtime state, not source-controlled application configuration.

## Workers

- `workers/radiology_mwl` — creates DICOM MWL files from Labit worklist rows and uploads them to the Orthanc MWL bridge.
- `workers/dicom_export` — serves the local DICOM dashboard API and renders/sends CR and CT studies.
- `workers/report_sender` — report dispatch and requisition enqueue workers.

## Configuration rule

Use `config/*.example.json` as the committed template. Machine-specific `config/*.json` files are ignored because they contain credentials and endpoints. Do not add live passwords, API keys, Basic-auth headers, patient data, rendered images, PDFs, SQLite state, or logs to git.

The committed `deploy/templates/*.json.template` files now contain `${VAR}` placeholders. `deploy/render_configs.py` reads `/etc/labbit-utils/labbit.env` (outside git) and writes the ignored live JSON files. The generated JSON is what the workers read.
