# labbit-utils architecture

This document describes the live integration PC as checked on 2026-09-24. It is an operational map, not a claim that every process shown in the repository is deployed on every machine.

## Repository layout

```
labbit-utils/
├── workers/
│   ├── dicom_export/       CR/X-ray and CT retrieval, rendering, PDF delivery, print code
│   ├── radiology_mwl/      DICOM Modality Worklist generation and Orthanc bridge upload
│   └── report_sender/      report enqueue and WhatsApp dispatch workers
├── scripts/                targeted deployment and backup helpers
├── docs/                   system-level operational documentation
└── README.md
```

Each worker is a separate Python folder. `dicom_export` is one live service but contains `main.py` for the HTTP API, `cr.py` and `ct.py` for modality behavior, `core.py` for shared logic, and `print_scu.py` for optional DICOM printing.

## Live PM2 process map

Verified on this PC:

| PM2 name | Role | Code/config | Status |
|---|---|---|---|
| `dicom-export-cr` | DICOM dashboard API, CR/X-ray and CT rendering/delivery | `workers/dicom_export/main.py` | online |
| `mwl-all` | One supervisor process running the X-ray, USG, and cardiology MWL loops | `mwl_supervisor.py` + the three modality JSON files | online |

Other PM2 applications on the PC are outside this repository, including `sdrc-dexa-app`, `sysmex-bridge`, `sdrc-collector-api`, `labbit-monitoring-local`, `erpnext`, and `zk-panel`.

## Data flow

```
Labit/Core worklist API
        │
        ├── MWL workers ──> Orthanc MWL bridge/worklist directory ──> modality C-FIND
        │
        └── DICOM export worker queries Orthanc
                ├── image selection / CT rendering / PDF composition
                ├── FTP or file publication
                └── WhatsApp/report delivery relay
```

The DICOM export worker uses configured primary and backup Orthanc sources. It is one HTTP worker for CR and CT; CT is not a separate PM2 process on this PC.

## Configuration boundaries today

1. **Committed examples:** safe templates such as `dicom_export.example.json` and `mwl_worker.example.json`.
2. **Machine-local live JSON:** ignored `config/*.json` files containing real endpoints, credentials, routing, polling, and local paths.
3. **Runtime state:** PM2 definitions/environment, SQLite state, logs, temporary images, outbox files, nginx configuration, and Mirth configuration.

The main operational risk is that upstream credentials and routing facts are repeated across worker JSON files. A future deployment layer should generate them from one machine manifest and a root-owned secret file, then restart only the affected service.

## Current modality mapping

| Function | Source | Destination | Process model |
|---|---|---|---|
| X-ray MWL | Labit machine MWL API, radiography | Orthanc MWL bridge; shared CR machines | one MWL loop |
| USG MWL | Labit machine MWL API, sonology | Orthanc MWL bridge; station overrides | one MWL loop |
| Cardiology MWL | Labit machine MWL API, cardiology | Orthanc MWL bridge, fixed cardiology AET | one MWL loop |
| CR/X-ray export | Orthanc studies | PDF/file relay and WhatsApp path | shared `dicom-export-cr` |
| CT export | Orthanc studies | PDF/file relay and WhatsApp path | shared `dicom-export-cr` |

The MWL loops now run under one supervisor process; each keeps its own JSON config and SQLite state. The CT MWL configuration is disabled/not part of the current live map.

## Security and data handling

- Live JSON configs contain secrets and remain local.
- `tmp/`, `outbox/`, logs, and SQLite state can contain patient identifiers or images and must not be committed.
- Nginx and Mirth are separate machine-level systems; changing this repository does not change them.
- A deployment must validate target host, config path, process name, and endpoint before restart.

## Intended next architecture

One machine manifest should describe Orthanc primary/backup, Core/Labit endpoints, relay credentials, enabled modalities, ports, polling schedules, and process definitions. A deploy command should render local worker configs, validate them, and apply only requested services. The current live JSON files remain the source of truth until that migration is explicit.
