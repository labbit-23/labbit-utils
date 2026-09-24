# labbit-utils deployment guide

This is the current safe operating procedure for the integration PC. It documents what exists now; it does not yet provide a one-command deployment system.

## Before changing anything

```bash
cd /opt/labbit-utils
git status --short --branch
pm2 list
```

Confirm the target process and config path before editing. Do not restart Mirth from this repo. Do not use the disabled CT MWL config as an active configuration.

## Current PM2 model

The DICOM export service is one PM2 process for CR and CT. MWL is being moved from three PM2 entries to one supervisor process:

```bash
cd /opt/labbit-utils/workers/radiology_mwl
./.venv/bin/python mwl_supervisor.py \
  --config config/mwl_worker_xray.json \
  --config config/mwl_worker_usg.json \
  --config config/mwl_worker_cardiology.json
```

Each config remains independent and keeps its own state database, outbox, source filter, destination AET, and routing overrides. One process does not merge the worklists; it only reduces process-management overhead.

Before the live cutover, inspect the current entries:

```bash
pm2 describe mwl-xray
pm2 describe mwl-usg
pm2 describe mwl-cardiology
```

The cutover should be controlled: stop the three old entries, start the one supervisor, confirm all three loop-start messages and successful bridge activity, then save the PM2 state. Keep the old PM2 definitions available for rollback until the supervisor has been observed through a normal polling interval.

## Validation checklist

1. `git diff --check` passes.
2. The changed worker compiles and its tests pass.
3. Every live JSON parses and points to the intended endpoint.
4. The PM2 process is online after restart.
5. Logs show one supervisor plus three named loops, with no auth/configuration error.
6. For MWL, use a controlled designated test order and verify the bridge response.
7. For DICOM export, verify accession boundary, series/instance order, PDF size, and delivery status.
8. Never perform an uncontrolled physical DICOM print test. The DRYPIX profile is disabled until the network path and approved test procedure are confirmed.

## What belongs in git

Commit source, tests, documentation, safe example templates, and non-secret deployment scripts. Do not commit live `config/*.json`, credentials, patient data, PDFs, rendered images, logs, SQLite databases, PM2 dumps, or host-specific nginx/Mirth state.

## Known deployment limitation

Configuration is not yet centralized. A new machine currently requires manually placing each worker's local JSON, creating virtual environments, and creating PM2 entries with the correct working directory and config argument. The next deployment task should add a non-secret machine manifest, a root-owned secret/env file outside git, config rendering and validation, a PM2 ecosystem definition, a dry-run doctor command, and targeted restart/rollback behavior.

Until that exists, document each live config change in the commit message or handoff and preserve the previous local config before restarting.
