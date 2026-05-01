# py_utils

Shared home for operational Python workers that are not tightly coupled to one app repo.

## Structure

Each utility must live in its own folder with:

- its own `README.md`
- its own `requirements.txt`
- its own `config/*.example.json`
- its own entry script(s)

Example:

```text
py_utils/
  workers/
    radiology_mwl/
      README.md
      requirements.txt
      radiology_mwl_worker.py
      config/
        mwl_worker.example.json
```

This allows independent deploy/runtime per worker.

## Workers (current)

- `workers/radiology_mwl`  
  Polls Supabase for `performed = 0` and creates Radiology MWL records via configured endpoint.

## Planned here

- Mirth-facing workers
- report sender workers
- additional automation pollers

## VPS deploy helper

For report sender worker updates on VPS:

```bash
cd /opt/py_utils
./scripts/deploy-vps-report-sender.sh
```

With PM2 restart:

```bash
cd /opt/py_utils
./scripts/deploy-vps-report-sender.sh --restart-pm2
```
