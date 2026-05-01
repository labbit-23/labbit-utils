# report_sender worker

Auto-dispatch worker for patient report WhatsApp sends.

## Components

1. `report_sender_worker.py`
- Polls `report_auto_dispatch_jobs`
- Active window: `07:30` to `21:30` local time
- Applies same-day readiness gating using `SAMEDAYREPORT`
- Applies cooloff (`latest_approved_at + cooloff_minutes`, default 30m)
- Supports evening partial-send window with randomized per-job cutoff (`partial_send_cutoff_from_hhmm` to `partial_send_cutoff_to_hhmm`)
- Sends via existing `report_pdf` template through `/api/internal/whatsapp/send`
- Supports `is_paused` and `force_send_now`

2. `enqueue_requisitions_worker.py`
- Fetches today's requisitions from existing Shivam endpoint
- No backend cursor API needed
- Performs daily full fetch and skips:
- reqnos already in `report_auto_dispatch_jobs`
- reqnos already in `report_dispatch_logs` with `status=success`
- Active window: `07:30` to `21:30` local time
- Go-live date guard via `enqueue.start_date` (default `2026-05-01`)
- Enqueues in paused mode by default for safe testing
- `--watch` mode cadence:
- hourly until `10:00`
- every `5 minutes` after `10:00`

## Run

Preferred:
```bash
cd py_utils/workers/report_sender
./deploy.sh setup
```

Manual:
```bash
cd py_utils/workers/report_sender
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config/report_sender.example.json config/report_sender.json
```

Enqueue:

```bash
./deploy.sh dryrun-enqueue
./deploy.sh run-enqueue-once
./deploy.sh run-enqueue-watch
```

Sender:

```bash
./deploy.sh dryrun-sender
./deploy.sh run-sender-once
./deploy.sh run-sender
```

## Required Supabase Tables

- `report_auto_dispatch_jobs`
- `report_auto_dispatch_events`
