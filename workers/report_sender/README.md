# report_sender worker

Auto-dispatch worker for patient report WhatsApp sends.

## Components

1. `report_sender_worker.py`
- Polls `report_auto_dispatch_jobs`
- Active window: `07:30` to `21:30` local time
- Applies same-day readiness gating using `SAMEDAYREPORT`
- Applies cooloff (`latest_approved_at + cooloff_minutes`, default 30m)
- Supports evening partial-send window with randomized per-job cutoff (`partial_send_cutoff_from_hhmm` to `partial_send_cutoff_to_hhmm`)
- Sends via existing report template endpoint `/api/internal/whatsapp/report-template-send`
- Marks sent test IDs back through labit-py's `/delivery/status/update` so Core can store per-report delivery events after cutover
- Supports `is_paused` and `force_send_now`
- Includes stuck-job watchdog + state reconciliation:
- auto-requeue stale `queued_wait` / `cooling_off`
- terminal `failed_timeout` after retry cap
- claim-based processing to reduce multi-worker race risk
- duplicate-send guard: skips resend when same-day ready count has not increased

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

Watchdog overrides (optional env vars):
```bash
REPORT_SENDER_STUCK_QUEUED_WAIT_HOURS=6
REPORT_SENDER_STUCK_COOLING_OFF_HOURS=2
REPORT_SENDER_STUCK_MAX_AUTO_REQUEUES=3
REPORT_SENDER_STUCK_SCAN_LIMIT=500
```

Admin recovery SQL:
`workers/report_sender/sql/recover_stuck_jobs.sql`

## Required Supabase Tables

- `report_auto_dispatch_jobs`
- `report_auto_dispatch_events`

## Incremental follow-up sends (`enqueue.incremental_followups`)

`off` (default) keeps the old rule: a follow-up job is created only when the requisition reaches FULL_REPORT.
`preview` logs `[followup-preview] would enqueue ...` and creates nothing. `on` enables the rule below.

After the first send a requisition stays open (up to `followup_max_age_days`, default 15, counted from its first
job) until every non-cancelled test is ready. A follow-up job is created whenever tests are ready that the LAST
SENT report did not contain; the send is cumulative (everything ready so far). The sender's same-day gate then
coalesces same-day tests into one send (held until all same-day tests are ready or the evening partial cutoff),
while non-same-day tests such as cultures create their own send when approved. Cancelled tests count as neither
ready nor pending; special/outsourced tests keep their own split-job path. Jobs whose previous snapshot has no
per-test data fall back to the old FULL-only rule. `followup_check_interval_minutes` (default 15) limits how
often an open requisition's live status is fetched.
