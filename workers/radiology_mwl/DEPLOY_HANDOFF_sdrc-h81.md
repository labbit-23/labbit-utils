# radiology_mwl — deploy handoff for sdrc-h81

Written 2026-09-11 by a Claude Code session on a different machine
(`sdrc-report-delivery`, a sandbox) that could not get SSH access to this
box to do the deployment itself (tried the right account/key, ruled out
permissions and fail2ban — connections from that sandbox simply never
reached this box's real `sshd`/`auth.log`; a reverse proxy was mentioned
as possibly involved but never confirmed. Not your problem to chase unless
it resurfaces — this handoff exists specifically so you can do the actual
deployment locally instead). Read `PROGRESS_2026-09-10.md` and
`PROGRESS_2026-09-11.md` in this same folder first for full background —
this file only covers what to *do*, assuming that context.

## Why this machine

`sdrc-h81` already runs Mirth and already sends X-Rays/ECGs out to Orthanc
(see `mirth/ORTHANC_Export.xml` / `mirth/ORTHANC_Export_Automated.xml` /
`mirth/Tricog ECG Sender.xml` in the `integrations` repo) — so outbound
connectivity from this box to Orthanc at `100.84.172.71:8042` is *already
proven working* via those live channels. This worker just needs to reuse
that same path.

## What's already done (don't redo)

- Orthanc (on the separate Windows box, `100.84.172.71`, Tailscale) was in
  a crash loop; fixed (corrupt worklist file removed). Not related to this
  deployment, mentioned only so you don't get confused if you see it in
  history.
- `SONOSCAPE` (10.16.18.70) and `ESAOTE` (10.16.18.80) registered as known
  Orthanc modalities.
- `radiology_mwl_worker.py` already has `mwl.station_overrides` support
  (commit `38a2fd5`) — the code itself needs no changes, just correct
  config.
- `labit-core`'s `arrived_at` gate is live in production — confirmed via a
  real API call today, a real X-Ray (accession `R202609110083`) came
  through it correctly.

## What you need to actually do

1. `git pull` this repo (`labbit-utils`) on this box, or clone it if it's
   not already here.
2. Confirm Python 3 + pip. Install the worker's deps:
   ```bash
   cd workers/radiology_mwl
   pip install -r requirements.txt
   ```
3. Get `LABIT_API_KEY_ID` / `LABIT_API_SECRET` from the site owner
   directly (same pair every Mirth channel already uses, from Mirth's
   configurationMap) — **do not hardcode these into any file you commit.**
   Put them in the config files below (which should NOT be committed —
   check this folder's `.gitignore`, add `config/mwl_worker_*.json` to it
   if it isn't already ignored).
4. Write `config/mwl_worker_xray.json`:
   ```json
   {
     "source": {
       "poll_url": "https://labit.sdrc.in/machine-api/mwl?department_name=radiology",
       "poll_method": "GET",
       "poll_headers": {
         "X-Api-Key-Id": "<get from site owner>",
         "X-Api-Secret": "<get from site owner>"
       },
       "poll_timeout_seconds": 20,
       "items_path": "items",
       "performed_field": "performed",
       "pending_value": false,
       "id_field": "reqid",
       "scheduled_date_field": "reqdt",
       "scheduled_time_field": "reqtm"
     },
     "destination": { "aet": "", "host": "100.84.172.71", "port": 4242 },
     "mwl": {
       "create_url": "http://100.84.172.71:8042/mwl-bridge/upload",
       "create_method": "POST",
       "create_headers": { "Authorization": "Basic cmFkaW86cmFkaW8=" },
       "timeout_seconds": 20,
       "transport": "json_base64",
       "outbox_dir": "./outbox",
       "payload_fields": {
         "accession_number": "accession_no",
         "patient_id": "mrn",
         "patient_name": "patient_name",
         "patient_sex": "patient_sex",
         "patient_dob": "patient_dob",
         "modality": "modality",
         "requested_procedure_description": "procedure_name"
       },
       "defaults": {
         "modality": "CR",
         "institution_name": "SDRC",
         "requested_procedure_description": "RADIOLOGY",
         "scheduled_step_description": "RADIOLOGY"
       }
     },
     "worker": {
       "poll_seconds": 15,
       "batch_size": 50,
       "log_level": "INFO",
       "state_db_path": "./state/radiology_xray_state.sqlite3"
     }
   }
   ```
5. Write `config/mwl_worker_usg.json` — identical shape, except:
   ```json
   "source": { "poll_url": "https://labit.sdrc.in/machine-api/mwl?department_name=sonology", ... },
   "mwl": {
     ...,
     "defaults": { "modality": "US", ... },
     "station_overrides": [ { "keyword": "CARDIOLOGY", "aet": "ESAOTE" } ]
   },
   "worker": { ..., "state_db_path": "./state/radiology_usg_state.sqlite3" }
   ```
   (`destination.aet` stays `""` here too — general dopplers/sonology are
   open to either `SONOSCAPE` or `ESAOTE`; only cardiology dopplers get
   pinned via the override.)
6. Test one cycle before making it persistent:
   ```bash
   python radiology_mwl_worker.py --config config/mwl_worker_xray.json --once
   ```
   Check the log output and `mwl_bridge.log` on the Orthanc box
   (`E:\OrthancScripts\mwl_bridge.log`, if you can reach it) for a
   successful write.
7. Set both up as persistent systemd services (not just `--once` runs) —
   e.g. `/etc/systemd/system/radiology-mwl-xray.service` and
   `radiology-mwl-usg.service`, each running
   `python3 radiology_mwl_worker.py --config config/mwl_worker_<x>.json`
   with `Restart=on-failure`, `WantedBy=multi-user.target`. Enable and
   start both.

## Traps already hit today — don't rediscover these

- **`patient_id` in the source API response is an internal UUID, not the
  MRN.** The DICOM-facing patient ID must come from the `mrn` field
  instead (`payload_fields.patient_id: "mrn"` above already does this
  correctly — don't "fix" it to `"patient_id"`, that would be wrong).
- **`performed` is a real JSON boolean** in the live API (`false`/`true`),
  not the example config's string `"0"` sentinel. `pending_value` must be
  JSON `false` to match, or every row will look "not pending" and nothing
  will ever get pushed.
- **The bridge (`/mwl-bridge/upload`) requires HTTP Basic auth** even
  though the Python callback code itself has no auth check — Orthanc
  enforces it at the HTTP layer regardless. Confirmed by direct test:
  no/wrong auth → `401`; `radio:radio` → `200`. Without
  `create_headers.Authorization` set, every upload will silently fail
  with 401.
- **`mwl.transport` must be `"json_base64"`, not `"multipart_file"`** —
  Orthanc's Python plugin invokes a REST callback once per multipart
  field, breaking the real HTTP response regardless of which invocation
  answers. `mwl_bridge.py` is written specifically for `json_base64`.
- **Don't leave malformed test payloads sitting in `E:\OrthancWorklists`
  on the Orthanc box** — a corrupt 10-byte file there crashed the whole
  Orthanc service in a loop for most of a day (Housekeeper thread rescans
  that folder every 60s and aborts on anything that doesn't parse as
  DICOM). If you send a test payload to the bridge to verify it works,
  make sure it's a real, complete `.wl` file, not a placeholder.

## Verifying it's actually working end to end

1. Confirm `machine-api/mwl?department_name=radiology` (or `sonology`)
   returns real pending items. This requires `arrived_at` to be set on a
   real requisition_item first — confirmed today that this field *does*
   get populated for real patients (one real X-Ray, accession
   `R202609110083`, showed up with `arrived_at` set). What was **not**
   confirmed: whether that came from an "Accept Patient" button in
   `labit-ui` specifically, or some other path — nobody checked `labit-ui`
   itself this session. If `machine-api/mwl` stays empty when you expect
   real traffic, that UI wiring (or lack of it) is the first thing to
   check, not this worker.
2. Confirm the worker's log shows it picking up that row and posting to
   the bridge successfully (not just log noise — check for the actual
   `{"ok": true, ...}` response body).
3. Confirm a new `.wl` file with the right accession number appears in
   `E:\OrthancWorklists` on the Orthanc box (ask whoever has access there,
   or check via the OrthancExplorer2 Worklists UI if you have it).
4. Real proof only comes from an actual modality console doing "Query
   Worklist" and finding it — nothing configured on any real console yet
   as of this handoff, that's a separate physical task per machine.
