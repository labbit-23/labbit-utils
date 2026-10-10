# Parallel Tricog ECG worker

This package is the Python replacement path for the active Mirth Tricog ECG
poller. Mirth remains live until this worker has been compared and explicitly
cut over.

## Branch discovery

At the start of each poll cycle the worker calls Tricog GET /api/users/clinics, which is the source for the portal branch dropdown. It normalizes the returned center/doctor pairs and polls every currently available scope in one process. Branch names are display metadata; the stable scope key is centerId:doctorId.

Watermarks and seen ECG IDs are stored under that scope key. Existing center-only state is reused when first encountered, so this change does not replay the current branches. A newly exposed Tricog scope starts with the bounded initial lookback and must be reviewed before live cutover.

## Modes

- `worker.py` defaults to shadow mode: login, switch through all SDRC branches,
  discover records in a bounded 24-hour initial window, and write a separate
  per-branch state/watermark file. It does not download, attach, upload, or
  send anything.
- `--live` enables the existing Python delivery chain: graph-PDF rebuild,
  Core attachment, FTP publication, ledger update, and WhatsApp delivery.
- `manual_send.py` is the local operator-selected manual-send entrypoint used by the
  DEXA ECG management page. It receives one ledger row over stdin and never
  opens a network listener.

## Technical-quality gate

Before patient WhatsApp delivery, the Python path derives a quality flag from
Tricog's structured quality field or from the diagnosis/final-classification
text. It blocks technical-acquisition warnings such as `Lead Reversal
Suspected`, `Lead Placement Suspected`, and `Poor Quality ECG`. Core/FTP
processing and ledger recording still occur; the patient WhatsApp stage is
recorded as `blocked`, and the configured default/staff number receives a
quality alert. A blocked ECG is considered handled so it does not retry forever.

## Shadow run

Use the DICOM worker's existing delivery JSON while the configuration is being
consolidated:

```sh
cd /opt/labbit-utils/workers/ecg_delivery
export TRICOG_USERNAME='...'
export TRICOG_PASSWORD='...'
/opt/labbit-utils/workers/dicom_export/.venv/bin/python worker.py \
  --delivery-config ../dicom_export/config/dicom_export.json \
  --state-file /var/tmp/orthanc-images/ECG/.python_tricog_state.json
```

Do not use `--live` while Mirth's ECG channel is `STARTED`. The cutover must
stop the Mirth ECG poller first, preserve its seen-ID boundary, and then start
Python with a reviewed state file so a study is not sent twice. An empty state
is intentionally limited to the last 24 hours; it must still be reviewed
before enabling live delivery.

The DEXA API invokes `manual_send.py` locally over stdin. The API requires the ECG
admin Basic-auth credentials and WhatsApp is a separate explicit checkbox.
