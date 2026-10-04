# Parallel Tricog ECG worker

This package is the Python replacement path for the active Mirth Tricog ECG
poller. Mirth remains live until this worker has been compared and explicitly
cut over.

## Modes

- `worker.py` defaults to shadow mode: login, switch through all SDRC branches,
  discover records, and write a separate state file. It does not download,
  attach, upload, or send anything.
- `--live` enables the existing Python delivery chain: graph-PDF rebuild,
  Core attachment, FTP publication, ledger update, and WhatsApp delivery.
- `repair.py` is the local operator-selected repair entrypoint used by the
  DEXA ECG management page. It receives one ledger row over stdin and never
  opens a network listener.

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
Python with a reviewed state file so a study is not sent twice.

The DEXA API invokes `repair.py` locally over stdin. The API requires the ECG
admin Basic-auth credentials and WhatsApp is a separate explicit checkbox.
