#!/bin/bash
# Streams a tar of Mirth's appdata (Derby DB, keystore, config) plus every
# real (gitignored) secrets/config file across labbit-py, labbit-utils, and
# the mirth integrations repo to stdout. Invoked over SSH from DICOMNEW's
# nightly backup pull (see the forced command in sdrc-report's
# authorized_keys) -- never run this and leave the output lying around
# unencrypted; the caller pipes it straight into gpg.
#
# Best-effort snapshot of Mirth's live embedded Derby DB (not a proper
# SYSCS_UTIL.SYSCS_BACKUP_DATABASE online backup) -- acceptable for a nightly
# low-traffic-hours safety net, not a guarantee of perfect point-in-time
# consistency if a write lands mid-tar.
set -euo pipefail

cd /opt

tar --ignore-failed-read -czf - \
  mirthconnect/appdata/mirthdb \
  mirthconnect/appdata/keystore.jks \
  mirthconnect/appdata/configuration.properties \
  mirthconnect/appdata/extension.properties \
  mirthconnect/appdata/server.id \
  labbit-utils/workers/dicom_export/config/dicom_export.json \
  labbit-utils/workers/radiology_mwl/config/mwl_worker_cardiology.json \
  labbit-utils/workers/radiology_mwl/config/mwl_worker_ct.json \
  labbit-utils/workers/radiology_mwl/config/mwl_worker_usg.json \
  labbit-utils/workers/radiology_mwl/config/mwl_worker_xray.json \
  labbit-py/services.local.ini \
  labbit-py/services.vps.ini \
  integrations/mirth/labit/.mirth.env \
  integrations/mirth/LABIT_DISPATCH_CREDENTIALS.env \
  /etc/labbit-utils/labbit.env \
  2>/dev/null
