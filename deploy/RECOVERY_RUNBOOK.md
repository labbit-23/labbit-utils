# New-machine recovery runbook

## 1. Install the repository

    sudo mkdir -p /opt
    sudo git clone https://github.com/labbit-23/labbit-utils.git /opt/labbit-utils
    cd /opt/labbit-utils

## 2. Put secrets outside git

    sudo install -d -m 750 /etc/labbit-utils
    sudo install -m 600 /secure-backup/labbit.env /etc/labbit-utils/labbit.env
    scp /secure-backup/labbit.env new-host:/tmp/labbit.env

The exact file to preserve separately is `/etc/labbit-utils/labbit.env`. It is not committed. The existing encrypted backup stream now includes this path. Keep a copy in the encrypted recovery archive and transfer it separately when rebuilding.

Use the variables listed in deploy/secrets.env.example. Never put passwords in git.

## 3. Restore selected live configs

Restore only the configs for enabled services:

    workers/dicom_export/config/dicom_export.json
    workers/radiology_mwl/config/mwl_worker_xray.json
    workers/radiology_mwl/config/mwl_worker_usg.json
    workers/radiology_mwl/config/mwl_worker_cardiology.json
    workers/report_sender/config/report_sender.json

Restore workers/radiology_mwl/state when available. Without MWL state, previously sent rows may be reconsidered.

## 4. Validate

    cp deploy/manifests/radiology.example.json /tmp/my-lab.json
    sed -i 's/replace-me/my-lab/' /tmp/my-lab.json
    ./deploy/recover.sh --manifest /tmp/my-lab.json --secrets /etc/labbit-utils/labbit.env --check

## 5. Install and render

    ./deploy/recover.sh --manifest /tmp/my-lab.json --secrets /etc/labbit-utils/labbit.env --install-deps --render

Review /tmp/labbit-ecosystem.config.js before applying it.

## 6. Start selected services

    ./deploy/recover.sh --manifest /tmp/my-lab.json --secrets /etc/labbit-utils/labbit.env --apply-pm2
    pm2 list
    pm2 logs mwl-all --lines 80
    pm2 logs dicom-export-cr --lines 80
    pm2 save

## Still separate from labbit-utils

A complete machine rebuild also requires nginx and Basic Auth, the frontend repository, Mirth installation/channels/appdata/keystore/held keys, Orthanc connectivity, network/DICOM firewall rules, and encrypted runtime-state backups. The recovery script deliberately does not overwrite those systems.
