#!/usr/bin/env bash
set -euo pipefail

# Deploy helper for VPS: pulls latest py_utils and prepares report_sender worker.
# Optional PM2 restart for long-running watcher/worker processes.

REPO_DIR="${REPO_DIR:-/opt/py_utils}"
WORKER_DIR="${WORKER_DIR:-${REPO_DIR}/workers/report_sender}"
RESTART_PM2=false
PM2_ENQUEUE_NAME="${PM2_ENQUEUE_NAME:-report-enqueue-watch}"
PM2_SENDER_NAME="${PM2_SENDER_NAME:-report-sender}"
ALLOW_STASH=false

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--restart-pm2] [--allow-stash]

Options:
  --restart-pm2   Restart PM2 apps after deploy (if they exist)
  --allow-stash   Auto-stash local changes before pull and pop after

Env overrides:
  REPO_DIR=/opt/py_utils
  WORKER_DIR=/opt/py_utils/workers/report_sender
  PM2_ENQUEUE_NAME=report-enqueue-watch
  PM2_SENDER_NAME=report-sender
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --restart-pm2) RESTART_PM2=true; shift ;;
    --allow-stash) ALLOW_STASH=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

if [[ ! -d "${REPO_DIR}/.git" ]]; then
  echo "Repo not found at ${REPO_DIR}" >&2
  exit 1
fi

cd "${REPO_DIR}"

echo "[deploy] repo: ${REPO_DIR}"

STASHED=false
if ! git diff --quiet || ! git diff --cached --quiet; then
  if [[ "${ALLOW_STASH}" == "true" ]]; then
    echo "[deploy] local changes detected, stashing"
    git stash push -m "deploy-vps-report-sender-auto-stash"
    STASHED=true
  else
    echo "[deploy] local changes detected. Commit/stash first or pass --allow-stash." >&2
    exit 1
  fi
fi

echo "[deploy] pulling latest"
git pull --ff-only

echo "[deploy] setup worker env"
cd "${WORKER_DIR}"
./deploy.sh setup

if [[ "${RESTART_PM2}" == "true" ]]; then
  if command -v pm2 >/dev/null 2>&1; then
    echo "[deploy] restarting PM2 apps (if present)"
    pm2 restart "${PM2_ENQUEUE_NAME}" || true
    pm2 restart "${PM2_SENDER_NAME}" || true
    pm2 save || true
  else
    echo "[deploy] pm2 not found, skipping restart"
  fi
fi

if [[ "${STASHED}" == "true" ]]; then
  echo "[deploy] restoring stashed local changes"
  cd "${REPO_DIR}"
  git stash pop || true
fi

echo "[deploy] done"
