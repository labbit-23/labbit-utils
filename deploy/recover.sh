#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MANIFEST=""
SECRETS=""
CHECK=false
INSTALL_DEPS=false
RENDER=false
APPLY_PM2=false
PM2_OUTPUT="/tmp/labbit-ecosystem.config.js"

usage() {
  echo "Usage: recover.sh --manifest PATH [--secrets PATH] [--check] [--install-deps] [--render] [--apply-pm2] [--all]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest) MANIFEST="$2"; shift 2 ;;
    --secrets) SECRETS="$2"; shift 2 ;;
    --check) CHECK=true; shift ;;
    --install-deps) INSTALL_DEPS=true; shift ;;
    --render) RENDER=true; shift ;;
    --apply-pm2) APPLY_PM2=true; shift ;;
    --all) CHECK=true; INSTALL_DEPS=true; RENDER=true; APPLY_PM2=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$MANIFEST" ]] || { echo "--manifest is required" >&2; exit 2; }
[[ -f "$MANIFEST" ]] || { echo "Manifest not found: $MANIFEST" >&2; exit 2; }

if [[ "$CHECK" == true || "$INSTALL_DEPS" == true || "$RENDER" == true || "$APPLY_PM2" == true ]]; then
  if [[ -n "$SECRETS" ]]; then
    "$ROOT/deploy/doctor.sh" --manifest "$MANIFEST" --secrets "$SECRETS"
  else
    "$ROOT/deploy/doctor.sh" --manifest "$MANIFEST"
  fi
fi

if [[ "$INSTALL_DEPS" == true ]]; then
  echo "[recover] installing Ubuntu prerequisites"
  sudo apt-get update
  sudo apt-get install -y git python3 python3-venv python3-dev build-essential imagemagick
  if ! command -v pm2 >/dev/null 2>&1; then
    command -v npm >/dev/null 2>&1 || sudo apt-get install -y nodejs npm
    sudo npm install -g pm2
  fi
  for worker in dicom_export radiology_mwl report_sender; do
    worker_dir="$ROOT/workers/$worker"
    requirements="$worker_dir/requirements.txt"
    [[ -f "$requirements" ]] || continue
    python3 -m venv "$worker_dir/.venv"
    "$worker_dir/.venv/bin/pip" install -r "$requirements"
  done
fi

if [[ "$RENDER" == true || "$APPLY_PM2" == true ]]; then
  python3 "$ROOT/deploy/render_pm2.py" --manifest "$MANIFEST" --output "$PM2_OUTPUT"
  echo "[recover] PM2 ecosystem: $PM2_OUTPUT"
fi

if [[ "$APPLY_PM2" == true ]]; then
  [[ -n "$SECRETS" ]] || { echo "--apply-pm2 requires --secrets" >&2; exit 3; }
  [[ -f "$SECRETS" ]] || { echo "Secrets file not found: $SECRETS" >&2; exit 3; }
  pm2 startOrReload "$PM2_OUTPUT"
  pm2 save
  pm2 list
fi
