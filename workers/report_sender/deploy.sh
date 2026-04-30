#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
CONFIG_EXAMPLE="${ROOT_DIR}/config/report_sender.example.json"
CONFIG_FILE="${ROOT_DIR}/config/report_sender.json"
PYTHON_BIN="${PYTHON_BIN:-python3}"

usage() {
  cat <<USAGE
Usage: ./deploy.sh <command>

Commands:
  setup                Create venv, install dependencies, create config if missing
  run-enqueue-once     Run one enqueue cycle
  run-enqueue-watch    Run enqueue watcher (hourly pre-10am, every 5m after)
  run-sender-once      Run one sender cycle
  run-sender           Run sender worker loop
  dryrun-enqueue       Enqueue one dry-run cycle
  dryrun-sender        Sender one dry-run cycle

Env overrides:
  PYTHON_BIN           Python interpreter (default: python3)
USAGE
}

ensure_venv() {
  if [[ ! -d "${VENV_DIR}" ]]; then
    "${PYTHON_BIN}" -m venv "${VENV_DIR}"
  fi
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
}

setup() {
  ensure_venv
  pip install --upgrade pip
  pip install -r "${ROOT_DIR}/requirements.txt"
  if [[ ! -f "${CONFIG_FILE}" ]]; then
    cp "${CONFIG_EXAMPLE}" "${CONFIG_FILE}"
    echo "Created ${CONFIG_FILE}. Update it with real values before running workers."
  fi
  echo "Setup complete."
}

run_enqueue_once() {
  ensure_venv
  python "${ROOT_DIR}/enqueue_requisitions_worker.py" --config "${CONFIG_FILE}"
}

run_enqueue_watch() {
  ensure_venv
  python "${ROOT_DIR}/enqueue_requisitions_worker.py" --config "${CONFIG_FILE}" --watch
}

run_sender_once() {
  ensure_venv
  python "${ROOT_DIR}/report_sender_worker.py" --config "${CONFIG_FILE}" --once
}

run_sender() {
  ensure_venv
  python "${ROOT_DIR}/report_sender_worker.py" --config "${CONFIG_FILE}"
}

dryrun_enqueue() {
  ensure_venv
  python "${ROOT_DIR}/enqueue_requisitions_worker.py" --config "${CONFIG_FILE}" --dry-run
}

dryrun_sender() {
  ensure_venv
  python "${ROOT_DIR}/report_sender_worker.py" --config "${CONFIG_FILE}" --dry-run --once
}

cmd="${1:-}"
case "${cmd}" in
  setup) setup ;;
  run-enqueue-once) run_enqueue_once ;;
  run-enqueue-watch) run_enqueue_watch ;;
  run-sender-once) run_sender_once ;;
  run-sender) run_sender ;;
  dryrun-enqueue) dryrun_enqueue ;;
  dryrun-sender) dryrun_sender ;;
  *) usage; exit 1 ;;
esac
