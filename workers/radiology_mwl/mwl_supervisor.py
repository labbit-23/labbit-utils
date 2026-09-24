#!/usr/bin/env python3
"""Run multiple independent MWL configurations under one PM2 process."""

import argparse
import logging
import signal
import threading
from typing import List

from radiology_mwl_worker import MWLWorker, load_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Radiology MWL multi-config supervisor")
    parser.add_argument("--config", action="append", required=True, help="Worker JSON path; repeat once per modality")
    parser.add_argument("--dry-run", action="store_true", help="Do not call bridge endpoints")
    return parser.parse_args()


def run_worker(worker: MWLWorker, name: str, stop_event: threading.Event) -> None:
    log = logging.getLogger(f"mwl-supervisor.{name}")
    poll_seconds = int(worker.cfg.get("worker", {}).get("poll_seconds", 15))
    log.info("Starting loop poll_seconds=%s dry_run=%s", poll_seconds, worker.dry_run)
    while not stop_event.is_set():
        try:
            worker.process_once()
        except Exception:
            log.exception("Loop error")
        stop_event.wait(poll_seconds)


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [mwl-supervisor] %(message)s")
    stop_event = threading.Event()
    workers: List[MWLWorker] = []
    threads: List[threading.Thread] = []

    def stop(_signum, _frame) -> None:
        logging.getLogger("mwl-supervisor").info("Shutdown requested")
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    for config_path in args.config:
        cfg = load_json(config_path)
        worker = MWLWorker(cfg, dry_run=args.dry_run)
        workers.append(worker)
        thread = threading.Thread(
            target=run_worker,
            args=(worker, config_path.rsplit("/", 1)[-1], stop_event),
            name=config_path.rsplit("/", 1)[-1],
            daemon=True,
        )
        threads.append(thread)
        thread.start()

    logging.getLogger("mwl-supervisor").info("Started %d MWL loops", len(threads))
    try:
        while not stop_event.wait(1):
            dead = [thread.name for thread in threads if not thread.is_alive()]
            if dead:
                logging.getLogger("mwl-supervisor").error("MWL loop stopped unexpectedly: %s", ", ".join(dead))
                stop_event.set()
    finally:
        for worker in workers:
            worker.session.close()
        for thread in threads:
            thread.join(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
