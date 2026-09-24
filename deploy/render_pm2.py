#!/usr/bin/env python3
"""Render a PM2 ecosystem file from a selectable lab manifest."""
import argparse
import json
from pathlib import Path

def app(name, cwd, script, args):
    return {
        "name": name,
        "cwd": str(cwd),
        "script": str(script),
        "interpreter": str(cwd / ".venv" / "bin" / "python"),
        "args": args,
        "autorestart": True,
        "watch": False,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    repo = Path(manifest.get("repo_dir") or "/opt/labbit-utils")
    apps = []
    d = manifest.get("services", {}).get("dicom_export", {})
    if d.get("enabled"):
        cwd = repo / "workers" / "dicom_export"
        apps.append(app(d.get("pm2_name", "dicom-export"), cwd, repo / d["entry"], ["--config", str(repo / d["config"])]))
    m = manifest.get("services", {}).get("mwl", {})
    if m.get("enabled"):
        cwd = repo / "workers" / "radiology_mwl"
        flattened = []
        for item in m.get("configs") or []:
            config = item.get("config") if isinstance(item, dict) else item
            flattened.extend(["--config", str(repo / config)])
        apps.append(app(m.get("pm2_name", "mwl-all"), cwd, repo / m["entry"], flattened))
    r = manifest.get("services", {}).get("report_sender", {})
    if r.get("enabled"):
        cwd = repo / "workers" / "report_sender"
        config = str(repo / r["config"])
        apps.append(app("report-sender", cwd, cwd / "report_sender_worker.py", ["--config", config]))
        apps.append(app("report-enqueue-watch", cwd, cwd / "enqueue_requisitions_worker.py", ["--config", config, "--watch"]))
    Path(args.output).write_text("module.exports = " + json.dumps({"apps": apps}, indent=2) + ";\n")
    print("Rendered", len(apps), "PM2 apps to", args.output)

if __name__ == "__main__":
    raise SystemExit(main())
