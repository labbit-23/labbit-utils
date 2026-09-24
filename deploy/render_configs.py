#!/usr/bin/env python3
"""Render ignored live JSON configs from a manifest and external dotenv file."""

import argparse
import json
import os
import re
import tempfile
from pathlib import Path

TOKEN = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")

def load_env(path):
    values = {}
    for raw in Path(path).read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values

def render(template, values):
    missing = set()
    def replace(match):
        key = match.group(1)
        if key not in values or values[key] == "":
            missing.add(key)
            return match.group(0)
        return values[key]
    text = TOKEN.sub(replace, template)
    if missing:
        raise SystemExit("Missing secret variables: " + ", ".join(sorted(missing)))
    json.loads(text)
    return text

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--secrets", required=True)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    values = load_env(args.secrets)
    jobs = []
    for name, cfg in manifest.get("services", {}).items():
        if not cfg.get("enabled"):
            continue
        if name == "mwl":
            jobs.extend(cfg.get("configs") or [])
        elif cfg.get("template") and cfg.get("config"):
            jobs.append({"template": cfg["template"], "config": cfg["config"]})
    for job in jobs:
        template = Path(job["template"])
        target = Path(job["config"])
        text = render(template.read_text(), values)
        print(("check " if args.check_only else "render ") + str(target))
        if args.check_only:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=target.name + ".", dir=target.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w") as handle:
                handle.write(text)
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

if __name__ == "__main__":
    raise SystemExit(main())
