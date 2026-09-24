#!/usr/bin/env python3
import argparse
import json
import pathlib
import shutil
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
KNOWN = {"dicom_export", "mwl", "report_sender"}

def check(errors, value, label):
    if not value:
        errors.append(label + ": missing")
    elif not (ROOT / str(value)).exists():
        errors.append(label + ": not found: " + str(value))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--secrets", default="")
    args = parser.parse_args()
    errors = []
    try:
        manifest = json.loads(pathlib.Path(args.manifest).read_text())
    except Exception as exc:
        print("FAIL manifest: " + str(exc), file=sys.stderr)
        return 2
    if manifest.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if not manifest.get("lab_id") or manifest.get("lab_id") == "replace-me":
        errors.append("lab_id must be set")
    services = manifest.get("services", {})
    if not isinstance(services, dict):
        errors.append("services must be an object")
        services = {}
    for name in set(services) - KNOWN:
        errors.append("unknown service: " + name)
    enabled = []
    for name, cfg in services.items():
        if not isinstance(cfg, dict) or not cfg.get("enabled"):
            continue
        enabled.append(name)
        check(errors, cfg.get("requirements"), name + ".requirements")
        check(errors, cfg.get("entry"), name + ".entry")
        if name != "mwl":
            check(errors, cfg.get("config"), name + ".config")
            check(errors, cfg.get("template"), name + ".template")
        else:
            configs = cfg.get("configs") or []
            if not configs:
                errors.append("mwl.configs: must contain at least one config")
            for i, item in enumerate(configs):
                if isinstance(item, dict):
                    check(errors, item.get("config"), "mwl.configs[" + str(i) + "].config")
                    check(errors, item.get("template"), "mwl.configs[" + str(i) + "].template")
                else:
                    check(errors, item, "mwl.configs[" + str(i) + "]")
    state = manifest.get("state", {})
    if state.get("backup_required") and not state.get("paths"):
        errors.append("state.paths must be listed when backup_required is true")
    if args.secrets:
        path = pathlib.Path(args.secrets)
        if not path.exists():
            errors.append("secrets file not found: " + args.secrets)
        else:
            names = {line.split("=", 1)[0].strip() for line in path.read_text().splitlines() if "=" in line and not line.lstrip().startswith("#")}
            for required in ("LABBIT_ORTHANC_PASSWORD", "LABBIT_CORE_API_KEY_ID", "LABBIT_CORE_API_SECRET"):
                if required not in names:
                    errors.append("secrets missing variable: " + required)
    print("Manifest: " + str(args.manifest))
    print("Lab: " + str(manifest.get("lab_id", "-")))
    print("Enabled services: " + (", ".join(enabled) if enabled else "none"))
    print("Python: " + str(shutil.which("python3") or "not found"))
    print("PM2: " + str(shutil.which("pm2") or "not found"))
    if errors:
        print("FAIL")
        for error in errors:
            print("- " + error)
        return 1
    print("PASS: manifest and repository prerequisites are present")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
