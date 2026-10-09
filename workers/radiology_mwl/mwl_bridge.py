"""
Orthanc Python-plugin bridge for the radiology_mwl_worker.

The worker uses the json_base64 transport.  The bridge writes the complete
DICOM worklist outside Orthanc's watched directory and atomically renames it
into place only after the write is closed and flushed.  Orthanc's Worklists
plugin scans the watched directory, so exposing a partially written file can
make it parse invalid DICOM and can bring down the Orthanc process.
"""

import base64
import datetime
import json
import os
import re
import tempfile
import time
import traceback

import orthanc


WORKLIST_DIR = r"E:\OrthancWorklists"
LOG_PATH = r"E:\OrthancScripts\mwl_bridge.log"
TEMP_DIR = os.path.dirname(LOG_PATH)

_FILENAME_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _log(message):
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write("[%s] %s\n" % (datetime.datetime.now().isoformat(timespec="seconds"), message))
    except Exception:
        pass


def _write_worklist_atomically(target_path, file_bytes):
    """Expose a worklist only after the complete file is safely written.

    TEMP_DIR is deliberately outside WORKLIST_DIR.  The Orthanc Worklists
    plugin scans every file in WORKLIST_DIR, so a temporary file there would
    still be visible to the scanner.  os.replace() is an atomic same-volume
    rename on Windows once the source file has been closed.
    """
    os.makedirs(TEMP_DIR, exist_ok=True)
    temp_fd, temp_path = tempfile.mkstemp(
        prefix=".mwl_",
        suffix=".tmp",
        dir=TEMP_DIR,
    )
    try:
        with os.fdopen(temp_fd, "wb") as f:
            temp_fd = None
            f.write(file_bytes)
            f.flush()
            os.fsync(f.fileno())
        os.makedirs(WORKLIST_DIR, exist_ok=True)
        os.replace(temp_path, target_path)
        temp_path = None
    finally:
        if temp_fd is not None:
            try:
                os.close(temp_fd)
            except Exception:
                pass
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


def _handle(output, uri, request):
    method = request.get("method", "?")
    body = request.get("body", b"") or b""

    _log("request: method=%s body_len=%d" % (method, len(body)))

    if method != "POST":
        output.SendMethodNotAllowed("POST")
        return

    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8")

    payload = json.loads(body)

    b64 = payload.get("mwl_dicom_base64")
    if not b64:
        raise ValueError('request JSON has no "mwl_dicom_base64" field')

    file_bytes = base64.b64decode(b64, validate=True)
    if not file_bytes:
        raise ValueError("decoded MWL file is empty")

    fallback_name = "upload_%d.wl" % int(time.time() * 1000)
    raw_name = payload.get("mwl_file_name") or fallback_name
    safe_name = _FILENAME_SAFE.sub("_", os.path.basename(raw_name)) or fallback_name

    target_path = os.path.join(WORKLIST_DIR, safe_name)
    _write_worklist_atomically(target_path, file_bytes)

    _log("atomically wrote %s (%d bytes) accession=%r" % (
        target_path,
        len(file_bytes),
        payload.get("accession_number"),
    ))

    output.AnswerBuffer(
        json.dumps({"ok": True, "written": safe_name, "bytes": len(file_bytes)}),
        "application/json",
    )


def OnUpload(output, uri, **request):
    try:
        _handle(output, uri, request)
    except Exception as exc:
        tb = traceback.format_exc()
        _log("EXCEPTION: %s\n%s" % (exc, tb))
        try:
            output.AnswerBuffer(json.dumps({"ok": False, "error": str(exc)}), "application/json")
        except Exception:
            pass


orthanc.RegisterRestCallback("/mwl-bridge/upload", OnUpload)
