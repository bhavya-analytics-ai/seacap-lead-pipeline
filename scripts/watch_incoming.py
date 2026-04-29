import time
import shutil
import logging
import subprocess
import sys
import json
import os
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")   # repo root

SUPABASE_URL  = os.getenv("SUPABASE_URL", "").strip().strip('"').strip("'").strip()
SUPABASE_KEY  = (os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY") or "").strip().strip('"').strip("'").strip()
PUSH_TO_CLOSE    = os.getenv("PUSH_TO_CLOSE",    "true").strip().lower() != "false"
PUSH_TO_SUPABASE = os.getenv("PUSH_TO_SUPABASE", "true").strip().lower() != "false"

# ============================================================
# CONFIGURATION
# ============================================================
BASE_DIR      = Path(__file__).parent.parent   # lead-pipeline/
INCOMING_DIR  = BASE_DIR / "incoming"
PROCESSED_DIR = BASE_DIR / "processed"
OUTPUT_DIR    = BASE_DIR / "output"
CLEAN_SCRIPT  = BASE_DIR / "scripts" / "clean_leads.py"

# Only watch these file types
WATCHED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

# Delete files from processed/ older than this many days
PROCESSED_MAX_DAYS = 30

# ============================================================
# LOGGING
# ============================================================
class ConsoleFormatter(logging.Formatter):
    """Terminal: normal lines clean, errors = loud ANSI red block."""
    NORMAL_FMT = "%(asctime)s  %(levelname)-7s  %(message)s"

    def format(self, record):
        if record.levelno >= logging.ERROR:
            self._style._fmt = self.NORMAL_FMT
            base = super().format(record)
            RED   = "\033[1;97;41m"  # bold white on red bg
            RESET = "\033[0m"
            border = RED + "█" * 60 + RESET
            return f"\n{border}\n{RED}  ❌  ERROR  —  {record.getMessage()}{RESET}\n{border}\n"
        else:
            self._style._fmt = self.NORMAL_FMT
            return super().format(record)

class FileFormatter(logging.Formatter):
    """Log file: normal lines clean, errors = bordered block."""
    NORMAL_FMT = "%(asctime)s  %(levelname)-7s  %(message)s"
    ERROR_FMT  = (
        "\n" + "=" * 60 + "\n"
        "❌  ERROR  |  %(asctime)s\n"
        "%(message)s\n"
        + "=" * 60
    )

    def format(self, record):
        if record.levelno >= logging.ERROR:
            self._style._fmt = self.ERROR_FMT
        else:
            self._style._fmt = self.NORMAL_FMT
        return super().format(record)

from logging.handlers import RotatingFileHandler

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(ConsoleFormatter(datefmt="%Y-%m-%d %H:%M:%S"))

_file_handler = RotatingFileHandler(
    Path(__file__).parent / "pipeline.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding="utf-8"
)
_file_handler.setFormatter(FileFormatter(datefmt="%Y-%m-%d %H:%M:%S"))

logging.basicConfig(level=logging.INFO, handlers=[_console_handler, _file_handler])
log = logging.getLogger(__name__)

# ============================================================
# PIPELINE TRIGGER
# ============================================================
def run_pipeline(filepath: Path):
    """Run clean_leads.py on the given file, then move it to processed/."""
    log.info(f"NEW FILE DETECTED: {filepath.name}")

    # Small delay — make sure file is fully written before reading
    time.sleep(2)

    try:
        log.info(f"Running clean_leads.py on: {filepath.name}")
        import io
        proc = subprocess.Popen(
            [sys.executable, str(CLEAN_SCRIPT), str(filepath)],
            stdout=subprocess.PIPE,
            stderr=sys.stderr,   # stream progress bar live to terminal
            text=True,
        )
        try:
            stdout_data, _ = proc.communicate(timeout=1800)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout_data, _ = proc.communicate()
            raise subprocess.TimeoutExpired(proc.args, 1800)

        class _FakeResult:
            def __init__(self, returncode, stdout, stderr):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = ""
        result = _FakeResult(proc.returncode, stdout_data, "")

        if result.returncode == 0:
            # Parse JSON summary
            summary = {}
            for line in result.stdout.splitlines():
                if line.startswith("SUMMARY_JSON:"):
                    summary = json.loads(line.replace("SUMMARY_JSON:", ""))
                    break
            # Clean one-line summary instead of full stdout
            log.info(f"Pipeline complete: {filepath.name} | "
                     f"Total: {summary.get('total_leads', 0)} | "
                     f"Qualified: {summary.get('list1_qualified', 0)} | "
                     f"Needs Fix: {summary.get('list2_needs_fixing', 0)} | "
                     f"DNC: {summary.get('list3_dnc', 0)} | "
                     f"Funded: {summary.get('list4_funded', 0)}")
            log_to_supabase(filepath.name, summary, "success")

            stem = filepath.stem

            # Auto-push to Close CRM
            pushed_to = []
            if PUSH_TO_CLOSE:
                try:
                    push_from_local(stem, "1")
                    pushed_to.append("Close CRM")
                except Exception as e:
                    log.error(f"Close push failed: {e}")
            else:
                log.info("Close CRM push skipped (PUSH_TO_CLOSE=false)")

            # SMS notification — sent after push
            try:
                from whatsapp_notify import notify
                notify(stem, summary, pushed_to=pushed_to)
            except Exception as e:
                log.warning(f"SMS notify failed (non-fatal): {e}")
        else:
            # Extract human-readable reason from stdout
            reason = None
            if result.stdout and "FILE REJECTED" in result.stdout:
                lines = result.stdout.splitlines()
                start = 0
                for i, line in enumerate(lines):
                    if "FILE REJECTED" in line:
                        start = i - 1 if i > 0 and "=" * 10 in lines[i - 1] else i
                        break
                reason = "\n".join(lines[start:]).rstrip()
            if not reason:
                reason = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else "unknown error"

            log.error(f"PIPELINE FAILED — {filepath.name}\n\n{reason or 'unknown error'}\n")
            log_to_supabase(filepath.name, {}, "failed")

    except subprocess.TimeoutExpired:
        log.error(f"Timed out processing: {filepath.name}")
    except Exception as e:
        log.error(f"Unexpected error: {e}")

    finally:
        # Always move file to processed/ whether it succeeded or not
        dest = PROCESSED_DIR / filepath.name
        if dest.exists():
            ts = time.strftime("%Y%m%d_%H%M%S")
            dest = PROCESSED_DIR / f"{filepath.stem}_{ts}{filepath.suffix}"

        shutil.move(str(filepath), str(dest))
        log.info(f"Moved to processed/: {dest.name}")

        # Upload original + cleaned outputs to Supabase Storage
        if PUSH_TO_SUPABASE:
            try:
                upload_original_to_supabase(dest)
            except Exception as e:
                log.warning(f"Original upload to Supabase failed (non-fatal): {e}")
            try:
                upload_outputs_to_supabase(filepath.stem)
            except Exception as e:
                log.warning(f"Output upload to Supabase failed (non-fatal): {e}")
        else:
            log.info("Supabase upload skipped (PUSH_TO_SUPABASE=false)")

        log.info("-" * 60)

# ============================================================
# POPUP + LOCAL PUSH
# ============================================================
def push_from_local(stem: str, choice: str):
    """Push leads to Close CRM."""
    push_script = BASE_DIR / "scripts" / "close_push_leads.py"
    if not push_script.exists():
        log.warning("close_push_leads.py not found — skipping")
        return
    log.info(f"Pushing to Close CRM: {stem}")
    result = subprocess.run(
        [sys.executable, str(push_script), stem],
        capture_output=True, text=True, timeout=3600
    )
    if result.returncode == 0:
        log.info("Close CRM push complete")
    else:
        log.error(f"Close CRM push failed: {result.stderr.strip()}")


def show_popup(stem: str, summary: dict):
    """Show Windows popup asking where to push. Windows only — skipped on Mac/Linux."""
    import sys
    if sys.platform != "win32":
        log.info("Popup skipped (Windows only)")
        return
    import threading

    def _popup():
        try:
            import tkinter as tk
            from tkinter import messagebox

            qualified = summary.get("list1_qualified", 0)
            needs_fix = summary.get("list2_needs_fixing", 0)
            dnc       = summary.get("list3_dnc", 0)
            total     = summary.get("total_leads", 0)

            root = tk.Tk()
            root.withdraw()

            msg = (
                f"Pipeline complete: {stem}\n\n"
                f"✅ Qualified:  {qualified:,}\n"
                f"🔧 Needs Fix:  {needs_fix:,}\n"
                f"🚫 DNC:        {dnc:,}\n"
                f"📊 Total:      {total:,}\n\n"
                f"Push to Close CRM?\n"
                f"(Adam can also approve via WhatsApp)"
            )

            push_close = messagebox.askyesno("SeaCap Lead Pipeline", msg)
            root.destroy()

            if not push_close:
                log.info("Local popup: push skipped by user")
                return

            log.info("Local popup: pushing to Close CRM")
            push_from_local(stem, "1")

        except Exception as e:
            log.warning(f"Popup failed (non-fatal): {e}")

    threading.Thread(target=_popup, daemon=True).start()


# ============================================================
# FILE WATCHER
# ============================================================
class IncomingHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        filepath = Path(event.src_path)
        if filepath.suffix.lower() in WATCHED_EXTENSIONS:
            run_pipeline(filepath)

    def on_moved(self, event):
        # Catches files dragged/moved into the folder
        if event.is_directory:
            return
        filepath = Path(event.dest_path)
        if filepath.suffix.lower() in WATCHED_EXTENSIONS:
            run_pipeline(filepath)

# ============================================================
# SUPABASE ORIGINAL UPLOAD
# ============================================================
def upload_original_to_supabase(filepath: Path):
    """Upload original incoming file to Supabase Storage under originals/YYYY-MM-DD/."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("Supabase not configured — skipping original upload")
        return
    import requests
    date_folder = time.strftime("%Y-%m-%d")
    fname       = filepath.name
    url         = f"{SUPABASE_URL}/storage/v1/object/pipeline-batches/originals/{date_folder}/{fname}"
    ext         = filepath.suffix.lower()
    ct          = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if ext in (".xlsx", ".xls") else "text/csv"
    with open(filepath, "rb") as f:
        r = requests.post(url, headers={
            "apikey":         SUPABASE_KEY,
            "Authorization":  f"Bearer {SUPABASE_KEY}",
            "Content-Type":   ct,
            "x-upsert":       "true"
        }, data=f, timeout=60)
    if r.status_code in (200, 201):
        log.info(f"Original uploaded to Supabase: originals/{date_folder}/{fname}")
    else:
        log.debug(f"Original upload skipped (Storage RLS): {r.status_code}")

def upload_outputs_to_supabase(stem: str):
    """Upload all cleaned output Excels to Supabase Storage under outputs/YYYY-MM-DD/{stem}/."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    import requests
    date_folder = time.strftime("%Y-%m-%d")
    suffixes = [
        f"{stem}_list1_qualified.xlsx",
        f"{stem}_list2_needs_fixing.xlsx",
        f"{stem}_list3_dnc.xlsx",
        f"{stem}_list4_funded.xlsx",
        f"{stem}_all.xlsx",
        f"{stem}_flagged_for_review.xlsx",
    ]
    uploaded = 0
    for fname in suffixes:
        fpath = OUTPUT_DIR / fname
        if not fpath.exists():
            continue
        url = f"{SUPABASE_URL}/storage/v1/object/pipeline-batches/outputs/{date_folder}/{stem}/{fname}"
        with open(fpath, "rb") as f:
            r = requests.post(url, headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "x-upsert":      "true"
            }, data=f, timeout=60)
        if r.status_code in (200, 201):
            uploaded += 1
        else:
            log.debug(f"Output upload skipped ({fname}): {r.status_code}")
    if uploaded:
        log.info(f"Uploaded {uploaded} output file(s) to Supabase: outputs/{date_folder}/{stem}/")

# ============================================================
# MAIN
# ============================================================
def log_to_supabase(file_name, summary, status):
    """Log pipeline run to Supabase pipeline_logs table."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("Supabase not configured — skipping log")
        return
    try:
        from supabase import create_client
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        client.table("pipeline_logs").insert({
            "file_name":          file_name,
            "total_leads":        summary.get("total_leads", 0),
            "list1_qualified":    summary.get("list1_qualified", 0),
            "list2_needs_fixing": summary.get("list2_needs_fixing", 0),
            "list3_dnc":          summary.get("list3_dnc", 0),
            "list4_funded":       summary.get("list4_funded", 0),
            "flagged_duplicates": summary.get("flagged_duplicates", 0),
            "status":             status
        }).execute()
        log.info(f"Logged to Supabase: {file_name}")
    except Exception as e:
        log.error(f"Supabase log failed: {e}")

def cleanup_processed():
    """Delete files in processed/ older than PROCESSED_MAX_DAYS days."""
    now = time.time()
    cutoff = now - (PROCESSED_MAX_DAYS * 86400)
    for f in PROCESSED_DIR.iterdir():
        if f.is_file() and f.stat().st_mtime < cutoff:
            f.unlink()
            log.info(f"Deleted old processed file: {f.name} (older than {PROCESSED_MAX_DAYS} days)")

if __name__ == "__main__":
    # Make sure folders exist
    INCOMING_DIR.mkdir(exist_ok=True)
    PROCESSED_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Drain any failed Close pushes in background — don't block startup
    import threading
    def _replay():
        try:
            from close_push_leads import replay_queue
            replay_queue()
        except Exception as e:
            log.warning(f"Queue replay failed (non-fatal): {e}")
    threading.Thread(target=_replay, daemon=True).start()

    # Clean up old processed files on startup
    cleanup_processed()

    log.info("=" * 60)
    log.info("SeaCap Lead Pipeline — Folder Watcher Started")
    log.info(f"Watching: {INCOMING_DIR}")
    log.info(f"Processed files go to: {PROCESSED_DIR}")
    log.info(f"Output files go to: {OUTPUT_DIR}")
    log.info("Drop any CSV or Excel file into incoming/ to trigger the pipeline")
    log.info("=" * 60)

    handler = IncomingHandler()
    observer = Observer()
    observer.schedule(handler, str(INCOMING_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Watcher stopped.")
        observer.stop()

    observer.join()
