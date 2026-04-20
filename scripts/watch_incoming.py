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
SUPABASE_KEY  = os.getenv("SUPABASE_ANON_KEY", "").strip().strip('"').strip("'").strip()

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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent / "pipeline.log", encoding="utf-8"),
    ]
)
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
        result = subprocess.run(
            [sys.executable, str(CLEAN_SCRIPT), str(filepath)],
            capture_output=True,
            text=True,
            timeout=600  # 10 min max
        )

        if result.returncode == 0:
            log.info(f"Pipeline complete for: {filepath.name}")
            if result.stdout:
                log.info(result.stdout.strip())
            # Parse JSON summary and log to Supabase
            summary = {}
            for line in result.stdout.splitlines():
                if line.startswith("SUMMARY_JSON:"):
                    summary = json.loads(line.replace("SUMMARY_JSON:", ""))
                    break
            log_to_supabase(filepath.name, summary, "success")

            # Upload files + notify Adam via WhatsApp
            stem = filepath.stem
            try:
                from whatsapp_notify import notify
                notify(stem, summary)
            except Exception as e:
                log.warning(f"WhatsApp notify failed (non-fatal): {e}")

            # Windows popup — person on computer can also approve
            show_popup(stem, summary)
        else:
            log.error(f"Pipeline failed for: {filepath.name}")
            if result.stderr:
                log.error(result.stderr.strip())
            log_to_supabase(filepath.name, {}, "failed")

    except subprocess.TimeoutExpired:
        log.error(f"Timed out processing: {filepath.name}")
    except Exception as e:
        log.error(f"Unexpected error: {e}")

    finally:
        # Always move file to processed/ whether it succeeded or not
        dest = PROCESSED_DIR / filepath.name
        # If file with same name already in processed, add timestamp
        if dest.exists():
            ts = time.strftime("%Y%m%d_%H%M%S")
            dest = PROCESSED_DIR / f"{filepath.stem}_{ts}{filepath.suffix}"

        shutil.move(str(filepath), str(dest))
        log.info(f"Moved to processed/: {dest.name}")
        log.info("-" * 60)

# ============================================================
# POPUP + LOCAL PUSH
# ============================================================
def push_from_local(stem: str, choice: str):
    """Push leads to CRM based on choice: 1=Close, 2=VanillaSoft, 3=Both."""
    targets = []
    if choice in ["1", "3"]:
        targets.append(("close_push_leads.py", "Close CRM"))
    if choice in ["2", "3"]:
        targets.append(("vanillasoft_push_leads.py", "VanillaSoft"))

    for script_name, crm_name in targets:
        push_script = BASE_DIR / "scripts" / script_name
        if not push_script.exists():
            log.warning(f"{script_name} not found — skipping {crm_name}")
            continue
        log.info(f"Pushing to {crm_name}: {stem}")
        result = subprocess.run(
            [sys.executable, str(push_script), stem],
            capture_output=True, text=True, timeout=3600
        )
        if result.returncode == 0:
            log.info(f"{crm_name} push complete")
        else:
            log.error(f"{crm_name} push failed: {result.stderr.strip()}")


def show_popup(stem: str, summary: dict):
    """Show Windows popup asking where to push. Non-blocking thread."""
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

            # Ask Yes/No for Close first
            push_close = messagebox.askyesno("SeaCap Lead Pipeline", msg)

            # Ask Yes/No for VanillaSoft
            push_vanilla = messagebox.askyesno(
                "SeaCap Lead Pipeline",
                f"Also push to VanillaSoft?"
            )

            root.destroy()

            if push_close and push_vanilla:
                choice = "3"
            elif push_close:
                choice = "1"
            elif push_vanilla:
                choice = "2"
            else:
                log.info("Local popup: push skipped by user")
                return

            log.info(f"Local popup: pushing choice={choice}")
            push_from_local(stem, choice)

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

    # Drain any failed Close pushes from previous runs
    try:
        from close_push_leads import replay_queue
        replay_queue()
    except Exception as e:
        log.warning(f"Queue replay failed (non-fatal): {e}")

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
