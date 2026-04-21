import os
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

log = logging.getLogger(__name__)

TWILIO_SID   = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_FROM  = os.getenv("TWILIO_FROM", "").strip() or os.getenv("TWILIO_WHATSAPP_FROM", "").strip().replace("whatsapp:", "")
ADAM_NUMBER  = os.getenv("ADAM_PHONE_NUMBER", "").strip() or os.getenv("ADAM_WHATSAPP_NUMBER", "").strip().replace("whatsapp:", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().strip('"').rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY", "").strip().strip('"')

OUTPUT_DIR   = Path(__file__).parent.parent / "output"
BUCKET       = "pipeline-batches"


def upload_excel_files(stem: str) -> bool:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("Supabase not configured — skipping upload")
        return False

    files = [
        f"{stem}_list1_qualified.xlsx",
        f"{stem}_list2_needs_fixing.xlsx",
        f"{stem}_list3_dnc.xlsx",
        f"{stem}_list4_funded.xlsx",
    ]

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    all_ok = True
    for fname in files:
        fpath = OUTPUT_DIR / fname
        if not fpath.exists():
            continue
        try:
            with open(fpath, "rb") as f:
                r = requests.post(
                    f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{stem}/{fname}",
                    headers={**headers,
                             "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             "x-upsert": "true"},
                    data=f,
                    timeout=60
                )
            if r.status_code in (200, 201):
                log.info(f"Uploaded to Supabase: {fname}")
            else:
                log.warning(f"Upload failed for {fname}: {r.status_code} {r.text[:100]}")
                all_ok = False
        except Exception as e:
            log.warning(f"Upload error for {fname}: {e}")
            all_ok = False

    return all_ok


def send_sms(stem: str, summary: dict) -> bool:
    if not TWILIO_SID or not TWILIO_TOKEN:
        log.warning("Twilio keys not set — SMS skipped")
        return False
    if not ADAM_NUMBER:
        log.warning("ADAM_PHONE_NUMBER not set — SMS skipped")
        return False
    if not TWILIO_FROM:
        log.warning("TWILIO_FROM not set — SMS skipped")
        return False

    qualified = summary.get("list1_qualified", 0)
    needs_fix = summary.get("list2_needs_fixing", 0)
    dnc       = summary.get("list3_dnc", 0)
    funded    = summary.get("list4_funded", 0)
    total     = summary.get("total_leads", 0)

    message = (
        f"SeaCap Lead Pipeline\n"
        f"Batch: {stem}\n\n"
        f"Qualified: {qualified:,}\n"
        f"Needs Fix: {needs_fix:,}\n"
        f"DNC: {dnc:,}\n"
        f"Funded: {funded:,}\n"
        f"Total: {total:,}\n\n"
        f"Reply to push:\n"
        f"1 = Close CRM\n"
        f"2 = VanillaSoft\n"
        f"3 = Both"
    )

    try:
        r = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
            auth=(TWILIO_SID, TWILIO_TOKEN),
            data={"From": TWILIO_FROM, "To": ADAM_NUMBER, "Body": message},
            timeout=15
        )
        if r.status_code == 201:
            log.info(f"SMS sent to {ADAM_NUMBER} for batch: {stem}")
            return True
        else:
            log.error(f"SMS failed: {r.status_code} {r.text[:200]}")
            return False
    except Exception as e:
        log.error(f"SMS error: {e}")
        return False


def notify(stem: str, summary: dict):
    log.info("Sending notification...")
    upload_excel_files(stem)
    send_sms(stem, summary)
