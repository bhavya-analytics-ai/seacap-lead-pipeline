"""
vanillasoft_push_leads.py
-------------------------
Pushes cleaned leads to VanillaSoft CRM via web-to-lead XML POST.
Called by push_leads.py when PUSH_TO=vanillasoft in .env

Requires in .env:
  VANILLASOFT_WEB_LEAD_ID   — from VanillaSoft Admin > Project Settings > API Integration
  PUSH_TO=vanillasoft
"""
import os
import json
import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

VANILLASOFT_WEB_LEAD_ID = os.getenv("VANILLASOFT_WEB_LEAD_ID", "").strip()
VANILLASOFT_URL = f"https://s2.vanillasoft.net/web/post.aspx?id={VANILLASOFT_WEB_LEAD_ID}"

OUTPUT_DIR  = Path(__file__).parents[1] / "output"
QUEUE_PATH  = Path(__file__).parent / "failed_vanilla_pushes.jsonl"
RETRY_DELAYS = [1, 5, 15]
MAX_QUEUE_ATTEMPTS = 10

LIST_MAP = {
    "list1_qualified":    "Pending 1",
    "list2_needs_fixing": "Pending 2",
    "list3_dnc":          "Pending 3",
    "list4_funded":       "Pending 4",
}

# ── RETRY ─────────────────────────────────────────────────────
def _with_retries(fn):
    last_err = None
    for delay in [0] + RETRY_DELAYS:
        if delay:
            time.sleep(delay)
        try:
            return fn(), None
        except Exception as e:
            last_err = str(e)
    return None, last_err

# ── QUEUE ─────────────────────────────────────────────────────
def _enqueue(payload):
    entry = {"queued_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "attempts": 0, "payload": payload}
    with open(QUEUE_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

def replay_queue():
    if not QUEUE_PATH.exists():
        return
    still_failing = []
    with open(QUEUE_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            entry["attempts"] = entry.get("attempts", 0) + 1
            result, err = _with_retries(lambda: _post_lead(entry["payload"]))
            if result:
                print(f"  [QUEUE] Replayed OK: {entry['payload'].get('company', '?')}")
            elif entry["attempts"] < MAX_QUEUE_ATTEMPTS:
                still_failing.append(entry)
            else:
                print(f"  [QUEUE] Giving up on: {entry['payload'].get('company', '?')}")

    if still_failing:
        with open(QUEUE_PATH, "w", encoding="utf-8") as f:
            for entry in still_failing:
                f.write(json.dumps(entry) + "\n")
    else:
        QUEUE_PATH.unlink(missing_ok=True)

# ── CORE: POST ONE LEAD ───────────────────────────────────────
def _post_lead(data: dict) -> bool:
    if not VANILLASOFT_WEB_LEAD_ID:
        raise Exception("VANILLASOFT_WEB_LEAD_ID not set in .env")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<lead>
  <fname>{data.get('fname', '')}</fname>
  <lname>{data.get('lname', '')}</lname>
  <company>{data.get('company', '')}</company>
  <workNumber>{data.get('phone1', '')}</workNumber>
  <mobileNumber>{data.get('phone2', '')}</mobileNumber>
  <email>{data.get('email1', '')}</email>
  <email2>{data.get('email2', '')}</email2>
  <address>{data.get('address1', '')}</address>
  <list>{data.get('list_value', '')}</list>
  <flag_reason>{data.get('flag_reason', '')}</flag_reason>
</lead>"""

    r = requests.post(
        VANILLASOFT_URL,
        data=xml.encode("utf-8"),
        headers={"Content-Type": "application/xml"},
        timeout=15
    )
    if r.status_code != 200 or "FAILURE" in r.text:
        raise Exception(f"VanillaSoft returned: {r.status_code} — {r.text[:200]}")
    return True


def push_to_vanillasoft(stem):
    total_ok = 0
    total_skipped = 0
    total_queued = 0

    print(f"Pushing leads to VanillaSoft — stem: {stem}", flush=True)

    for file_key, list_value in LIST_MAP.items():
        filepath = OUTPUT_DIR / f"{stem}_{file_key}.xlsx"
        if not filepath.exists():
            print(f"  Skipping {file_key} — file not found")
            continue

        df = pd.read_excel(filepath)
        print(f"\n  {list_value} — {len(df):,} leads", flush=True)

        for i, row in df.iterrows():
            company = str(row.get("Company") or row.get("Business Name") or "").strip()
            first   = str(row.get("First Name") or "").strip()
            last    = str(row.get("Last Name") or "").strip()

            if not company and not first and not last:
                total_skipped += 1
                continue

            data = {
                "fname":       first,
                "lname":       last,
                "company":     company,
                "phone1":      str(row.get("Best Phone") or "").strip(),
                "phone2":      str(row.get("SeaCap_Phone_2") or "").strip(),
                "email1":      str(row.get("Best Email") or "").strip(),
                "email2":      str(row.get("SeaCap_Email_2") or "").strip(),
                "address1":    str(row.get("Best Address") or "").strip(),
                "list_value":  list_value,
                "flag_reason": str(row.get("SeaCap_Flag_Reason") or "").strip(),
            }

            result, err = _with_retries(lambda d=data: _post_lead(d))
            if result:
                total_ok += 1
            else:
                total_queued += 1
                _enqueue(data)
                if total_queued <= 5:
                    print(f"  QUEUED row {i} ({company}): {err}", flush=True)

            if (i + 1) % 100 == 0:
                print(f"    {i+1:,} processed...", flush=True)

    print(f"\n✅ Done pushing to VanillaSoft:")
    print(f"  Pushed  : {total_ok:,}")
    print(f"  Skipped : {total_skipped:,}")
    print(f"  Queued  : {total_queued:,}  (will retry on next run)")
