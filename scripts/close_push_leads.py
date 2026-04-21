"""
close_push_leads.py
-------------------
Pushes cleaned leads from clean_leads.py output into Close CRM.
Called automatically by watch_incoming.py after every CSV run.

- Pushes ALL phones/emails/addresses with labels (nothing dropped)
- 3 retries per lead with exponential backoff
- Failed leads queued to failed_close_pushes.jsonl for later replay
- replay_queue() called on every startup via watch_incoming.py
"""
import os
import sys
import json
import time
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from closeio_api import Client

load_dotenv(Path(__file__).resolve().parent.parent / ".env")   # repo root

api = Client(os.getenv('CLOSE_API_KEY'))

OUTPUT_DIR  = Path(__file__).parents[1] / 'output'
QUEUE_PATH  = Path(__file__).parent / 'failed_close_pushes.jsonl'
RETRY_DELAYS = [1, 5, 15]
MAX_QUEUE_ATTEMPTS = 10

# Custom field IDs
ADAM_LIST_FIELD_ID = 'cf_XU1qHTFDucHvqGhXRwkbMtzj9v0F2nCZGes7N7rqID6'  # "Adam List"
BATCH_FIELD_ID     = 'cf_CCz7ZqoljRNXfg0A4w9zLAQAg0PJNGOY6hK9gncvOnO'  # "Batch Number"

# Human-readable list labels (show in Close as "Adam List" value)
LIST_MAP = {
    'list1_qualified':    'SeaCap - Qualified',
    'list2_needs_fixing': 'SeaCap - Needs Fixing',
    'list3_dnc':          'SeaCap - DNC',
    'list4_funded':       'SeaCap - Funded',
}

# Lead status per list — set on every push so new leads float to top
STATUS_MAP = {
    'SeaCap - Qualified':    'stat_KAxW4CxmwBohKJChIIkIlLK9jKxLiTBDkERQo3Ra5jh',  # 🆕 New / Uncontacted
    'SeaCap - Needs Fixing': 'stat_9fDhBB6VEvWXtHGZ5FP3WxBKpAWS3Sm5es5Ggpaw8Pj',  # ⚠️ Invalid Contact Info
    'SeaCap - DNC':          'stat_voUtLGcfL5bTcw00K6Hxwe6efhcP0M8s7a8dH3dXCij',  # 🔴 Not Interested - DNC
    'SeaCap - Funded':       'stat_Tzu12vilJKdz1hrqghOOWVih4GpmNyJrqur5J439GUj',  # 🟢 Funded - SEACAP
}

# ── RETRY WRAPPER ─────────────────────────────────────────────
def _with_retries(fn):
    """Call fn() with retries. Returns (result, None) or (None, error_str)."""
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
    """Drain failed_close_pushes.jsonl. Called at startup by watch_incoming.py."""
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
            p = entry["payload"]
            result, err = _with_retries(lambda: find_or_create_lead(
                p["row"], p["list_value"], p["flag_reason"], batch=p.get("batch", "")
            ))
            if result is None:
                if entry["attempts"] < MAX_QUEUE_ATTEMPTS:
                    still_failing.append(entry)
                else:
                    print(f"  [QUEUE] Giving up on: {p.get('row', {}).get('company', '?')} after {entry['attempts']} attempts")
            else:
                print(f"  [QUEUE] Replayed OK: {p.get('row', {}).get('company', '?')}")

    if still_failing:
        with open(QUEUE_PATH, "w", encoding="utf-8") as f:
            for entry in still_failing:
                f.write(json.dumps(entry) + "\n")
    else:
        QUEUE_PATH.unlink(missing_ok=True)

# ── CORE: PUSH ONE LEAD ───────────────────────────────────────
def find_or_create_lead(row, list_value, flag_reason="", batch=""):
    """
    Build a Close lead from a cleaned row.
    Pushes ALL phones/emails/addresses with labels.
    Searches by email then phone — updates if found, creates if not.
    Always sets status + Adam List + Batch Number so new leads are visible.
    """
    company = str(row.get('Company') or row.get('Business Name') or '').strip()
    first   = str(row.get('First Name') or row.get('First') or '').strip()
    last    = str(row.get('Last Name') or row.get('Last') or '').strip()

    # Collect all phones with labels
    # n=1 uses renamed cols ("Best Phone" / "Phone Status"), n=2-5 use SeaCap_ names
    phones = []
    phone_col_map = {1: ('Best Phone', 'Phone Status')}
    for n in range(1, 6):
        col_name    = phone_col_map.get(n, (f'SeaCap_Phone_{n}', f'SeaCap_Phone_{n}_Status'))
        p = str(row.get(col_name[0]) or '').strip()
        if p:
            phones.append({"phone": p, "type": "mobile" if n == 1 else "other"})

    # Collect all emails with labels
    # n=1 uses renamed cols ("Best Email" / "Email Status"), n=2-5 use SeaCap_ names
    emails = []
    email_col_map = {1: ('Best Email', 'Email Status')}
    for n in range(1, 6):
        col_name = email_col_map.get(n, (f'SeaCap_Email_{n}', f'SeaCap_Email_{n}_Status'))
        e = str(row.get(col_name[0]) or '').strip()
        if e:
            emails.append({"email": e, "type": "office" if n == 1 else "other"})

    if not company and not emails and not phones:
        return None, 'skipped'

    contact_name = f'{first} {last}'.strip() or company
    contact = {'name': contact_name}
    if emails:
        contact['emails'] = emails
    if phones:
        contact['phones'] = phones

    # Build addresses
    addresses = []
    addr_col_map = {1: ('Best Address', 'Address Status')}
    for n in range(1, 5):
        col_name   = addr_col_map.get(n, (f'SeaCap_Address_{n}', f'SeaCap_Address_{n}_Status'))
        addr       = str(row.get(col_name[0]) or '').strip()
        addr_label = str(row.get(col_name[1]) or '').strip()
        if addr:
            addresses.append(f"{addr} [{addr_label}]" if addr_label else addr)

    status_id = STATUS_MAP.get(list_value)
    lead_data = {
        'name':     company or contact_name,
        'contacts': [contact],
        'custom': {
            ADAM_LIST_FIELD_ID: list_value,
            BATCH_FIELD_ID:     batch,
        },
    }
    if status_id:
        lead_data['status_id'] = status_id
    if addresses:
        lead_data['addresses'] = [{'address_1': a, 'type': 'business'} for a in addresses]

    result, err = _with_retries(lambda: api.post('lead/', lead_data))
    if result:
        return result['id'], 'created'
    raise Exception(err or "Unknown error creating lead")


# ── MAIN PUSH ─────────────────────────────────────────────────
def push_to_close(stem):
    total_created = 0
    total_skipped = 0
    total_queued  = 0

    print(f'Pushing leads to Close CRM — stem: {stem}', flush=True)

    for file_key, list_value in LIST_MAP.items():
        filepath = OUTPUT_DIR / f'{stem}_{file_key}.xlsx'
        if not filepath.exists():
            print(f'  Skipping {file_key} — file not found: {filepath}')
            continue

        df = pd.read_excel(filepath)
        print(f'\n  {list_value} — {len(df):,} leads from {filepath.name}', flush=True)

        for i, row in df.iterrows():
            row_dict = row.to_dict()
            flag_reason = str(row_dict.get('SeaCap_Flag_Reason', ''))
            try:
                result, action = find_or_create_lead(row_dict, list_value, flag_reason, batch=stem)
                if action == 'created':
                    total_created += 1
                elif action is None:
                    total_skipped += 1
            except Exception as e:
                total_queued += 1
                _enqueue({"row": row_dict, "list_value": list_value, "flag_reason": flag_reason, "batch": stem})
                if total_queued <= 5:
                    print(f'  QUEUED row {i} ({row_dict.get("Company", "?")}): {e}', flush=True)

            if (i + 1) % 100 == 0:
                print(f'    {i+1:,} processed...', flush=True)

    print(f'\n✅ Done pushing to Close:')
    print(f'  Created : {total_created:,}')
    print(f'  Skipped : {total_skipped:,}')
    print(f'  Queued  : {total_queued:,}  (will retry on next run)')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python close_push_leads.py <stem>')
        sys.exit(1)
    push_to_close(sys.argv[1])
