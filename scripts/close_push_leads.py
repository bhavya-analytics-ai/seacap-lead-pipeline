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
import re
import requests as _requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")   # repo root

_api_key  = os.getenv('CLOSE_API_KEY')
_AUTH     = (_api_key, '')
_BASE_URL = 'https://api.close.com/api/v1'
_TIMEOUT  = 30  # seconds per request

OUTPUT_DIR  = Path(__file__).parents[1] / 'output'
QUEUE_PATH  = Path(__file__).parent / 'failed_close_pushes.jsonl'
RETRY_DELAYS = [1, 5, 15]
MAX_QUEUE_ATTEMPTS = 10

# Custom field IDs
ADAM_LIST_FIELD_ID = 'cf_XU1qHTFDucHvqGhXRwkbMtzj9v0F2nCZGes7N7rqID6'  # "Adam List" — choices field
BATCH_FIELD_ID     = 'cf_CCz7ZqoljRNXfg0A4w9zLAQAg0PJNGOY6hK9gncvOnO'  # "Batch Number" — source + date stem
TAG_FIELD_ID       = 'cf_rQD87n0fO25IW7SzDZFTLAGcWGBRH0JtE9JkFBawatz'  # "Tag" — hidden filter for pipeline leads

# Valid choices for Adam List: Pending 1/2/3/4 (pipeline inbox) → List 1/2/3/4 (after rep works them)
LIST_MAP = {
    'list1_qualified':    'Pending 1',
    'list2_needs_fixing': 'Pending 2',
    'list3_dnc':          'Pending 3',
    'list4_funded':       'Pending 4',
}

# Lead status per list — set on every push so new leads are visible
STATUS_MAP = {
    'Pending 1': 'stat_KAxW4CxmwBohKJChIIkIlLK9jKxLiTBDkERQo3Ra5jh',  # 🆕 New / Uncontacted
    'Pending 2': 'stat_9fDhBB6VEvWXtHGZ5FP3WxBKpAWS3Sm5es5Ggpaw8Pj',  # ⚠️ Invalid Contact Info
    'Pending 3': 'stat_voUtLGcfL5bTcw00K6Hxwe6efhcP0M8s7a8dH3dXCij',  # 🔴 Not Interested - DNC
    'Pending 4': 'stat_Tzu12vilJKdz1hrqghOOWVih4GpmNyJrqur5J439GUj',  # 🟢 Funded - SEACAP
}

# ── SOURCE NAME DETECTION ─────────────────────────────────────
def detect_source(stem: str) -> str:
    """Extract human-readable source name from filename stem.
    e.g. 'infousa_apr27' → 'Infousa', 'bizbuysell_2026-04-27' → 'Bizbuysell'
    Falls back to 'Batch' if nothing useful found.
    """
    # Remove date-like patterns: 2026-04-27, 20260427, apr27, 04-27, etc.
    cleaned = re.sub(r'\d{4}[-_]\d{2}[-_]\d{2}', '', stem)
    cleaned = re.sub(r'\d{6,8}', '', cleaned)
    cleaned = re.sub(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\d*', '', cleaned, flags=re.IGNORECASE)
    # Remove standalone numbers and common filler words
    cleaned = re.sub(r'\b(leads?|list|batch|file|data|upload|new|updated?)\b', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'[\d_\-]+', ' ', cleaned).strip()
    # Title-case and clean whitespace
    result = ' '.join(cleaned.split()).title()
    return result if len(result) >= 2 else 'Batch'

def build_batch_id(stem: str) -> str:
    """Build batch ID: '{stem} | YYYY-MM-DD HH:MM'"""
    return f"{stem} | {time.strftime('%Y-%m-%d %H:%M')}"

def create_smart_view(batch_id: str, name: str, list_value: str):
    """Create one Smart View filtered by batch + list value."""
    payload = {
        "name": name,
        "s_query": {
            "query": {
                "negate": False,
                "queries": [
                    {
                        "condition": {"type": "text", "mode": "full_words", "value": batch_id},
                        "field": {"custom_field_id": BATCH_FIELD_ID, "type": "custom_field"},
                        "negate": False, "type": "field_condition"
                    },
                    {
                        "condition": {"type": "term", "values": [list_value]},
                        "field": {"custom_field_id": ADAM_LIST_FIELD_ID, "type": "custom_field"},
                        "negate": False, "type": "field_condition"
                    },
                    {"negate": False, "object_type": "lead", "type": "object_type"}
                ],
                "type": "and"
            },
            "results_limit": None,
            "sort": []
        }
    }
    try:
        r = _requests.post(f'{_BASE_URL}/saved_search/', json=payload, auth=_AUTH, timeout=_TIMEOUT)
        if r.status_code in (200, 201):
            print(f'  📌 Smart View: "{name}"')
        else:
            print(f'  ⚠️  Smart View failed ({name}): {r.status_code} {r.text[:150]}')
    except Exception as e:
        print(f'  ⚠️  Smart View error: {e}')

def create_batch_smart_views(batch_id: str, source: str, counts: dict):
    """Create Smart Views for this batch. Always Qualified + Needs Fixing. DNC/Funded only if data exists."""
    ts = time.strftime('%b %-d, %-I:%M %p')
    prefix = f"{source} — {ts}"
    # Always create these two
    create_smart_view(batch_id, f"{prefix} — Qualified",    'Pending 1')
    create_smart_view(batch_id, f"{prefix} — Needs Fixing", 'Pending 2')
    # Only if data exists
    if counts.get('list3_dnc', 0) > 0:
        create_smart_view(batch_id, f"{prefix} — DNC",     'Pending 3')
    if counts.get('list4_funded', 0) > 0:
        create_smart_view(batch_id, f"{prefix} — Funded",  'Pending 4')

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
    Always creates new — no duplicate search (too slow on 218k leads).
    Sets Adam List + status on every lead so new batch is visible immediately.
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
    custom = {
        ADAM_LIST_FIELD_ID: list_value,
        TAG_FIELD_ID:       'sp',
    }
    if batch:
        custom[BATCH_FIELD_ID] = batch
    lead_data = {
        'name':     company or contact_name,
        'contacts': [contact],
        'custom':   custom,
    }
    if status_id:
        lead_data['status_id'] = status_id
    if addresses:
        lead_data['addresses'] = [{'address_1': a, 'type': 'business'} for a in addresses]

    def _post():
        r = _requests.post(f'{_BASE_URL}/lead/', json=lead_data, auth=_AUTH, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()

    result, err = _with_retries(_post)
    if result:
        return result['id'], 'created'
    raise Exception(err or "Unknown error creating lead")


# ── MAIN PUSH ─────────────────────────────────────────────────
WORKERS = 10  # parallel threads

def _progress(label, done, total):
    filled = int(20 * done / total) if total else 20
    bar = "█" * filled + "░" * (20 - filled)
    print(f'\r  {label}  [{bar}] {done}/{total}', end='', flush=True)

def push_to_close(stem):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    counters = {"created": 0, "skipped": 0, "queued": 0}
    lock = threading.Lock()

    batch_id = build_batch_id(stem)
    source   = detect_source(stem)
    print(f'Pushing to Close CRM — {stem}  [batch: {batch_id}]', flush=True)

    def _push_row(row_dict, list_value):
        flag_reason = str(row_dict.get('Flag Reason', row_dict.get('SeaCap_Flag_Reason', '')))
        try:
            result, action = find_or_create_lead(row_dict, list_value, flag_reason, batch=batch_id)
            with lock:
                if action == 'created':
                    counters["created"] += 1
                elif action is None:
                    counters["skipped"] += 1
        except Exception as e:
            with lock:
                counters["queued"] += 1
            _enqueue({"row": row_dict, "list_value": list_value, "flag_reason": flag_reason, "batch": batch_id})

    list_counts = {}
    for file_key, list_value in LIST_MAP.items():
        filepath = OUTPUT_DIR / f'{stem}_{file_key}.xlsx'
        if not filepath.exists():
            list_counts[file_key] = 0
            continue

        df = pd.read_excel(filepath)
        total = len(df)
        list_counts[file_key] = total
        print(f'\n  {list_value} ({total:,} leads)', flush=True)

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(_push_row, row.to_dict(), list_value)
                       for _, row in df.iterrows()]
            for i, f in enumerate(as_completed(futures), 1):
                _progress(list_value, i, total)
        print()

    print(f'\n✅ Done:  Created {counters["created"]:,}  |  Skipped {counters["skipped"]:,}  |  Queued {counters["queued"]:,}')
    create_batch_smart_views(batch_id, source, list_counts)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python close_push_leads.py <stem>')
        sys.exit(1)
    push_to_close(sys.argv[1])
