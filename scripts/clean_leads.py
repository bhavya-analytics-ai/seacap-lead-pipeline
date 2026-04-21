import re
import sys
import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

# ============================================================
# ENV & PATHS
# ============================================================
load_dotenv(Path(__file__).resolve().parent.parent / ".env")   # repo root

GOOGLE_API_KEY       = os.getenv("GOOGLE_MAPS_API_KEY", "").strip().strip('"')
MILLIONVERIFIER_KEY  = os.getenv("MILLIONVERIFIER_API_KEY", "").strip().strip('"')
COBALT_KEY           = os.getenv("COBALT_API_KEY", "").strip().strip('"')
TWILIO_SID           = os.getenv("TWILIO_ACCOUNT_SID", "").strip().strip('"')
TWILIO_TOKEN         = os.getenv("TWILIO_AUTH_TOKEN", "").strip().strip('"')

def _pick_input():
    if len(sys.argv) > 1:
        return sys.argv[1]
    incoming = Path(__file__).parent.parent / "incoming"
    files = sorted(
        [f for f in incoming.iterdir() if f.suffix.lower() in {".csv", ".xlsx", ".xls"}],
        key=lambda f: f.stat().st_mtime, reverse=True
    )
    if files:
        return str(files[0])
    print("❌ No file in incoming/ and no file passed as argument.")
    sys.exit(1)

INPUT_FILE = _pick_input()
OUTPUT_DIR = Path(__file__).parent.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
STEM = Path(INPUT_FILE).stem

# Set to a number to only process that many rows (saves API credits while testing)
# Set to None to process the full file
TEST_MODE = 20

# ============================================================
# COLUMN DETECTION
# ============================================================
PHONE_COLS   = ["Mobile", "MainPhone", "Phone", "Phone Number", "Cell", "CellPhone",
                "WorkPhone", "Telephone", "Phone1", "Phone2", "Phone3", "Phone 1",
                "Phone 2", "Phone 3", "Alt Phone", "AltPhone", "Other Phone", "Fax"]
EMAIL_COLS   = ["Email", "Email Address", "EmailAddress", "E-mail", "Email1", "Email2",
                "Email 1", "Email 2", "Alt Email", "AltEmail", "Work Email", "Personal Email"]
COMPANY_COLS = ["Company", "Company Name", "Business Name", "Business", "DBA", "Merchant Name"]
FIRST_COLS   = ["First Name", "FirstName", "First", "fname"]
LAST_COLS    = ["Last Name", "LastName", "Last", "lname"]
STATUS_COLS  = ["Lead Status", "Status", "LeadStatus", "Disposition"]
SOURCE_COLS  = ["Lead Source", "Source", "LeadSource", "Campaign"]
ADDRESS_COLS = ["Address", "Address1", "Address 1", "Street", "Street Address",
                "Mailing Address", "Business Address", "Home Address", "Billing Address",
                "Address Line 1", "Addr1", "Addr"]

def find_col(df, options):
    for c in options:
        if c in df.columns:
            return c
    return None

def find_all_cols(df, options, pattern_fn=None):
    """Find ALL matching columns (not just first). Used for multi-phone/email/address."""
    found = []
    for c in df.columns:
        if c in options:
            found.append(c)
    # Also detect by pattern if function provided
    if pattern_fn:
        for c in df.columns:
            if c in found:
                continue
            sample = df[c].dropna().astype(str).head(30)
            if len(sample) == 0:
                continue
            hit_rate = sample.apply(pattern_fn).sum() / len(sample)
            if hit_rate >= 0.7:
                found.append(c)
    return found

def detect_by_pattern(df, field, already_found, used_cols):
    if already_found:
        return already_found

    def is_phone(val):
        raw = str(val).split(".")[0].strip()
        digits = re.sub(r'\D', '', raw)
        return len(digits) in [10, 11]

    def is_email(val):
        return "@" in str(val) and "." in str(val).split("@")[-1]

    def is_name(val):
        v = str(val).strip()
        return bool(re.match(r'^[A-Za-z\s\'\-\.]+$', v)) and 2 <= len(v) <= 40

    def is_company(val):
        v = str(val).strip()
        return len(v) > 2 and not re.match(r'^[\d\s\(\)\-\+]+$', v)

    checkers = {
        "phone":   is_phone,
        "email":   is_email,
        "first":   is_name,
        "last":    is_name,
        "company": is_company,
    }

    checker = checkers.get(field)
    if not checker:
        return None

    for col in df.columns:
        if col in used_cols:
            continue
        sample = df[col].dropna().astype(str).head(30)
        if len(sample) == 0:
            continue
        hit_rate = sample.apply(checker).sum() / len(sample)
        if hit_rate >= 0.7:
            print(f"   ⚠️  '{field}' not found by name — detected by pattern in column: '{col}' ({int(hit_rate*100)}% match)")
            return col
    return None

# ============================================================
# CLEANING HELPERS
# ============================================================
FAKE_NAMES = {"test","user","john doe","jane doe","na","n/a","none","unknown","asdf","admin","no name","sample"}
HONORIFICS = re.compile(r'^(mr\.?|mrs\.?|ms\.?|dr\.?|prof\.?|jr\.?|sr\.?)\s+', re.I)
COMPANY_INDICATORS = re.compile(r'\b(llc|inc|corp|ltd|co\.|company|enterprises|group|solutions|services|consulting)\b', re.I)
ILLEGAL_CHARS = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

def strip_illegal(val):
    if pd.isna(val):
        return val
    return ILLEGAL_CHARS.sub('', str(val))

def clean_money(val):
    if pd.isna(val) or not str(val).strip():
        return None
    cleaned = re.sub(r'[^\d.]', '', str(val))
    try:
        return float(cleaned)
    except:
        return None

def clean_name(val):
    if pd.isna(val) or not str(val).strip():
        return ""
    name = HONORIFICS.sub('', str(val).strip()).strip().title()
    if any(f in name.lower() for f in FAKE_NAMES):
        return ""
    if re.search(r'\d', name):
        return ""  # has digits → not a real name
    if COMPANY_INDICATORS.search(name):
        return ""  # looks like a company name
    return name

def clean_phone(val):
    if pd.isna(val) or not str(val).strip():
        return ""
    raw = str(val).strip().split(".")[0]
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return ""

def clean_email(val):
    if pd.isna(val) or not str(val).strip():
        return ""
    email = str(val).strip().lower()
    if "@" in email and "." in email.split("@")[-1]:
        return email
    return ""

def is_garbage_business(val):
    """Returns reason string if business name looks like garbage, else empty string."""
    if not val or pd.isna(val):
        return ""
    v = str(val).strip()
    digits = re.sub(r'\D', '', v)
    if len(digits) >= 7:
        return "Business name looks like a phone number"
    if re.match(r'^\d+\s+\w+', v):
        return "Business name looks like an address"
    if re.match(r'^[A-Za-z\s\'\-\.]+$', v) and 2 <= len(v) <= 30 and not COMPANY_INDICATORS.search(v):
        # Looks like a person's name not a company — don't flag, could be sole prop
        pass
    return ""

def clean_date(val):
    """Parse any date format to ISO YYYY-MM-DD, blank if unparseable."""
    if pd.isna(val) or not str(val).strip():
        return ""
    try:
        return pd.to_datetime(val, infer_datetime_format=True, errors='coerce').strftime('%Y-%m-%d')
    except:
        return ""

def split_full_name(val):
    """Split 'John Smith' into ('John', 'Smith'). Returns (first, last)."""
    if not val or pd.isna(val):
        return "", ""
    parts = str(val).strip().split()
    if len(parts) >= 2:
        return parts[0].title(), " ".join(parts[1:]).title()
    return str(val).strip().title(), ""

# ============================================================
# VERIFICATION
# ============================================================

def verify_email_millionverifier(email):
    """Returns: VERIFIED / UNVERIFIED / INVALID / DISPOSABLE / SPAMTRAP / FORMAT-BAD / UNKNOWN"""
    if not email:
        return "FORMAT-BAD"
    if not MILLIONVERIFIER_KEY:
        return "UNKNOWN"
    try:
        r = requests.get(
            "https://api.millionverifier.com/api/v3/",
            params={"api": MILLIONVERIFIER_KEY, "email": email},
            timeout=5
        )
        data = r.json()
        result = data.get("result", "").lower()
        quality = data.get("quality", "").lower()
        if result in ["ok", "valid"]:
            return "VERIFIED"
        elif result == "disposable" or quality == "disposable":
            return "DISPOSABLE"
        elif result == "spamtrap":
            return "SPAMTRAP"
        elif result in ["error", "invalid"]:
            return "INVALID"
        return "UNVERIFIED"
    except Exception:
        return "UNKNOWN"

def verify_business_google(company):
    """Returns dict: exists, found_phone, found_address, status"""
    if not company or not GOOGLE_API_KEY:
        return {"exists": None, "found_phone": "", "found_address": "", "status": "UNKNOWN"}
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
            params={
                "input": company,
                "inputtype": "textquery",
                "fields": "name,formatted_phone_number,business_status,formatted_address",
                "key": GOOGLE_API_KEY
            },
            timeout=5
        )
        data = r.json()
        candidates = data.get("candidates", [])
        if not candidates:
            return {"exists": False, "found_phone": "", "found_address": "", "status": "NOT-FOUND"}
        place = candidates[0]
        biz_status = place.get("business_status", "").upper()
        exists = biz_status in ["OPERATIONAL", ""]
        return {
            "exists": exists,
            "found_phone": clean_phone(place.get("formatted_phone_number", "")),
            "found_address": place.get("formatted_address", ""),
            "status": "VERIFIED-ACTIVE" if exists else "VERIFIED-INACTIVE"
        }
    except Exception:
        return {"exists": None, "found_phone": "", "found_address": "", "status": "UNKNOWN"}

def verify_sos_cobalt(company):
    """Returns: active / inactive / unknown"""
    if not company or not COBALT_KEY:
        return "UNKNOWN"
    try:
        r = requests.get(
            "https://api.cobaltintelligence.com/v1/search",
            headers={"Authorization": f"Bearer {COBALT_KEY}"},
            params={"name": company},
            timeout=8
        )
        if r.status_code != 200:
            return "UNKNOWN"
        data = r.json()
        results = data.get("data", [])
        if not results:
            return "UNKNOWN"
        status = str(results[0].get("status", "")).lower()
        if "active" in status or "good standing" in status:
            return "active"
        elif "inactive" in status or "dissolved" in status or "revoked" in status:
            return "inactive"
        return "UNKNOWN"
    except Exception:
        return "UNKNOWN"

def verify_phone_twilio(phone):
    """
    Returns dict: label, caller_name, line_type
    label: VERIFIED / VERIFIED-NO-NAME / SECONDARY / MISMATCH / VOIP / FORMAT-BAD / UNKNOWN
    """
    if not phone:
        return {"label": "FORMAT-BAD", "caller_name": "", "line_type": ""}
    if not TWILIO_SID or not TWILIO_TOKEN:
        return {"label": "UNKNOWN", "caller_name": "", "line_type": ""}
    try:
        digits = re.sub(r'\D', '', phone)
        if len(digits) == 10:
            digits = "1" + digits
        url = f"https://lookups.twilio.com/v2/PhoneNumbers/+{digits}"
        r = requests.get(
            url,
            params={"Fields": "line_type_intelligence,caller_name"},
            auth=(TWILIO_SID, TWILIO_TOKEN),
            timeout=8
        )
        if r.status_code == 404:
            return {"label": "FORMAT-BAD", "caller_name": "", "line_type": ""}
        if r.status_code != 200:
            return {"label": "UNKNOWN", "caller_name": "", "line_type": ""}
        data = r.json()
        line_info = data.get("line_type_intelligence") or {}
        caller_info = data.get("caller_name") or {}
        line_type = line_info.get("type", "").lower()
        caller_name = (caller_info.get("caller_name") or "").strip()
        valid = line_info.get("error_code") is None
        if not valid:
            return {"label": "FORMAT-BAD", "caller_name": "", "line_type": line_type}
        if line_type == "voip":
            return {"label": "VOIP", "caller_name": caller_name, "line_type": line_type}
        if caller_name:
            return {"label": "VERIFIED", "caller_name": caller_name, "line_type": line_type}
        return {"label": "VERIFIED-NO-NAME", "caller_name": "", "line_type": line_type}
    except Exception:
        return {"label": "UNKNOWN", "caller_name": "", "line_type": ""}

def check_name_phone_match(csv_name, twilio_name):
    """Compare CSV name to Twilio caller name. Returns MATCH / MISMATCH / NO-DATA / UNKNOWN"""
    if not twilio_name:
        return "NO-DATA"
    if not csv_name:
        return "UNKNOWN"
    # Simple: check if any word from csv name appears in twilio name
    csv_words = set(csv_name.lower().split())
    twilio_words = set(twilio_name.lower().split())
    if csv_words & twilio_words:
        return "MATCH"
    return "MISMATCH"

def rank_phones(phone_list, csv_name):
    """
    Score each phone and return sorted list of dicts with label.
    Higher score = better.
    """
    results = []
    for phone in phone_list:
        cleaned = clean_phone(phone)
        if not cleaned:
            results.append({
                "original": phone, "cleaned": str(phone),
                "label": "FORMAT-BAD", "caller_name": "", "line_type": "",
                "name_match": "UNKNOWN", "score": 0
            })
            continue
        twilio = verify_phone_twilio(cleaned)
        label = twilio["label"]
        caller_name = twilio["caller_name"]
        line_type = twilio["line_type"]
        name_match = check_name_phone_match(csv_name, caller_name)
        if name_match == "MISMATCH":
            label = "MISMATCH"

        score = {
            "VERIFIED": 5,
            "VERIFIED-NO-NAME": 4,
            "SECONDARY": 3,
            "VOIP": 2,
            "UNKNOWN": 1,
            "MISMATCH": 1,
            "FORMAT-BAD": 0,
        }.get(label, 1)

        results.append({
            "original": phone, "cleaned": cleaned,
            "label": label, "caller_name": caller_name,
            "line_type": line_type, "name_match": name_match, "score": score
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results

def rank_emails(email_list):
    """Score each email, return sorted list."""
    results = []
    for email in email_list:
        cleaned = clean_email(email)
        if not cleaned:
            results.append({"original": email, "cleaned": str(email), "label": "FORMAT-BAD", "score": 0})
            continue
        label = verify_email_millionverifier(cleaned)
        score = {
            "VERIFIED": 5,
            "UNVERIFIED": 3,
            "UNKNOWN": 2,
            "INVALID": 1,
            "DISPOSABLE": 1,
            "SPAMTRAP": 0,
            "FORMAT-BAD": 0,
        }.get(label, 1)
        results.append({"original": email, "cleaned": cleaned, "label": label, "score": score})
    results.sort(key=lambda x: x["score"], reverse=True)
    return results

def verify_addresses(address_list, google_address):
    """Label each address as GOOGLE-VERIFIED or UNVERIFIED."""
    results = []
    google_norm = google_address.lower().strip() if google_address else ""
    for addr in address_list:
        if not addr or pd.isna(addr) or not str(addr).strip():
            continue
        addr_str = str(addr).strip()
        addr_norm = addr_str.lower()
        # Simple match: check if key parts overlap
        if google_norm and any(word in google_norm for word in addr_norm.split()[:3] if len(word) > 3):
            results.append({"address": addr_str, "label": "GOOGLE-VERIFIED"})
        elif google_norm:
            results.append({"address": addr_str, "label": "UNVERIFIED"})
        else:
            results.append({"address": addr_str, "label": "UNKNOWN"})
    return results

# ============================================================
# SORT INTO 4 LISTS
# ============================================================
def get_list(row):
    # DNC check
    dnc = str(row.get("_dnc_flag", "")).upper()
    if dnc in ["Y", "YES", "1", "TRUE", "DO NOT CALL"]:
        return "DNC"

    # Funded check
    status = str(row.get("_status", "")).lower()
    if any(x in status for x in ["funded", "closed won", "won"]):
        return "Funded"

    phone1_label = str(row.get("SeaCap_Phone_1_Status", "")).upper()
    email1_label = str(row.get("SeaCap_Email_1_Status", "")).upper()
    sos_v        = str(row.get("SeaCap_SOS_Check", "")).lower()
    phone1       = str(row.get("SeaCap_Phone_1", "")).strip()
    email1       = str(row.get("SeaCap_Email_1", "")).strip()

    # SOS confirmed inactive → Needs Fixing
    if sos_v == "inactive":
        return "Needs Fixing"

    # Name/phone mismatch → Needs Fixing
    if phone1_label == "MISMATCH":
        return "Needs Fixing"

    # Spamtrap email AND no phone → Needs Fixing
    if email1_label == "SPAMTRAP" and not phone1:
        return "Needs Fixing"

    # Has a usable phone → Qualified
    if phone1 and phone1_label not in ["FORMAT-BAD"]:
        return "Qualified"

    # No phone but has email → Qualified
    if email1 and email1_label not in ["FORMAT-BAD", "INVALID", "DISPOSABLE", "SPAMTRAP"]:
        return "Qualified"

    return "Needs Fixing"

# ============================================================
# MAIN
# ============================================================
def main():
    print(f"\n📂 Loading: {INPUT_FILE}")

    def looks_like_data_row(row):
        """True if a row looks like actual data, not column headers."""
        vals = [str(v).strip() for v in row if str(v).strip() not in ("", "nan")]
        if not vals:
            return False
        # Looks like data if: contains email, phone digits, or pure numbers
        has_email  = any("@" in v for v in vals)
        has_phone  = any(re.sub(r'\D','',v).__len__() >= 9 for v in vals)
        has_number = any(re.match(r'^\d[\d,\.]+$', v) for v in vals)
        return has_email or has_phone or has_number

    def load_file(fp):
        """Load CSV or Excel — handles bad encodings, missing headers, junk top rows."""
        fp = str(fp)
        if fp.endswith((".xlsx", ".xls")):
            df = pd.read_excel(fp, header=0)
            cols = [str(c).strip() for c in df.columns]
            # If header row looks like actual data (has email/phone), file has no headers
            if looks_like_data_row(cols):
                df = pd.read_excel(fp, header=None)
                df.columns = [f"Col_{i}" for i in range(len(df.columns))]
            # If all columns are integers, scan for real header row
            elif all(c.lstrip('-').isdigit() for c in cols):
                df = pd.read_excel(fp, header=None)
                for idx in range(min(10, len(df))):
                    row = df.iloc[idx]
                    str_count = sum(1 for v in row if isinstance(v, str) and len(str(v).strip()) > 1)
                    if str_count >= len(df.columns) * 0.5 and not looks_like_data_row(row):
                        df.columns = [str(v).strip() for v in df.iloc[idx]]
                        df = df.iloc[idx+1:].reset_index(drop=True)
                        break
                else:
                    df.columns = [f"Col_{i}" for i in range(len(df.columns))]
            return df
        elif fp.endswith(".csv"):
            for enc in ["utf-8", "latin1", "cp1252"]:
                try:
                    df = pd.read_csv(fp, low_memory=False, encoding=enc)
                    # If all columns are integers, try skipping junk top rows
                    if all(str(c).strip().lstrip('-').isdigit() for c in df.columns):
                        for skip in range(1, 10):
                            df2 = pd.read_csv(fp, low_memory=False, encoding=enc, skiprows=skip)
                            if not all(str(c).strip().lstrip('-').isdigit() for c in df2.columns):
                                df = df2
                                break
                    return df
                except UnicodeDecodeError:
                    continue
            raise ValueError("Could not decode CSV with any known encoding")
        else:
            print("❌ Unsupported file type.")
            sys.exit(1)

    try:
        df = load_file(INPUT_FILE)
    except FileNotFoundError:
        print(f"❌ Not found: {INPUT_FILE}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Failed to load file: {e}")
        sys.exit(1)

    df.columns = [str(c).strip() for c in df.columns]
    # Drop fully empty columns and rows
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0).reset_index(drop=True)

    if TEST_MODE:
        df = df.head(TEST_MODE)
        print(f"✅ Loaded {len(df):,} records (TEST MODE — {TEST_MODE} rows only)")
    else:
        print(f"✅ Loaded {len(df):,} records (full file)")

    # ── DETECT COLUMNS ──────────────────────────────────────────

    def is_phone_val(val):
        digits = re.sub(r'\D', '', str(val).split(".")[0].strip())
        return len(digits) in [10, 11]

    def is_email_val(val):
        return "@" in str(val) and "." in str(val).split("@")[-1]

    def is_address_val(val):
        v = str(val).strip()
        return bool(re.match(r'^\d+\s+\w+', v)) or any(
            w in v.lower() for w in ["street", "st ", "ave", "blvd", "rd ", "drive", "lane", "court"]
        )

    # Single-column fields
    col = {
        "company": find_col(df, COMPANY_COLS),
        "first":   find_col(df, FIRST_COLS),
        "last":    find_col(df, LAST_COLS),
        "status":  find_col(df, STATUS_COLS),
        "source":  find_col(df, SOURCE_COLS),
    }
    used = set(v for v in col.values() if v)
    for field in ["first", "last", "company"]:
        col[field] = detect_by_pattern(df, field, col[field], used)
        if col[field]:
            used.add(col[field])

    # Multi-column fields
    phone_cols   = find_all_cols(df, PHONE_COLS, is_phone_val)
    email_cols   = find_all_cols(df, EMAIL_COLS, is_email_val)
    address_cols = find_all_cols(df, ADDRESS_COLS, is_address_val)

    # Auto-detect Annual Revenue column — large numeric values not already used
    if not find_col(df, ["Annual Revenue", "AnnualRevenue", "Revenue", "Annual Rev", "Ann Rev"]):
        already_used = set(phone_cols + email_cols + address_cols + list(col.values()))
        for c in df.columns:
            if c in already_used:
                continue
            sample = df[c].dropna()
            if len(sample) == 0:
                continue
            try:
                nums = pd.to_numeric(sample.astype(str).str.replace(',',''), errors='coerce').dropna()
                if len(nums) >= len(sample) * 0.8 and nums.median() >= 10000:
                    df = df.rename(columns={c: "Annual Revenue"})
                    break
            except Exception:
                continue

    # Auto-detect Company column — runs always, overrides if better match found
    already_used = set(phone_cols + email_cols + address_cols)
    already_used.update(v for v in [col.get("first"), col.get("last"), col.get("status"), col.get("source")] if v)
    best_company_col = col.get("company")
    best_score = 0
    for c in df.columns:
        if c in already_used or c == "Annual Revenue":
            continue
        sample = df[c].dropna().astype(str)
        if len(sample) < 3:
            continue
        # Score: how many values look like business names
        biz_hits  = sample.str.contains(r'\b(llc|inc|corp|ltd|co\.|company|enterprises|group|services|solutions|consulting|trucking|construction|plumbing|electric|mechanical|dental|medical|clinic|salon|repair|painting|roofing|cleaning|logistics|staffing|restaurant|hotel|motel|bakery|florist|landscaping|insurance|realty|agency|studio|academy|institute|foundation|association)\b', case=False, regex=True).sum()
        # Penalise if it looks like a name column (short words, no business keywords)
        score = biz_hits / max(len(sample), 1)
        if score > best_score and score >= 0.2:
            best_score = score
            best_company_col = c
    if best_company_col:
        col["company"] = best_company_col
                break

    print(f"\n🔎 Columns detected:")
    for k, v in col.items():
        print(f"   {k:<10} → {v or 'NOT FOUND'}")
    print(f"   phones     → {phone_cols or 'NOT FOUND'}")
    print(f"   emails     → {email_cols or 'NOT FOUND'}")
    print(f"   addresses  → {address_cols or 'NOT FOUND'}")

    # ── CLEAN SINGLE FIELDS ──────────────────────────────────────
    print("\n🔧 Cleaning...")
    df["_company_clean"] = df[col["company"]].apply(lambda x: str(x).strip().title() if pd.notna(x) else "") if col["company"] else ""
    df["_status"]        = df[col["status"]].fillna("") if col["status"] else ""
    df["_dnc_flag"]      = ""

    # Handle first/last — if only combined "Name" column exists, split it
    if col["first"] and col["last"]:
        df["_first_clean"] = df[col["first"]].apply(clean_name)
        df["_last_clean"]  = df[col["last"]].apply(clean_name)
    elif col["first"]:
        # Might be a full name column — try to split
        splits = df[col["first"]].apply(lambda x: split_full_name(x))
        df["_first_clean"] = splits.apply(lambda x: x[0])
        df["_last_clean"]  = splits.apply(lambda x: x[1])
    else:
        df["_first_clean"] = ""
        df["_last_clean"]  = ""

    df["_fullname"] = (df["_first_clean"] + " " + df["_last_clean"]).str.strip().str.lower()

    # Clean date columns
    date_cols = [c for c in df.columns if "date" in c.lower()]
    for dc in date_cols:
        df[dc] = df[dc].apply(clean_date)

    # Clean money columns
    MONEY_COLS = ["Monthly Revenue", "Annual Revenue", "Annual rev", "Requested Amount",
                  "Approved Amount", "requested_funding_amount", "Revenue", "Funding Amount"]
    for mc in MONEY_COLS:
        if mc in df.columns:
            df[mc] = df[mc].apply(clean_money)

    # DNC flags
    for dnc_col in ["Closed by DNC", "SMS Opted Out", "DNC", "Do Not Call"]:
        if dnc_col in df.columns:
            df.loc[df[dnc_col].astype(str).str.upper().isin(["Y","YES","1","TRUE"]), "_dnc_flag"] = "YES"

    # ── DUPLICATE FLAGGING ───────────────────────────────────────
    print("🔍 Checking for potential duplicates...")
    df["Duplicate_Flag"] = ""

    # Use first phone col for dup check
    if phone_cols:
        df["_phone_primary"] = df[phone_cols[0]].apply(clean_phone)
        phone_dup = df["_phone_primary"].ne("") & df.duplicated(subset=["_phone_primary"], keep=False)
        same_name = phone_dup & df.duplicated(subset=["_phone_primary", "_fullname"], keep=False) & df["_fullname"].ne("")
        diff_name = phone_dup & ~same_name
        df.loc[same_name, "Duplicate_Flag"] = "Possible duplicate — same person"
        df.loc[diff_name, "Duplicate_Flag"] = "Same number, different name — check manually"
    else:
        df["_phone_primary"] = ""

    if email_cols:
        df["_email_primary"] = df[email_cols[0]].apply(clean_email)
        email_dup = df["_email_primary"].ne("") & df.duplicated(subset=["_email_primary"], keep=False)
        df.loc[email_dup & (df["Duplicate_Flag"] == ""), "Duplicate_Flag"] = "Same email — check manually"
    else:
        df["_email_primary"] = ""

    if col["first"] and col["last"]:
        name_dup = df["_fullname"].ne("") & df.duplicated(subset=["_fullname"], keep=False)
        df.loc[name_dup & (df["Duplicate_Flag"] == ""), "Duplicate_Flag"] = "Same name — check manually"

    flagged = df["Duplicate_Flag"].ne("")
    print(f"   Total flagged: {flagged.sum()}")

    # ── VERIFY ALL PHONES / EMAILS / ADDRESSES + BUILD SeaCap COLS ──
    print("\n🔬 Verifying leads (this may take a while)...")

    seacap_rows = []

    for i, row in df.iterrows():
        csv_name = str(row.get("_fullname", "")).strip()
        company  = str(row.get("_company_clean", "")).strip()

        # Collect raw phone values from all phone columns
        raw_phones = []
        for pc in phone_cols:
            v = row.get(pc, "")
            if v and not pd.isna(v) and str(v).strip():
                raw_phones.append(str(v).strip())

        # Collect raw email values from all email columns
        raw_emails = []
        for ec in email_cols:
            v = row.get(ec, "")
            if v and not pd.isna(v) and str(v).strip():
                raw_emails.append(str(v).strip())

        # Collect raw addresses
        raw_addresses = []
        for ac in address_cols:
            v = row.get(ac, "")
            if v and not pd.isna(v) and str(v).strip():
                raw_addresses.append(str(v).strip())

        # Business verification (Google + SOS)
        biz_result = verify_business_google(company)
        sos_result = verify_sos_cobalt(company)
        biz_garbage = is_garbage_business(company)
        google_address = biz_result.get("found_address", "")
        google_phone = biz_result.get("found_phone", "")

        # Add Google's phone as a candidate if not already present
        if google_phone and google_phone not in raw_phones:
            raw_phones.append(google_phone)

        # Rank phones and emails
        ranked_phones = rank_phones(raw_phones, csv_name) if raw_phones else []
        ranked_emails = rank_emails(raw_emails) if raw_emails else []
        verified_addresses = verify_addresses(raw_addresses, google_address)

        # Build SeaCap row dict
        sc = {}

        # Phones — up to 5
        for idx in range(5):
            n = idx + 1
            if idx < len(ranked_phones):
                p = ranked_phones[idx]
                sc[f"SeaCap_Phone_{n}"]        = p["cleaned"]
                sc[f"SeaCap_Phone_{n}_Status"]  = p["label"]
                sc[f"SeaCap_Phone_{n}_Source"]  = phone_cols[idx] if idx < len(phone_cols) else "Google"
            else:
                sc[f"SeaCap_Phone_{n}"]        = ""
                sc[f"SeaCap_Phone_{n}_Status"]  = ""
                sc[f"SeaCap_Phone_{n}_Source"]  = ""

        # Emails — up to 5
        for idx in range(5):
            n = idx + 1
            if idx < len(ranked_emails):
                e = ranked_emails[idx]
                sc[f"SeaCap_Email_{n}"]       = e["cleaned"]
                sc[f"SeaCap_Email_{n}_Status"] = e["label"]
            else:
                sc[f"SeaCap_Email_{n}"]       = ""
                sc[f"SeaCap_Email_{n}_Status"] = ""

        # Addresses — up to 4
        for idx in range(4):
            n = idx + 1
            if idx < len(verified_addresses):
                a = verified_addresses[idx]
                sc[f"SeaCap_Address_{n}"]        = a["address"]
                sc[f"SeaCap_Address_{n}_Status"]  = a["label"]
            else:
                sc[f"SeaCap_Address_{n}"]        = ""
                sc[f"SeaCap_Address_{n}_Status"]  = ""

        # Business checks
        sc["SeaCap_Business_Check"] = biz_result.get("status", "UNKNOWN")
        sc["SeaCap_SOS_Check"]      = sos_result
        sc["SeaCap_Business_Garbage"] = biz_garbage

        # Name/phone match
        best_phone_name_match = ranked_phones[0]["name_match"] if ranked_phones else "UNKNOWN"
        sc["SeaCap_Name_Match"] = best_phone_name_match

        # Build plain-English flag reason — only real problems, not NOT-FOUND
        reasons = []
        if ranked_phones and ranked_phones[0]["label"] == "MISMATCH":
            reasons.append("Phone belongs to different person")
        if ranked_phones and ranked_phones[0]["label"] == "VOIP":
            reasons.append("Phone is VoIP")
        if ranked_emails and ranked_emails[0]["label"] == "SPAMTRAP":
            reasons.append("Email is spamtrap")
        if ranked_emails and ranked_emails[0]["label"] == "DISPOSABLE":
            reasons.append("Email is disposable")
        if biz_result.get("status") in ("VERIFIED-INACTIVE", "PERMANENTLY_CLOSED"):
            reasons.append("Business closed/dissolved")
        if biz_garbage:
            reasons.append(biz_garbage)
        sc["SeaCap_Flag_Reason"] = " | ".join(reasons)  # blank = clean

        seacap_rows.append(sc)

        # Per-row terminal output
        row_num   = seacap_rows.__len__()
        phone_out = f"{sc.get('SeaCap_Phone_1','')} [{sc.get('SeaCap_Phone_1_Status','—')}]" if sc.get('SeaCap_Phone_1') else "❌ no phone"
        email_out = f"{sc.get('SeaCap_Email_1','')} [{sc.get('SeaCap_Email_1_Status','—')}]" if sc.get('SeaCap_Email_1') else "❌ no email"
        biz_out   = sc.get('SeaCap_Business_Check', 'UNKNOWN')
        sos_out   = sc.get('SeaCap_SOS_Check', 'UNKNOWN')
        flag_out  = sc.get('SeaCap_Flag_Reason', '')
        print(f"\n[{row_num}] {csv_name or company or 'Unknown'}")
        print(f"   📞 {phone_out}")
        print(f"   📧 {email_out}")
        # Only print business checks if they found something useful
        if biz_out not in ("NOT-FOUND", "UNKNOWN", ""):
            print(f"   🏢 Google: {biz_out}")
        if sos_out not in ("UNKNOWN", "NOT-FOUND", ""):
            print(f"   🏛  SOS: {sos_out}")
        if flag_out:
            print(f"   ⚠️  {flag_out}")

    # Merge SeaCap columns into df
    seacap_df = pd.DataFrame(seacap_rows, index=df.index)

    # ── DNC flags from known columns ─────────────────────────────────
    for dnc_col in ["Closed by DNC", "SMS Opted Out", "DNC", "Do Not Call"]:
        if dnc_col in df.columns:
            df.loc[df[dnc_col].astype(str).str.upper().isin(["Y","YES","1","TRUE"]), "_dnc_flag"] = "YES"

    # Merge SeaCap cols + original df (SeaCap first, original after)
    df_out = pd.concat([seacap_df, df], axis=1)

    # Drop internal _ columns from output
    internal_cols = [c for c in df_out.columns if c.startswith("_")]
    df_out = df_out.drop(columns=internal_cols)

    # Sort into 4 lists
    print("\n📋 Sorting into 4 lists...")
    # Re-add _dnc_flag and _status for get_list
    df_out["_dnc_flag"] = df["_dnc_flag"]
    df_out["_status"]   = df["_status"]
    df_out["List"] = df_out.apply(get_list, axis=1)
    df_out["Fix_Reason"] = ""

    def get_fix_reason(row):
        if row["List"] != "Needs Fixing":
            return ""
        reasons = []
        if not str(row.get("SeaCap_Phone_1", "")).strip():
            reasons.append("Missing phone")
        if not str(row.get("SeaCap_Email_1", "")).strip():
            reasons.append("Missing email")
        if str(row.get("SeaCap_Phone_1_Status", "")) == "MISMATCH":
            reasons.append("Phone belongs to different person")
        if str(row.get("SeaCap_SOS_Check", "")).lower() == "inactive":
            reasons.append("SOS inactive")
        if str(row.get("SeaCap_Email_1_Status", "")) in ["INVALID", "SPAMTRAP", "DISPOSABLE"]:
            reasons.append("Bad email")
        return ", ".join(reasons) if reasons else "Incomplete data"

    df_out["Fix_Reason"] = df_out.apply(get_fix_reason, axis=1)

    # Drop the temp internal cols from output
    df_out = df_out.drop(columns=["_dnc_flag", "_status"], errors="ignore")

    # Strip illegal chars
    str_cols = df_out.select_dtypes(include=["object"]).columns
    for c in str_cols:
        df_out[c] = df_out[c].apply(strip_illegal)

    df_qualified = df_out[df_out["List"] == "Qualified"]
    df_fixing    = df_out[df_out["List"] == "Needs Fixing"]
    df_dnc       = df_out[df_out["List"] == "DNC"]
    df_funded    = df_out[df_out["List"] == "Funded"]
    df_flagged   = df_out[df_out["Duplicate_Flag"].ne("")]

    # ── Rename detected original cols to standard names ──────────────────
    rename_map = {}
    if col.get("first")   and col["first"]   != "First Name":  rename_map[col["first"]]   = "First Name"
    if col.get("last")    and col["last"]     != "Last Name":   rename_map[col["last"]]    = "Last Name"
    if col.get("company") and col["company"]  != "Company":     rename_map[col["company"]] = "Company"
    if rename_map:
        df_out = df_out.rename(columns=rename_map)

    # Rename SeaCap columns to human-readable names
    df_out = df_out.rename(columns={
        "SeaCap_Phone_1":         "Best Phone",
        "SeaCap_Phone_1_Status":  "Phone Status",
        "SeaCap_Phone_1_Source":  "Phone Found In",
        "SeaCap_Email_1":         "Best Email",
        "SeaCap_Email_1_Status":  "Email Status",
        "SeaCap_Address_1":       "Best Address",
        "SeaCap_Address_1_Status":"Address Status",
    })

    # Drop original phone/email/address columns — SeaCap verified versions replace them
    orig_phone_cols   = [c for c in phone_cols   if c in df_out.columns]
    orig_email_cols   = [c for c in email_cols   if c in df_out.columns]
    orig_address_cols = [c for c in address_cols if c in df_out.columns]
    df_out = df_out.drop(columns=orig_phone_cols + orig_email_cols + orig_address_cols, errors="ignore")

    # Drop SeaCap columns that are entirely empty
    all_seacap = [c for c in df_out.columns if c.startswith("SeaCap_")]
    drop_empty = [c for c in all_seacap if df_out[c].replace("", pd.NA).isna().all()]
    df_out = df_out.drop(columns=drop_empty)

    # Also drop Phone_Found_In — not useful for reps
    df_out = df_out.drop(columns=["Phone Found In"], errors="ignore")

    # ── Column order ──────────────────────────────────────────────────────
    # Qualified list: Fix_Reason + Duplicate_Flag at back (usually empty)
    # Needs Fixing / DNC: Fix_Reason + Duplicate_Flag at front
    extra_phones = sorted([c for c in df_out.columns if re.match(r"SeaCap_Phone_[2-5]", c)])
    extra_emails = sorted([c for c in df_out.columns if re.match(r"SeaCap_Email_[2-5]", c)])
    extra_addrs  = sorted([c for c in df_out.columns if re.match(r"SeaCap_Address_[2-4]", c)])
    ref_end      = [c for c in ["SeaCap_Business_Check", "SeaCap_SOS_Check",
                                 "SeaCap_Name_Match", "SeaCap_Flag_Reason",
                                 "SeaCap_Business_Garbage"] if c in df_out.columns]
    decision     = [c for c in ["Fix_Reason", "Duplicate_Flag"] if c in df_out.columns]
    core         = [c for c in ["First Name", "Last Name", "Company", "List",
                                 "Best Phone", "Phone Status",
                                 "Best Email", "Email Status",
                                 "Best Address", "Address Status",
                                 "Annual Revenue"] if c in df_out.columns]
    already      = set(core + extra_phones + extra_emails + extra_addrs + ref_end + decision)
    other_orig   = [c for c in df_out.columns if c not in already]

    def make_ordered(front_decision):
        base = core + extra_phones + extra_emails + extra_addrs + other_orig + ref_end
        if front_decision:
            ordered = decision + base
        else:
            ordered = base + decision
        seen = set()
        return [c for c in ordered if c in df_out.columns and not (c in seen or seen.add(c))]

    # Re-slice after reorder — apply per-list ordering
    df_qualified = df_out[df_out["List"] == "Qualified"].copy()
    df_fixing    = df_out[df_out["List"] == "Needs Fixing"].copy()
    df_dnc       = df_out[df_out["List"] == "DNC"].copy()
    df_funded    = df_out[df_out["List"] == "Funded"].copy()
    df_flagged   = df_out[df_out["Duplicate_Flag"].ne("")].copy()

    df_qualified = df_qualified[make_ordered(front_decision=False)]
    df_fixing    = df_fixing[make_ordered(front_decision=True)]
    df_dnc       = df_dnc[make_ordered(front_decision=True)]
    df_funded    = df_funded[make_ordered(front_decision=False)]
    df_flagged   = df_flagged[make_ordered(front_decision=True)]
    df_out       = df_out[make_ordered(front_decision=False)]

    def save_xlsx(df, path):
        """Save with auto-fit column widths and frozen header row."""
        from openpyxl.utils import get_column_letter
        from openpyxl.styles import Font, PatternFill, Alignment
        with pd.ExcelWriter(str(path), engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Leads")
            ws = writer.sheets["Leads"]
            # Freeze header
            ws.freeze_panes = "A2"
            # Bold + grey header
            header_fill = PatternFill("solid", fgColor="D9D9D9")
            for cell in ws[1]:
                cell.font      = Font(bold=True)
                cell.fill      = header_fill
                cell.alignment = Alignment(horizontal="center")
            # Auto-fit widths (cap at 50)
            for col_idx, col_cells in enumerate(ws.columns, 1):
                max_len = max((len(str(c.value or "")) for c in col_cells), default=10)
                ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 2, 50)

    # Save — skip empty lists
    print("💾 Saving...")
    save_xlsx(df_out, OUTPUT_DIR / f"{STEM}_all.xlsx")
    if len(df_qualified): save_xlsx(df_qualified, OUTPUT_DIR / f"{STEM}_list1_qualified.xlsx")
    if len(df_fixing):    save_xlsx(df_fixing,    OUTPUT_DIR / f"{STEM}_list2_needs_fixing.xlsx")
    if len(df_dnc):       save_xlsx(df_dnc,       OUTPUT_DIR / f"{STEM}_list3_dnc.xlsx")
    if len(df_funded):    save_xlsx(df_funded,    OUTPUT_DIR / f"{STEM}_list4_funded.xlsx")
    if len(df_flagged):   save_xlsx(df_flagged,   OUTPUT_DIR / f"{STEM}_flagged_for_review.xlsx")

    print("\n" + "="*55)
    print("✅  SUMMARY")
    print("="*55)
    print(f"📊 Total              : {len(df_out):,}")
    print(f"✅ List 1 Qualified   : {len(df_qualified):,}")
    print(f"🔧 List 2 Needs Fix   : {len(df_fixing):,}")
    print(f"🚫 List 3 DNC         : {len(df_dnc):,}")
    print(f"💰 List 4 Funded      : {len(df_funded):,}")
    print(f"⚠️  Flagged for review : {len(df_flagged):,}  ← rep decides")
    print(f"\n📁 Output: {OUTPUT_DIR}")
    print("="*55)

    import json
    summary = {
        "total_leads": len(df_out),
        "list1_qualified": len(df_qualified),
        "list2_needs_fixing": len(df_fixing),
        "list3_dnc": len(df_dnc),
        "list4_funded": len(df_funded),
        "flagged_duplicates": len(df_flagged)
    }
    print(f"SUMMARY_JSON:{json.dumps(summary)}")

if __name__ == "__main__":
    main()
