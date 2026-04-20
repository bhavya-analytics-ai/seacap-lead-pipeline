import os
import csv
from pathlib import Path
from dotenv import load_dotenv
from closeio_api import Client
from datetime import datetime

load_dotenv(Path(__file__).parents[2] / '.env')

api = Client(os.getenv('CLOSE_API_KEY'))

OUTPUT_DIR = Path(__file__).parents[1] / 'data'
OUTPUT_DIR.mkdir(exist_ok=True)


def is_email(text):
    return '@' in text and '.' in text.split('@')[-1]


def find_junk_leads():
    junk = []
    total = 0
    has_more = True
    cursor = None

    print('Scanning for junk leads...', flush=True)

    while has_more:
        params = {
            '_limit': 100,
            '_fields': 'id,display_name,contacts,status_label,date_created',
        }
        if cursor:
            params['_cursor'] = cursor

        resp = api.get('lead/', params=params)
        batch = resp.get('data', [])
        has_more = resp.get('has_more', False)
        cursor = resp.get('cursor_next')

        for lead in batch:
            total += 1
            name = lead.get('display_name', '').strip()
            contacts = lead.get('contacts', [])

            reasons = []

            # Name is just an email address
            if is_email(name):
                reasons.append('Name is email address')

            # Completely blank name
            if not name:
                reasons.append('Blank name')

            # No contacts at all
            if not contacts:
                reasons.append('No contacts')
            else:
                # Has contacts but no phone and no email
                has_phone = any(
                    c.get('phones') for c in contacts
                )
                has_email = any(
                    c.get('emails') for c in contacts
                )
                if not has_phone and not has_email:
                    reasons.append('No phone or email on any contact')

            if reasons:
                junk.append({
                    'id': lead['id'],
                    'name': name,
                    'status': lead.get('status_label', ''),
                    'date_created': lead.get('date_created', '')[:10],
                    'reasons': ', '.join(reasons),
                    'close_url': f"https://app.close.com/lead/{lead['id']}/"
                })

        if total % 10000 == 0:
            print(f'  {total:,} scanned, {len(junk):,} junk found so far...', flush=True)

    print(f'\nDone — {total:,} leads scanned, {len(junk):,} junk found')
    return junk


def write_report(junk):
    filename = OUTPUT_DIR / f"close_junk_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'name', 'status', 'date_created', 'reasons', 'close_url'])
        writer.writeheader()
        writer.writerows(junk)

    print(f'Report saved → {filename}')
    print(f'\nBreakdown:')
    from collections import Counter
    reason_counts = Counter()
    for j in junk:
        for r in j['reasons'].split(', '):
            reason_counts[r] += 1
    for reason, count in reason_counts.most_common():
        print(f'  {count:,} — {reason}')

    print(f'\nReview the CSV, then run close_delete_junk.py to delete approved leads.')
    return filename


if __name__ == '__main__':
    junk = find_junk_leads()
    if junk:
        write_report(junk)
    else:
        print('No junk found!')
