import os
import csv
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
from closeio_api import Client
from datetime import datetime

load_dotenv(Path(__file__).parents[2] / '.env')

api = Client(os.getenv('CLOSE_API_KEY'))

OUTPUT_DIR = Path(__file__).parents[1] / 'data'
OUTPUT_DIR.mkdir(exist_ok=True)


def fetch_all_leads():
    leads = []
    has_more = True
    cursor = None
    total = 0

    print("Fetching all leads...")

    while has_more:
        params = {
            '_limit': 100,
            '_fields': 'id,display_name,status_label,contacts,date_created',
        }
        if cursor:
            params['_cursor'] = cursor

        resp = api.get('lead/', params=params)
        batch = resp.get('data', [])
        has_more = resp.get('has_more', False)
        cursor = resp.get('cursor_next')

        leads.extend(batch)
        total += len(batch)

        if total % 10000 == 0:
            print(f'  Fetched {total:,} leads...')

    print(f'Done — {total:,} leads fetched')
    return leads


def find_duplicates(leads):
    phone_map = defaultdict(list)
    email_map = defaultdict(list)

    for lead in leads:
        contacts = lead.get('contacts', [])
        for contact in contacts:
            for phone in contact.get('phones', []):
                num = phone.get('phone_formatted') or phone.get('phone', '')
                num = ''.join(filter(str.isdigit, num))[-10:]  # normalize to last 10 digits
                if num and len(num) >= 7:
                    phone_map[num].append(lead)
            for email in contact.get('emails', []):
                addr = email.get('email', '').strip().lower()
                if addr and '@' in addr:
                    email_map[addr].append(lead)

    # Find groups with duplicates
    dupe_groups = []
    seen_lead_ids = set()

    for phone, group in phone_map.items():
        if len(group) > 1:
            ids = tuple(sorted(l['id'] for l in group))
            if ids not in seen_lead_ids:
                seen_lead_ids.add(ids)
                dupe_groups.append({
                    'match_type': 'phone',
                    'match_value': phone,
                    'leads': group
                })

    for email, group in email_map.items():
        if len(group) > 1:
            ids = tuple(sorted(l['id'] for l in group))
            if ids not in seen_lead_ids:
                seen_lead_ids.add(ids)
                dupe_groups.append({
                    'match_type': 'email',
                    'match_value': email,
                    'leads': group
                })

    return dupe_groups


def write_report(dupe_groups):
    filename = OUTPUT_DIR / f"close_duplicates_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Group', 'Match Type', 'Match Value', 'Lead ID', 'Lead Name', 'Status', 'Date Created', 'Close URL'])

        for i, group in enumerate(dupe_groups, 1):
            for lead in group['leads']:
                writer.writerow([
                    i,
                    group['match_type'],
                    group['match_value'],
                    lead['id'],
                    lead['display_name'],
                    lead.get('status_label', ''),
                    lead.get('date_created', '')[:10],
                    f"https://app.close.com/lead/{lead['id']}/"
                ])

    print(f'\nDuplicate report saved → {filename}')
    print(f'Total duplicate groups: {len(dupe_groups):,}')
    total_dupes = sum(len(g["leads"]) for g in dupe_groups)
    print(f'Total affected leads: {total_dupes:,}')
    return filename


if __name__ == '__main__':
    leads = fetch_all_leads()
    print('\nFinding duplicates...')
    dupe_groups = find_duplicates(leads)
    write_report(dupe_groups)
    print('\nSend this CSV to Lucas — he merges in Close UI.')
