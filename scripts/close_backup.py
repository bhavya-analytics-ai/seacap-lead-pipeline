import os
import json
import time
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / '.env')

API_KEY = os.getenv('CLOSE_API_KEY')
BASE_URL = 'https://api.close.com/api/v1'
BACKUP_DIR = Path(__file__).parents[1] / 'data'
BACKUP_DIR.mkdir(exist_ok=True)


def backup_leads():
    session = requests.Session()
    session.auth = (API_KEY, '')

    all_leads = []
    skip = 0
    limit = 100

    print(f"Starting Close CRM backup — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        resp = session.get(f'{BASE_URL}/lead/', params={
            '_limit': limit,
            '_skip': skip,
            '_fields': 'id,display_name,status_id,status_label,contacts,custom,addresses,description,created_by,date_created,date_updated,tasks,opportunities'
        })

        if resp.status_code == 429:
            print("Rate limited — waiting 2s...")
            time.sleep(2)
            continue

        resp.raise_for_status()
        data = resp.json()
        batch = data.get('data', [])

        if not batch:
            break

        all_leads.extend(batch)
        skip += len(batch)

        print(f"  Pulled {skip:,} leads...", end='\r')

        # Close allows 60 req/sec — small sleep to be safe
        time.sleep(0.05)

    print(f"\nDone — {len(all_leads):,} leads pulled.")

    filename = BACKUP_DIR / f"close_backup_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    with open(filename, 'w') as f:
        json.dump({
            'backed_up_at': datetime.now().isoformat(),
            'total': len(all_leads),
            'leads': all_leads
        }, f, indent=2)

    print(f"Backup saved → {filename}")
    print(f"File size: {filename.stat().st_size / 1024 / 1024:.1f} MB")
    return filename


if __name__ == '__main__':
    backup_leads()
