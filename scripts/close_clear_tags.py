import os
from pathlib import Path
from dotenv import load_dotenv
from closeio_api import Client

load_dotenv(Path(__file__).parents[2] / '.env')

api = Client(os.getenv('CLOSE_API_KEY'))

ADAM_LIST_FIELD_ID = 'cf_XU1qHTFDucHvqGhXRwkbMtzj9v0F2nCZGes7N7rqID6'

def clear_tags():
    total = 0
    cleared = 0
    has_more = True
    cursor = None

    print("Clearing Adam List tags...", flush=True)

    while has_more:
        params = {
            '_limit': 100,
            '_fields': f'id,display_name,custom',
            'query': f'custom.{ADAM_LIST_FIELD_ID}:*',
        }
        if cursor:
            params['_cursor'] = cursor

        resp = api.get('lead/', params=params)
        batch = resp.get('data', [])
        has_more = resp.get('has_more', False)
        cursor = resp.get('cursor_next')

        for lead in batch:
            try:
                api.put(f'lead/{lead["id"]}', {'custom': {ADAM_LIST_FIELD_ID: ''}})
                cleared += 1
                if cleared % 100 == 0:
                    print(f'  Cleared {cleared:,} leads...', flush=True)
            except Exception as e:
                print(f'  Error on {lead["display_name"]}: {e}')

        total += len(batch)
        if not batch:
            break

    print(f'\nDone — cleared {cleared:,} leads.')

if __name__ == '__main__':
    clear_tags()
