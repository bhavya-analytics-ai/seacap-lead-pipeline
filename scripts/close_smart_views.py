import os
from pathlib import Path
from dotenv import load_dotenv
from closeio_api import Client

load_dotenv(Path(__file__).parents[2] / '.env')

api = Client(os.getenv('CLOSE_API_KEY'))

# Adam List custom field ID
ADAM_LIST_FIELD_ID = 'cf_XU1qHTFDucHvqGhXRwkbMtzj9v0F2nCZGes7N7rqID6'

# The 4 Adam smart views to create
ADAM_VIEWS = [
    {
        'name': '✅ Adam — List 1 (Qualified)',
        'query': f'custom.{ADAM_LIST_FIELD_ID}:"List 1"',
        'selected_fields': ['display_name', 'contacts', 'status_label', f'custom.{ADAM_LIST_FIELD_ID}', 'date_updated'],
    },
    {
        'name': '🔧 Adam — List 2 (Needs Fixing)',
        'query': f'custom.{ADAM_LIST_FIELD_ID}:"List 2"',
        'selected_fields': ['display_name', 'contacts', 'status_label', f'custom.{ADAM_LIST_FIELD_ID}', 'date_updated'],
    },
    {
        'name': '🚫 Adam — List 3 (DNC)',
        'query': f'custom.{ADAM_LIST_FIELD_ID}:"List 3"',
        'selected_fields': ['display_name', 'contacts', 'status_label', f'custom.{ADAM_LIST_FIELD_ID}', 'date_updated'],
    },
    {
        'name': '💰 Adam — List 4 (Funded)',
        'query': f'custom.{ADAM_LIST_FIELD_ID}:"List 4"',
        'selected_fields': ['display_name', 'contacts', 'status_label', f'custom.{ADAM_LIST_FIELD_ID}', 'date_updated'],
    },
]


def create_adam_views():
    # Check which ones already exist
    existing = api.get('saved_search/', params={'_limit': 200})
    existing_names = {v['name'] for v in existing.get('data', [])}

    for view in ADAM_VIEWS:
        if view['name'] in existing_names:
            print(f"Already exists — skipping: {view['name']}")
            continue

        result = api.post('saved_search/', {
            'name': view['name'],
            'query': view['query'],
            'type': 'lead',
            'is_shared': True,
        })
        print(f"Created: {view['name']} (id: {result['id']})")

    print('\nDone — 4 Adam smart views created.')
    print('They appear in the Smart Views sidebar in Close.')


if __name__ == '__main__':
    create_adam_views()
