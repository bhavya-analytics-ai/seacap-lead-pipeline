import os
from pathlib import Path
from dotenv import load_dotenv
from closeio_api import Client

load_dotenv(Path(__file__).parents[2] / '.env')

api = Client(os.getenv('CLOSE_API_KEY'))

# Check if "Adam List" already exists
existing = api.get('custom_field/lead/')
for field in existing['data']:
    if field['name'] == 'Adam List':
        print(f"'Adam List' already exists — id: {field['id']}")
        exit()

# Create it
field = api.post('custom_field/lead/', {
    'name': 'Adam List',
    'type': 'choices',
    'choices': ['List 1', 'List 2', 'List 3', 'List 4'],
    'accepts_multiple_values': False,
    'is_shared': True,
})

print(f"Created 'Adam List' custom field")
print(f"ID: {field['id']}")
print(f"Choices: {field['choices']}")
