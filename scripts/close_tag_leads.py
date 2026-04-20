import os
from pathlib import Path
from dotenv import load_dotenv
from closeio_api import Client

load_dotenv(Path(__file__).parents[2] / '.env')

api = Client(os.getenv('CLOSE_API_KEY'))

ADAM_LIST_FIELD_ID = 'cf_XU1qHTFDucHvqGhXRwkbMtzj9v0F2nCZGes7N7rqID6'

# Status → List mapping
LIST_1 = {
    'stat_KAxW4CxmwBohKJChIIkIlLK9jKxLiTBDkERQo3Ra5jh',  # New / Uncontacted
    'stat_68MukSe1wWdElVlhJsstur8Z0NIUD0J5BX8KJUFK8ZW',  # Contact Attempted
    'stat_TzdUVSMds0INCStIoW1hIWrrzaXTc019zViCVNdiOS1',  # Interested - Follow Up
    'stat_reoLms8grYMpklvOk0MTFfnBcMskU5iJFYMiZL1W2Zo',  # App Sent
    'stat_vlZXBI3OKtvFSYKXj3Re0s9GP1A2TZyGKDt1h6oieIL',  # App + Statements Received
    'stat_MnovAYThbB0ZTX15VuuLgeera53JKTm3Gp6N7FJ4Bx9',  # Follow Up - Hot
    'stat_V4TUsyBlszdzJNjK4ONxiGiSkV5wFF1s820JySFhppe',  # Follow Up - Warm
    'stat_h0AZqke49fu1pGEtEz0LAYPOCLX5Lxn71d9UJuBgqkR',  # Follow Up - Cool
}

LIST_2 = {
    'stat_NufvGPpLbssvcAdetOTes5mqQuvo9AVRoovs4OQJNJp',  # Not Ready / Follow-Up Needed
    'stat_9fDhBB6VEvWXtHGZ5FP3WxBKpAWS3Sm5es5Ggpaw8Pj',  # Invalid Contact Info
    'stat_Kz7ghReLXddpF5zqq4UyskisTEFVQeB5VAoYzy153KX',  # Overleveraged
    'stat_ufcLGCoy41qlSGM4p6jMe45RYW6GDPOUj8WuZMrz78I',  # Submitted - No offer made
}

LIST_3 = {
    'stat_voUtLGcfL5bTcw00K6Hxwe6efhcP0M8s7a8dH3dXCij',  # Not Interested - DNC
    'stat_RyKOH6xaZMr4TZdGHN0DiNJ8Q1d7Tm7UqOxj4N5avY4',  # Disqualified - Not Eligible
    'stat_cyX4V24xgc1PEtpNIGX6SxJbNtkPjGDplroHEgRgDia',  # Default
    'stat_DXwQSS8y4jCIhPr7VU966YCG2rRwzGkoAHXDqeTK090',  # Declined (Funder)
}

LIST_4 = {
    'stat_Tzu12vilJKdz1hrqghOOWVih4GpmNyJrqur5J439GUj',  # Funded - SEACAP
    'stat_IRfVjaC1k3FDaIAg4T90fVUEgnHYY8PFAyP1fUh8b8v',  # Funded Elsewhere
    'stat_dWxAWLnOb5ZW2rThZXhkI0Ybzr04kBPX4aRwa2S5ZDo',  # Approved Not Funded
    'stat_ehkT5IhIpnearpbFmbmXn4E8de6EM0yhxJcQ2j3yAkc',  # Approved Not Funded (Automation)
}

JUNK = {
    'stat_AO8oz1tlLP4ktp8on9U9K6rC7tBdcanVzeBHIbs2jPr',  # L
}


def get_list_for_status(status_id):
    if status_id in LIST_1:
        return 'List 1'
    elif status_id in LIST_2:
        return 'List 2'
    elif status_id in LIST_3:
        return 'List 3'
    elif status_id in LIST_4:
        return 'List 4'
    elif status_id in JUNK:
        return None
    else:
        return 'List 2'  # unknown → needs fixing


def tag_leads():
    total = 0
    written = 0
    skipped = 0
    errors = 0
    counts = {'List 1': 0, 'List 2': 0, 'List 3': 0, 'List 4': 0}

    has_more = True
    cursor = None

    print('Starting tagging...', flush=True)

    while has_more:
        params = {
            '_limit': 100,
            '_fields': f'id,display_name,status_id,custom',
            'query': f'not custom.{ADAM_LIST_FIELD_ID}:"List 1" and not custom.{ADAM_LIST_FIELD_ID}:"List 2" and not custom.{ADAM_LIST_FIELD_ID}:"List 3" and not custom.{ADAM_LIST_FIELD_ID}:"List 4"',
        }
        if cursor:
            params['_cursor'] = cursor

        resp = api.get('lead/', params=params)
        batch = resp.get('data', [])
        has_more = resp.get('has_more', False)
        cursor = resp.get('cursor_next')

        for lead in batch:
            total += 1
            status_id = lead.get('status_id', '')
            current_list = lead.get('custom', {}).get('Adam List')
            assigned_list = get_list_for_status(status_id)

            # Skip junk statuses
            if assigned_list is None:
                skipped += 1
                continue

            # Skip if already tagged correctly
            if current_list == assigned_list:
                skipped += 1
                continue

            # Write the tag
            try:
                api.put(f'lead/{lead["id"]}', {
                    'custom': {ADAM_LIST_FIELD_ID: assigned_list}
                })
                written += 1
                counts[assigned_list] += 1
            except Exception as e:
                errors += 1
                print(f'  ERROR {lead["display_name"]}: {e}', flush=True)

        print(f'  {total:,} processed | {written:,} written | {skipped:,} skipped | {errors} errors', flush=True)

    print(f'\n✅ Done — {total:,} leads total')
    print(f'  Written to Close:')
    print(f'    List 1 (Qualified):    {counts["List 1"]:,}')
    print(f'    List 2 (Needs Fixing): {counts["List 2"]:,}')
    print(f'    List 3 (DNC):          {counts["List 3"]:,}')
    print(f'    List 4 (Funded):       {counts["List 4"]:,}')
    print(f'  Skipped (already tagged or junk): {skipped:,}')
    print(f'  Errors: {errors}')


if __name__ == '__main__':
    tag_leads()
