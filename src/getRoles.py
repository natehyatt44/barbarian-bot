import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

CFP_TOKEN_ID = '0.0.2235264'

def fetch_from_mirror_node(accountId, nextUrl=None):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = nextUrl or f'/api/v1/accounts/{accountId}/nfts?limit=100'
    response = requests.get(f'{url}{path}')
    accountNfts = response.json()

    nfts = accountNfts.get('nfts', [])

    # Check for a next URL in the API's paginated response
    if 'links' in accountNfts and 'next' in accountNfts['links'] and accountNfts['links']['next']:
        nfts += fetch_from_mirror_node(accountId, accountNfts['links']['next'])

    return nfts

def match_nfts_to_discord_helper(nfts):
    matched_records = []

    with open('dataAnalytics/discordRoleHelper.json', 'r') as f:
        discord_helper = json.load(f)

    for item in nfts:
        if item['token_id'] == CFP_TOKEN_ID:
            serial_number = item['serial_number']

            # Matching logic
            for helper_item in discord_helper:
                if helper_item['serialNumber'] == serial_number:
                    matched_record = {
                        'serial_number': serial_number,
                        'isZombieSpirit': helper_item['isZombieSpirit']
                    }
                    if not any(record['serial_number'] == matched_record['serial_number'] for record in matched_records):
                        matched_records.append(matched_record)

    return matched_records


def determine_roles(matched_records):
    count = len(matched_records)
    roles = []

    # Check for Zombie/Spirit first
    if any(record.get('isZombieSpirit') == 1 for record in matched_records):
        roles.append('Zombie/Spirit')

    # Check based on count
    if count >= 30:
        roles.append('Hbarbarian GOD')
    if count >= 20:
        roles.append('Hbarbarian Chieftain')
    if count >= 10:
        roles.append('Hbarbarian Berserker')
    if count >= 1:
        roles.append('Hbarbarian')

    return roles



