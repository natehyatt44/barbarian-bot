import requests
import json
import os
import boto3

CFP_TOKEN_ID = '0.0.2235264'
TLO_TOKEN_ID = '0.0.3721853'

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

    s3 = boto3.client('s3')
    bucket_name = 'lost-ones-upload32737-staging'
    object_key = f'public/discordAccounts/discordRoleHelper.json'

    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    res = response['Body'].read().decode('utf-8')
    discord_helper = json.loads(res)

    for item in nfts:
        if item['token_id'] == CFP_TOKEN_ID or item['token_id'] == TLO_TOKEN_ID:
            serial_number = item['serial_number']
            token_id = item['token_id']

            # Matching logic
            for helper_item in discord_helper:
                if helper_item['serial_number'] == serial_number and helper_item['tokenId'] == token_id:

                    matched_record = {
                        'token_id': helper_item['tokenId'],
                        'serial_number': serial_number,
                        'isZombieSpirit': helper_item.get('isZombieSpirit', 0),
                        'race': helper_item.get('race', 'Mortal')
                    }
                    if not any(record['serial_number'] == matched_record['serial_number'] and record['token_id'] == matched_record['token_id'] for record in matched_records):
                        matched_records.append(matched_record)

    return matched_records


def determine_roles(matched_records):
    roles = []


    # Determine counts for each race
    mortal_count = sum(1 for record in matched_records if record.get('race', 'Mortal') == 'Mortal')
    gaian_count = sum(1 for record in matched_records if record.get('race') == 'Gaian')
    runekin_count = sum(1 for record in matched_records if record.get('race') == 'Runekin')
    soulweaver_count = sum(1 for record in matched_records if record.get('race') == 'Soulweaver')
    zephyr_count = sum(1 for record in matched_records if record.get('race') == 'Zephyr')
    archangel_count = sum(1 for record in matched_records if record.get('race') == 'ArchAngel')

    # Check for Zombie/Spirit first
    if any(record.get('isZombieSpirit') == 1 for record in matched_records):
        roles.append('Zombie/Spirit')

    # Assign roles based on Mortal count
    if mortal_count >= 30:
        roles.append('Hbarbarian GOD')
    if mortal_count >= 20:
        roles.append('Hbarbarian Chieftain')
    if mortal_count >= 10:
        roles.append('Hbarbarian Berserker')
    if mortal_count >= 1:
        roles.append('Hbarbarian')

    # Assign roles for other races
    if gaian_count >= 10:
        roles.append('Gaian Treelord')
    if runekin_count >= 10:
        roles.append('Runekin High Council')
    if soulweaver_count >= 10:
        roles.append('Soulweaver Seer')
    if zephyr_count >= 10:
        roles.append('Zephyr Ace')
    if archangel_count >= 3:
        roles.append('ArchAngel Guardian')

    return roles




