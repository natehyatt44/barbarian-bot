import requests
import json
import time
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

CFP_TOKEN_ID = os.environ["CFP_TOKEN_ID"]
def fetch_transaction_from_mirror_node(transactionId):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = f'/api/v1/transactions/{transactionId}'

    response = requests.get(f'{url}{path}')
    transaction = response.json()

    # Define the Hedera epoch
    hedera_epoch = datetime.datetime(2010, 1, 1)

    # Check if transactions is in the response
    if 'transactions' in transaction and len(transaction['transactions']) > 0:
        # We are only interested in the first transaction
        transaction = transaction['transactions'][0]

        # Convert the consensus_timestamp to datetime using the Hedera epoch
        consensus_timestamp = hedera_epoch + datetime.timedelta(
            seconds=int(transaction['consensus_timestamp'].split('.')[0]))

        # Extract the desired information
        extracted_data = {
            'consensus_timestamp': consensus_timestamp.isoformat(),  # Convert datetime to string using ISO format
            'name': transaction.get('name', None),  # Use .get() in case 'name' key does not exist
            'nft_transfers': transaction.get('nft_transfers', None),
            'transfers': transaction.get('transfers', None)
        }

        return extracted_data

    # In case there are no transactions in the response
    return None


def fetch_nfts_from_mirror_node(serialNumber, nextUrl = None):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = nextUrl or f'/api/v1/tokens/{CFP_TOKEN_ID}/nfts/{serialNumber}/transactions'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    nft_data = []
    for nft in nfts['transactions']:
        transaction_id = nft['transaction_id']
        transaction_data = fetch_transaction_from_mirror_node(transaction_id)
        nft_data.append(transaction_data)

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_data.extend(fetch_nfts_from_mirror_node(serialNumber, nfts['links']['next']))

    return nft_data

def main():
    all_nft_data = []
    for serialNumber in range(1, 10):  # Loop over serial numbers 1-1000
        nft_data = fetch_nfts_from_mirror_node(serialNumber)

        all_nft_data.append(nft_data)

    # Save all_nft_data to a JSON file
    with open('nft_data.json', 'w') as f:
        json.dump(all_nft_data, f, indent=4)

if __name__ == "__main__":
    main()
