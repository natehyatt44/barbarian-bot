import requests
import json
import time
from datetime import datetime, timedelta
import csv
import boto3
import io
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import pandas as pd
from dotenv import load_dotenv
import os
import base64
import re

# Bucket used for processing data
bucket = 'lost-ones-upload32737-staging'
def hedera_timestamp_to_datetime(timestamp):
    unix_epoch = datetime.strptime('1970-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
    seconds_since_epoch = int(float(timestamp))
    dt_object = unix_epoch + timedelta(seconds=seconds_since_epoch)

    # Convert datetime object to string in the desired format
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')

def decode_memo_base64(encoded_str):
    """Decode a base64 encoded string."""
    decoded_bytes = base64.b64decode(encoded_str)
    return decoded_bytes.decode('utf-8')

def read_json_s3(token_id, filename):
    """Read the config JSON from an S3 bucket."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return json.loads(data)
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"No json {filename} found for token_id {token_id}.")
            return {}  # Returning an empty dictionary for consistency
        else:
            print(f"Unexpected error: {e}")
            return {}
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")
        return {}

def read_df_s3(token_id, filename):
    """Read a DataFrame from an S3 bucket."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return pd.read_csv(io.StringIO(data), delimiter='|')  # Specify delimiter here
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"No data found for token_id {token_id} & {filename}.")
            return pd.DataFrame()
        else:
            print(f"Unexpected error: {e}")
            return pd.DataFrame()
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")
        return pd.DataFrame()
def upload_json_s3(token_id, filename, json_data):
    """Update and save the config JSON to S3."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'

    # Convert config to JSON format
    str_data = json.dumps(json_data)

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=str_data)
        print(f"Updated json for token_id {token_id} {filename} saved to S3.")
    except ClientError as e:
        print(f"Error saving updated config to S3: {e}")
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")


def upload_df_s3(token_id, filename, df):
    """Save the updated NFT data back to S3, overwriting the original file."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(df)

    # Convert DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep='|', index=False)

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"Updated data token_id {token_id} {filename} saved to S3.")
    except ClientError as e:
        print(f"Error saving updated NFT data to S3: {e}")
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")

def fetch_all_nfts(token_id, nextUrl=None):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = nextUrl or f'/api/v1/tokens/{token_id}/nfts/'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    nft_data = []
    if len(nfts['nfts']) > 0:
        for item in nfts['nfts']:
            account_id = item['account_id']
            token_id = item['token_id']
            serial_number = item['serial_number']
            modified_timestamp = item['modified_timestamp']
            spender = item['spender']
            nft_data.append({'account_id': account_id, 'token_id': token_id, 'serial_number': serial_number,
                             'modified_timestamp': modified_timestamp, 'spender': spender})

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_data.extend(fetch_all_nfts(token_id, nfts['links']['next']))

    return nft_data

def compare_nfts_with_existing_data(token_id, config, nft_data):
    """
    Compare NFTs with the existing data using the config.

    Args:
    - token_id: The token ID
    - config: A dict containing configuration values.
    - nft_data: List of new NFT data.

    Returns:
    - A list of updated serial numbers based on the provided configuration.
    """

    # Convert the spender column
    for item in nft_data:
        item['spender'] = get_market_account_name([item['spender']])

    # Convert the list of dictionaries into DataFrame
    current_nft_df = pd.DataFrame(nft_data)

    # Convert columns to correct data type for comparison
    current_nft_df['modified_timestamp'] = current_nft_df['modified_timestamp'].astype(float)

    # Filter data based on modified_timestamp
    updated_nft_df = current_nft_df[current_nft_df['modified_timestamp'] > float(config["last_nft_listing_ts"])]

    # Extract the updated NFTs' details
    updated_nft_records = updated_nft_df[
        ['account_id', 'token_id', 'serial_number', 'modified_timestamp', 'spender']].to_dict(orient='records')

    # Save the new NFT data
    upload_df_s3(token_id, 'nft_collection.csv', current_nft_df)

    return updated_nft_records


def fetch_transaction_from_mirror_node(transactionId):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = f'/api/v1/transactions/{transactionId}'

    response = requests.get(f'{url}{path}')
    transaction = response.json()

    # In case there are no transactions in the response
    return transaction

def fetch_nfts_from_mirror_node(token_id, config, nft_record, nextUrl = None):
    url = 'https://mainnet-public.mirrornode.hedera.com'

    # If last_timestamp is provided, append it to the path
    path = nextUrl or f'/api/v1/tokens/{token_id}/nfts/{nft_record["serial_number"]}/transactions'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    nft_listing_data = []
    for nft in nfts['transactions']:
        if nft['type'] in ['CRYPTOAPPROVEALLOWANCE'] and float(nft['consensus_timestamp']) > float(config["last_nft_listing_ts"]):
            listing_data = fetch_transaction_from_mirror_node(nft['transaction_id'])
            # Append the nft_record data to the listing_data
            listing_data['account_id'] = nft_record['account_id']
            listing_data['token_id'] = nft_record['token_id']
            listing_data['serial_number'] = nft_record['serial_number']
            listing_data['modified_timestamp'] = nft_record['modified_timestamp']
            nft_listing_data.append(listing_data)

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_listing_data.extend(fetch_nfts_from_mirror_node(token_id, config, nft_record, nfts['links']['next']))

    return nft_listing_data

def get_market_account_name(transfer_accounts):
    mapping = {
        "0.0.1064038": "sentx",
        "0.0.690356": "zuse"
    }

    for account_id in transfer_accounts:
        if account_id in mapping:
            return mapping[account_id]
    return None

def extract_hbar_amount(memo_decoded):
    # Check for patterns and extract the HBAR amount
    patterns = [
        r"for (\d+) HBAR",     # Matches "for 665 HBAR" or "for 400 HBAR"
        r"#\d+ for (\d+) HBAR" # Matches "#930 for 400 HBAR" or "#27 for 2222 HBAR"
    ]

    for pattern in patterns:
        match = re.search(pattern, memo_decoded)
        if match:
            return match.group(1)  # Return the matched HBAR amount
    return None  # If no matches found

def extract_market_name(memo_decoded):
    # Check for unique patterns in memo to determine the market name
    # Zuse Example:
    #   (0.0.1317633) Confirm listing of NFT: 0.0.2235264 with serial number 29 for 500 HBAR
    # SentX Examples:
    #   Approve NFT Token 0.0.2235264 Serial 27 marketplace listing for 2500 HBAR
    #   SentX Market Listing: NFT 0.0.2235264 #27 for 2222 HBAR
    #   Approve Bulk Listing of 5 NFTs on Sentient Marketplace
    #   SentX Market Bulk Listing: 4 NFTs

    if "Confirm listing of NFT:" in memo_decoded:
        return "Zuse"
    elif "SentX Market Listing:" in memo_decoded or "Approve NFT Token" in memo_decoded:
        return "SentX"
    return None  # If no market name can be determined

def nft_listings(token_id, transactions):
    listings = []

    for transaction_block in transactions:
        txn = transaction_block['transactions'][0]
        memo_decoded = decode_memo_base64(txn['memo_base64'])

        # If the decoded memo includes the word "Bulk", skip this transaction
        if "Bulk" in memo_decoded:
            continue

        amount = extract_hbar_amount(memo_decoded)
        market_name = extract_market_name(memo_decoded)

        if amount:  # Only proceed if we've successfully extracted an amount
            txn_time_as_datetime = hedera_timestamp_to_datetime(txn['consensus_timestamp'])
            txn_id = txn['transaction_id']

            listings.append({
                'txn_time': txn_time_as_datetime,
                'txn_id': txn_id,
                'txn_type': "List",
                'account_id_seller': transaction_block['account_id'],
                'token_id': transaction_block['token_id'],
                'serial_number': transaction_block['serial_number'],
                'market_name': market_name,  # using spender as market_name
                'amount': amount
            })

    new_listings_df = pd.DataFrame(listings)

    # Convert the columns to string type for consistency
    columns_to_convert = ['account_id_seller', 'token_id', 'serial_number']
    for column in columns_to_convert:
        new_listings_df[column] = new_listings_df[column].astype(str)

    # Save the resultant DataFrame to nft_listings.csv.
    new_listings_df.to_csv('nft_listings.csv', index=False)

    # Pull existing list data, merge, and re-upload
    existing_listings_df = read_df_s3(token_id, 'nft_listings.csv')

    # If there's data in existing_listings_df, then merge
    if not existing_listings_df.empty:
        # Filter out entries from existing_listings_df that already exist in new_listings_df
        existing_listings_df = existing_listings_df[~existing_listings_df['txn_id'].isin(new_listings_df['txn_id'])]

        # Concatenate the two DataFrames
        combined_df = pd.concat([new_listings_df, existing_listings_df], ignore_index=True)
    else:
        combined_df = new_listings_df

    # If you still want to sort them (e.g., by 'txn_time' in descending order)
    combined_df.sort_values(by='txn_time', ascending=False, inplace=True)

    upload_df_s3(token_id, 'nft_listings.csv', combined_df)


def main():
    token_id = '0.0.2235264'
    # Pull config file
    config = read_json_s3(token_id, 'nft_config.json')
    # Pull nft_data
    nft_data = fetch_all_nfts(token_id)
    # Find updated records
    updated_nft_records = compare_nfts_with_existing_data(token_id, config, nft_data)

    all_nft_data = []  # To store data fetched for each updated serial number

    # Loop over updated NFTs and fetch details from mirror node
    for nft_record in updated_nft_records:
        nft_transactions = fetch_nfts_from_mirror_node(token_id, config, nft_record)
        all_nft_data.extend(nft_transactions)
        print(nft_transactions)

    upload_json_s3(token_id, f'nft_listings_raw/nft_listings-{datetime.now()}.json', all_nft_data)

    nft_listings(token_id, all_nft_data)

    # Pull Listing data for discord HERE

    # Assuming nft_data has been populated using the fetch_all_nfts function
    nft_data_sorted = sorted(nft_data, key=lambda x: x['modified_timestamp'], reverse=True)
    most_recent_timestamp = nft_data_sorted[0]['modified_timestamp'] if nft_data_sorted else None
    config['last_nft_listing_ts'] = most_recent_timestamp
    upload_json_s3(token_id, 'nft_config.json', config)



if __name__ == "__main__":
    main()
