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

def read_config_s3(token_id):
    """Read the config JSON from an S3 bucket."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/nft_config.json'
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return json.loads(data)
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"No config found for token_id {token_id}.")
            return {}  # Returning an empty dictionary for consistency
        else:
            print(f"Unexpected error: {e}")
            return {}
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")
        return {}

def update_and_save_config_s3(token_id, config, timestamp_col):
    """Update and save the config JSON to S3."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/nft_config.json'

    # Extract the latest timestamp from the dataframe
    latest_timestamp = dataframe[timestamp_col].max()

    # Update the config
    config = read_config_s3(token_id, filename)
    config[timestamp_col] = str(latest_timestamp)  # Converting to string in case it's a datetime object

    # Convert config to JSON format
    config_str = json.dumps(config)

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=config_str)
        print(f"Updated config for token_id {token_id} {filename} saved to S3.")
    except ClientError as e:
        print(f"Error saving updated config to S3: {e}")
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")

def save_updated_nft_data_s3(token_id, filename, updated_nft_data):
    """Save the updated NFT data back to S3, overwriting the original file."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(updated_nft_data)

    # Convert DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep='|', index=False)

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"Updated NFT data for token_id {token_id} {filename} saved to S3.")
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
    updated_nft_df = current_nft_df[current_nft_df['modified_timestamp'] > config["last_nft_listing_ts"]]

    # Extract the updated NFTs' details
    updated_nft_records = updated_nft_df[
        ['account_id', 'token_id', 'serial_number', 'modified_timestamp', 'spender']].to_dict(orient='records')

    # Save the new NFT data
    save_updated_nft_data_s3(token_id, 'nft_collection.csv', nft_data)

    return updated_nft_records
def nft_sales(token_id, all_nft_data, filename="nft_transactions.csv"):
    # Flatten the nested list structure
    flattened_data = [transaction for sublist in all_nft_data for transaction in sublist["transactions"]]

    # Convert flattened_data to DataFrame and drop duplicates based on 'transaction_id'
    df_flattened = pd.DataFrame(flattened_data)
    df_flattened.drop_duplicates(subset='transaction_id', inplace=True)

    # Convert the DataFrame back to a list of dictionaries
    flattened_data = df_flattened.to_dict('records')

    csv_data = []

    for item in flattened_data:
        txn_time_as_datetime = hedera_timestamp_to_datetime(item['consensus_timestamp'])
        txn_id = item['transaction_id']

        # Get all the account IDs from the transfers list
        transfer_accounts = [transfer['account'] for transfer in item.get('transfers', [])]

        # Calculate the total amount for the transaction
        total_amount = 0
        for transfer in item.get('transfers', []):
            if any(transfer['account'] == nft['receiver_account_id'] for nft in item['nft_transfers']):
                total_amount += abs(float(transfer['amount']) / 100000000)

        # Check if total_amount is 0; if it is, continue to the next iteration
        if total_amount == 0:
            continue

        # Calculate individual NFT price by dividing by the number of nft_transfers
        individual_nft_price = total_amount / len(item['nft_transfers']) if item['nft_transfers'] else None

        for nft_transfer in item['nft_transfers']:
            receiver = nft_transfer['receiver_account_id']
            sender = nft_transfer['sender_account_id'] if nft_transfer['sender_account_id'] else "N/A"
            serial_number = nft_transfer['serial_number']

            # Use the get_market_account_name function to set market_id
            market_name = get_market_account_name(transfer_accounts)
            csv_data.append([txn_time_as_datetime, txn_id, 'Sale', market_name, sender, receiver, token_id, serial_number, individual_nft_price ])

    # Convert csv_data to a DataFrame
    df = pd.DataFrame(csv_data, columns=["txn_time",
                                         "txn_id",
                                         "txn_type",
                                         "market_name",
                                         "account_id_seller",
                                         "account_id_buyer",
                                         "token_id",
                                         "serial_number",
                                         "amount"])

    # Sort DataFrame by Serial # and then by Transaction Time (both ascending)
    df = df.sort_values(by=["txn_time", "serial_number"])

    # Remove duplicates
    df = df.drop_duplicates()

    # Save DataFrame to S3
    upload_df_s3(token_id, filename, df)

    # Calculate and print the total amount
    total_amount = df["amount"].sum()
    print(f"Total Amount: {total_amount}")

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
        if nft['type'] in ['CRYPTOAPPROVEALLOWANCE'] and float(nft['consensus_timestamp']) > config["last_nft_listing_ts"]:
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
        print (memo_decoded)
        # If the decoded memo includes the word "Bulk", skip this transaction
        if "Bulk" in memo_decoded:
            continue

        amount = extract_hbar_amount(memo_decoded)
        market_name = extract_market_name(memo_decoded)

        if amount:  # Only proceed if we've successfully extracted an amount
            txn_time_as_datetime = hedera_timestamp_to_datetime(txn['consensus_timestamp'])
            txn_id = txn['trasnaction_id']

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

    listings_df = pd.DataFrame(listings)

    # Convert the columns to string type for consistency
    columns_to_convert = ['account_id_seller', 'token_id', 'serial_number']
    for column in columns_to_convert:
        listings_df[column] = listings_df[column].astype(str)

    # Save the resultant DataFrame to nft_listings.csv.
    listings_df.to_csv('nft_listings.csv', index=False)

def main():
    token_id = '0.0.2235264'
    config = read_config_s3(token_id)
    nft_data = fetch_all_nfts(token_id)
    updated_nft_records = compare_nfts_with_existing_data(token_id, config, nft_data)

    all_nft_data = []  # To store data fetched for each updated serial number

    # Loop over updated NFTs and fetch details from mirror node
    for nft_record in updated_nft_records:
        nft_transactions = fetch_nfts_from_mirror_node(token_id, config, nft_record)
        all_nft_data.extend(nft_transactions)

    # # Save all_nft_data to a JSON file
    # with open('nft_new.json', 'w') as f:
    #     json.dump(all_nft_data, f, indent=4)

    with open("nft_new.json", "r") as file:
        data = json.load(file)

    # nft_sales(token_id, all_nft_data)

    #nft_data_with_spender = nft_data[nft_data['spender'].notna()]
    #nft_sales(token_id, all_nft_data, nft_data_with_spender)

    nft_listings(token_id, all_nft_data)



if __name__ == "__main__":
    main()
