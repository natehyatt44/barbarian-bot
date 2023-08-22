import requests
import json
import time
import datetime
import csv
import boto3
import io
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import pandas as pd
from dotenv import load_dotenv
import os

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

def read_data_s3(token_id, filename):
    """Read the discord users from an S3 bucket into a DataFrame."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'], delimiter='|')
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"No data found for token_id {token_id}.")
            return pd.DataFrame()  # Returning an empty dataframe for consistency
        else:
            # Print the unexpected error
            print(f"Unexpected error: {e}")
            return pd.DataFrame()  # Returning an empty dataframe for consistency
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")
        return pd.DataFrame()


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


import pandas as pd


def compare_nfts_with_existing_data(token_id, nft_data):
    # Load existing data from S3
    previous_nft_df = read_data_s3(token_id, 'nftConfig.csv')

    # Convert the spender column
    for item in nft_data:
        item['spender'] = get_market_account_name([item['spender']])

    # If previous data is empty, consider all current data as updated
    if previous_nft_df.empty:
        updated_serial_numbers = [item['serial_number'] for item in nft_data]
        save_updated_nft_data_s3(token_id, 'nftConfig.csv', nft_data)
        return updated_serial_numbers

    # Convert the list of dictionaries into DataFrame
    current_nft_df = pd.DataFrame(nft_data)

    # Convert merge columns to string for both dataframes to ensure matching data types
    columns_to_convert = ['account_id', 'token_id', 'serial_number', 'modified_timestamp']
    for column in columns_to_convert:
        previous_nft_df[column] = previous_nft_df[column].astype(str)
        current_nft_df[column] = current_nft_df[column].astype(str)

    # Merge the two dataframes on your specified columns
    merged_df = pd.merge(left=previous_nft_df, right=current_nft_df,
                         on=['account_id', 'token_id', 'serial_number', 'modified_timestamp'],
                         how='outer', indicator=True)

    # Filter the rows where the data doesn't match
    discrepancies = merged_df[merged_df['_merge'] != 'both']

    # Get the serial numbers of the discrepancies
    updated_serial_numbers = discrepancies['serial_number'].tolist()
    if updated_serial_numbers == ['serial_number']:
        updated_serial_numbers = 0

    save_updated_nft_data_s3(token_id, 'nftConfig.csv', nft_data)

    return updated_serial_numbers


def fetch_transaction_from_mirror_node(transactionId):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = f'/api/v1/transactions/{transactionId}'

    response = requests.get(f'{url}{path}')
    transaction = response.json()

    # In case there are no transactions in the response
    return transaction

def fetch_nfts_from_mirror_node(token_id, serial_number, nextUrl = None):
    url = 'https://mainnet-public.mirrornode.hedera.com'

    # If last_timestamp is provided, append it to the path
    path = nextUrl or f'/api/v1/tokens/{token_id}/nfts/{serial_number}/transactions'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    mint_transaction_ids = set(nft['transaction_id'] for nft in nfts['transactions'] if nft['type'] == 'TOKENMINT')

    nft_data = []
    for nft in nfts['transactions']:
        transaction_id = nft['transaction_id']
        if nft['type'] in ['CRYPTOTRANSFER', 'CRYPTOAPPROVEALLOWANCE'] and transaction_id not in mint_transaction_ids:
            transaction_data = fetch_transaction_from_mirror_node(transaction_id)
            nft_data.append(transaction_data)

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_data.extend(fetch_nfts_from_mirror_node(token_id, serial_number, nfts['links']['next']))

    return nft_data

def get_market_account_name(transfer_accounts):
    mapping = {
        "0.0.1064038": "sentx",
        "0.0.690356": "zuse"
    }

    for account_id in transfer_accounts:
        if account_id in mapping:
            return mapping[account_id]
    return None

def process_and_store_data(token_id, all_nft_data, filename="nft_transactions.csv"):
    # Flatten the nested list structure
    flattened_data = [transaction for sublist in all_nft_data for transaction in sublist["transactions"]]

    # Convert flattened_data to DataFrame and drop duplicates based on 'transaction_id'
    df_flattened = pd.DataFrame(flattened_data)
    df_flattened.drop_duplicates(subset='transaction_id', inplace=True)

    # Convert the DataFrame back to a list of dictionaries
    flattened_data = df_flattened.to_dict('records')

    csv_data = []

    for item in flattened_data:
        txn_time_as_datetime = hedera_timestamp_to_datetime(txn['consensus_timestamp'])

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
            csv_data.append([txn_time_as_datetime, 'Sale', market_name, sender, receiver, token_id, serial_number, individual_nft_price ])

    # Convert csv_data to a DataFrame
    df = pd.DataFrame(csv_data, columns=["txn_time",
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
    save_updated_nft_data_s3(token_id, filename, df)

    # Calculate and print the total amount
    total_amount = df["Amount"].sum()
    print(f"Total Amount: {total_amount}")

def extract_nft_listing_data(token_id, nft_data_with_spender):



    # Filter for "CRYPTOALLOWANCE" transactions.
    crypto_allowance_transactions = [transaction for transaction_data in nft_transactions for transaction in
                                     transaction_data['transactions'] if
                                     transaction['name'] == 'CRYPTOAPPROVEALLOWANCE']

    listings = []

    for txn in crypto_allowance_transactions:
        memo_decoded = decode_memo_base64(txn['memo_base64'])

        # If the decoded memo includes the word "Bulk", skip this transaction
        if "Bulk" in memo_decoded:
            continue

        parts = memo_decoded.split()

        # Find the position of the word "Serial" in parts
        if "Serial" in parts:
            index = parts.index("Serial")
            serial_number = parts[index + 1]  # serial number is right next to "Serial"

            # Assuming the structure remains consistent, you can get the amount in this manner
            amount = parts[-2]

            # Extract the account ID from the transfers list
            # We will look for an account that is not "0.0.8" and "0.0.98"
            account_id = None
            for transfer in txn["transfers"]:
                if transfer["account"] not in ["0.0.8", "0.0.98"]:
                    account_id = transfer["account"]
                    break

            txn_time_as_datetime = hedera_timestamp_to_datetime(txn['consensus_timestamp'])

            listings.append({
                'txn_time': txn_time_as_datetime,
                'txn_type': "List",
                'account_id_seller': account_id,
                'token_id': token_id,
                'serial_number': serial_number,
                'amount': amount
            })

    print(listings)

    listings_df = pd.DataFrame(listings)

    # Convert the columns to string type for both DataFrames
    listings_df['account_id_seller'] = listings_df['account_id_seller'].astype(str)
    listings_df['token_id'] = listings_df['token_id'].astype(str)
    listings_df['serial_number'] = listings_df['serial_number'].astype(str)

    nft_data_with_spender['account_id'] = nft_data_with_spender['account_id'].astype(str)
    nft_data_with_spender['token_id'] = nft_data_with_spender['token_id'].astype(str)
    nft_data_with_spender['serial_number'] = nft_data_with_spender['serial_number'].astype(str)

    # Assuming you have the nft_data_with_spender DataFrame already.
    merged_df = listings_df.merge(
        nft_data_with_spender,
        left_on=['account_id_seller', 'token_id', 'serial_number'],
        right_on=['account_id', 'token_id', 'serial_number'],
        how='inner'
    )

    # Write the resultant DataFrame to nft_listings.csv.
    merged_df.to_csv('nft_listings.csv', index=False, columns=['txn_time',
                                                               'txn_type',
                                                               'market_name',
                                                               'account_id_seller',
                                                               'token_id',
                                                               'serial_number',
                                                               'amount'])
    nft_data_with_spender.to_csv('here.csv', index=False)

def main():
    token_id = '0.0.2235264'
    nft_data = fetch_all_nfts(token_id)
    updated_serial_numbers = compare_nfts_with_existing_data(token_id, nft_data)

    all_nft_data = []  # To store data fetched for each updated serial number

    # Loop over updated serial numbers and fetch details from mirror node
    for serial_number in updated_serial_numbers:
        nft_transactions = fetch_nfts_from_mirror_node(token_id, serial_number)
        all_nft_data.extend(nft_transactions)

    process_and_store_data(token_id, all_nft_data)

    nft_data_with_spender = nft_data[nft_data_df['spender'].notna()]
    extract_nft_listing_data(token_id, all_nft_data, nft_data_with_spender)



if __name__ == "__main__":
    main()
