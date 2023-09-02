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
from PIL import Image
from io import BytesIO

# Bucket used for processing data
bucket = 'lost-ones-upload32737-staging'

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

def discord_nft_listings(token_id, config):
    last_listing_date = config['last_discord_listings_ts']
    print(last_listing_date)

    if last_listing_date:
        last_listing_timestamp = datetime.strptime(last_listing_date, '%Y-%m-%d %H:%M:%S')
    else:
        last_listing_timestamp = "2023-01-01 00:00:00"

    #last_listing_timestamp = "2023-08-25 00:00:00"

    # read listings csv
    df = read_df_s3(token_id, 'nft_listings.csv')
    df['txn_time'] = pd.to_datetime(df['txn_time'])
    # Assuming df is the dataframe obtained from read_df_s3
    filtered_df = df[df['txn_time'] > last_listing_timestamp]

    # Group by 'serial_number' and take the row with the latest timestamp
    grouped_df = filtered_df.groupby('serial_number', group_keys=True).apply(
        lambda x: x.sort_values('txn_time', ascending=False).iloc[0])

    results = []
    for index, row in grouped_df.iterrows():
        # Set your required variables
        txn_time = row['txn_time']
        account_id_seller = row['account_id_seller']
        serial_number = row['serial_number']
        market_name = row['market_name']
        amount = row['amount']

        # Fetch the metadata from the provided API
        response = requests.get(f'https://mainnet-public.mirrornode.hedera.com/api/v1/tokens/{token_id}/nfts/{serial_number}')
        data = response.json()
        metadata = data.get('metadata')
        if metadata:
            cid = base64.b64decode(metadata).decode('utf-8').replace('ipfs://', '')

            # Fetch the IPFS content using the CID
            response = requests.get(f'https://ipfs.io/ipfs/{cid}')
            data = response.json()

            name = data['name']
            image = data['image']
            image = image.replace('ipfs://', '')
            if token_id == '0.0.2371643':
                image_url = f'{image}'
            else:
                image_url = f'https://ipfs.io/ipfs/{image}'

            market_link = ""
            if market_name == "SentX":
                market_link = f"https://sentx.io/nft-marketplace/{token_id}/{serial_number}"
            else:
                market_link = f"https://zuse.market/collection/{token_id}"

            results.append({
                "txn_time": txn_time,
                "account_id_seller": account_id_seller,
                "serial_number": serial_number,
                "market_name": market_name,
                "amount": amount,
                "market_link": market_link,
                "image_url": image_url,
                "name": name,
            })

        else:
            print(f"No metadata found for token_id: {token_id}, serial_number: {serial_number}")

    return results

def execute(token_id):
    # Pull config file
    config = read_json_s3(token_id, 'nft_config.json')
    # Pull nft_data
    listings = discord_nft_listings(token_id, config)

    if not listings:
        return

    # Sort the listings by txn_time in descending order
    listings_sorted = sorted(listings, key=lambda x: x['txn_time'], reverse=True)
    # Get the most recent txn_time
    most_recent_timestamp = listings_sorted[0]['txn_time'] if listings_sorted else None

    config['last_discord_listings_ts'] = most_recent_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    upload_json_s3(token_id, 'nft_config.json', config)

    return listings
