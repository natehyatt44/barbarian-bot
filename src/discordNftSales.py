import requests
import json
import time
from datetime import datetime, timedelta
import csv
import pandas as pd
import src.s3helper as s3helper
from dotenv import load_dotenv
import os
import base64
import re
from PIL import Image
from io import BytesIO
import math

def discord_nft_sales(token_id, config):
    last_sales_date = config['last_discord_sales_ts']

    if last_sales_date:
        last_sales_timestamp = datetime.strptime(last_sales_date, '%Y-%m-%d %H:%M:%S')
    else:
        last_sales_timestamp = "2023-01-01 00:00:00"

    # last_sales_timestamp = "2023-08-31 00:00:00"

    # read sales csv
    df = s3helper.read_df_s3(token_id, 'nft_transactions.csv')
    results = []

    if df.empty == False:
        df['txn_time'] = pd.to_datetime(df['txn_time'])
        # Assuming df is the dataframe obtained from read_df_s3
        filtered_df = df[df['txn_time'] > last_sales_timestamp]

        # Group by 'serial_number' and take the row with the latest timestamp
        grouped_df = filtered_df.groupby('serial_number', group_keys=True).apply(
            lambda x: x.sort_values('txn_time', ascending=False).iloc[0])


        for index, row in grouped_df.iterrows():
            # Set your required variables
            txn_time = row['txn_time']
            account_id_seller = row['account_id_seller']
            account_id_buyer = row['account_id_buyer']
            serial_number = row['serial_number']
            market_name = row['market_name']
            amount = math.ceil(row['amount'])

            name = ''
            if token_id == '0.0.2235264':
                name = 'Community Founders Pass'
            if token_id == '0.0.2371643':
                name = 'The Alixon Collection'
            if token_id == '0.0.3721853':
                name = 'The Lost Ones'
            if token_id == '0.0.3954030':
                name = 'TrizTazz - Collection 1 : 1'
            if token_id == '0.0.4350721':
                name = 'The Tools'

            image_url = f'https://lost-ones-upload32737-staging.s3.amazonaws.com/public/data-analytics/{token_id}/images/{serial_number}.webp'

            market_link = ""
            if market_name == "SentX":
                market_link = f"https://sentx.io/nft-marketplace/{token_id}/{serial_number}"
            else:
                market_link = f"https://zuse.market/collection/{token_id}"

            results.append({
                "txn_time": txn_time,
                "account_id_seller": account_id_seller,
                "account_id_buyer": account_id_buyer,
                "serial_number": serial_number,
                "market_name": market_name,
                "amount": amount,
                "market_link": market_link,
                "image_url": image_url,
                "name": name,
            })

    return results

def execute(token_id):
    # Pull config file
    config = s3helper.read_json_s3(token_id, 'nft_config.json')
    # Pull nft_data
    sales = discord_nft_sales(token_id, config)

    if not sales:
        return

    # Sort the sales by txn_time in descending order
    sales_sorted = sorted(sales, key=lambda x: x['txn_time'], reverse=True)
    # Get the most recent txn_time
    most_recent_timestamp = sales_sorted[0]['txn_time'] if sales_sorted else None

    config['last_discord_sales_ts'] = most_recent_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    s3helper.upload_json_s3(token_id, 'nft_config.json', config)

    return sales_sorted
