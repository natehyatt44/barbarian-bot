import json
import boto3
import pandas as pd
import base64
import src.s3helper as s3helper
import io
from datetime import datetime, timedelta

def read_discord_users_from_s3():
    """Read the discord users from an S3 bucket into a DataFrame."""
    bucket = 'lost-ones-upload32737-staging'
    key = 'public/discordAccounts/accounts.csv'
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)

    return pd.read_csv(obj['Body'], delimiter='|', names=['account_id', 'name', 'user_id', 'timestamp'])


def execute(token_id):
    nfts_df = s3helper.read_df_s3(token_id, 'nft_collection.csv')
    nfts_df = nfts_df[nfts_df['spender'].notna()]

    nft_listings = s3helper.read_df_s3(token_id, 'nft_listings.csv')
    nft_transactions = s3helper.read_df_s3(token_id, 'nft_transactions.csv')
    nft_mints = s3helper.read_df_s3(token_id, 'nft_mints.csv')

    discord_df = read_discord_users_from_s3()

    # Remove Bulk Listing entries
    nft_listings = nft_listings[nft_listings['amount'] != 'Bulk Listing']

    # Drop duplicates based on serial_number keeping the latest entry
    nft_listings = nft_listings.sort_values('txn_time').drop_duplicates('serial_number', keep='last')

    # Compute the total sale volume and buy volume for each seller and buyer
    total_sale_volume = nft_transactions.groupby('account_id_seller')['amount'].sum().reset_index().rename(
        columns={'amount': 'total_sale_volume'})
    total_buy_volume = nft_transactions.groupby('account_id_buyer')['amount'].sum().reset_index().rename(
        columns={'amount': 'total_buy_volume'})

    # Compute the total mint volume
    total_mint_volume = nft_mints.groupby('account_id_buyer')['amount'].sum().reset_index().rename(
        columns={'amount': 'total_mint_volume'})

    # Compute the lowest list price for each seller
    lowest_list_price = nft_listings.groupby('account_id_seller')['amount'].min().reset_index().rename(
        columns={'amount': 'lowest_list_price'})

    merged_df = nfts_df.merge(discord_df[['account_id', 'name']], on='account_id', how='outer')
    merged_df = merged_df.merge(total_sale_volume, left_on='account_id', right_on='account_id_seller',
                                how='left').drop('account_id_seller', axis=1)
    merged_df = merged_df.merge(total_buy_volume, left_on='account_id', right_on='account_id_buyer',
                                how='left').drop('account_id_buyer', axis=1)
    merged_df = merged_df.merge(total_mint_volume, left_on='account_id', right_on='account_id_buyer', how='left')
    merged_df = merged_df.merge(lowest_list_price, left_on='account_id', right_on='account_id_seller', how='left')

    merged_df['name'].fillna('Unknown', inplace=True)
    merged_df.loc[:, ['total_sale_volume', 'total_buy_volume', 'total_mint_volume', 'lowest_list_price']] = \
        merged_df[['total_sale_volume', 'total_buy_volume', 'total_mint_volume', 'lowest_list_price']].fillna(0)

    # Group by account_id and name, count the NFTs, and sort the result
    result = merged_df.groupby(['account_id', 'name']).agg(
        {'account_id': 'count', 'lowest_list_price': 'min', 'total_sale_volume': 'sum', 'total_buy_volume': 'sum', 'total_mint_volume': 'sum'}).rename(
        columns={'account_id': 'NFTs Listed'})
    result = result.sort_values(by='NFTs Listed', ascending=False).reset_index()  # Drop index here

    return result

def main():
    token_id_input = "0.0.2235264"
    results = execute(token_id_input)
    print(results.head(10).to_string(index=False))  # prints top 15 rows, if you only want it once

if __name__ == '__main__':
    main()

