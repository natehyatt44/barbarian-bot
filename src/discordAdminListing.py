import json
import boto3
import pandas as pd
import base64
import s3helper
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

    discord_df = read_discord_users_from_s3()

    merged_df = nfts_df.merge(discord_df[['account_id', 'name']], on='account_id', how='outer')

    # Replace NaN in 'name' column with 'Unknown'.
    merged_df['name'].fillna('Unknown', inplace=True)

    # Group by account_id and name, count the NFTs, and sort the result.
    result = merged_df.groupby(['account_id', 'name']).size().reset_index(name='NFTs Listed')
    result = result.sort_values(by='NFTs Listed', ascending=False)
    result = result.reset_index(drop=True)

    return result
