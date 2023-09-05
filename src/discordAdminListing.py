import json
import boto3
import pandas as pd
import base64
import io
from datetime import datetime, timedelta

def read_df_s3(token_id, filename):
    """Read a DataFrame from an S3 bucket."""
    s3 = boto3.client('s3')
    bucket = 'lost-ones-upload32737-staging'
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

def read_discord_users_from_s3():
    """Read the discord users from an S3 bucket into a DataFrame."""
    bucket = 'lost-ones-upload32737-staging'
    key = 'public/discordAccounts/accounts.csv'
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)

    return pd.read_csv(obj['Body'], delimiter='|', names=['account_id', 'name', 'user_id', 'timestamp'])


def execute(token_id):

    nfts_df = read_df_s3(token_id, 'nft_collection.csv')
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
