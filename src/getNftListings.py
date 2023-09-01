import json
import boto3
import pandas as pd
import base64
from datetime import datetime, timedelta

def hedera_timestamp_to_datetime(timestamp):
    unix_epoch = datetime.strptime('1970-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
    seconds_since_epoch = int(float(timestamp))
    dt_object = unix_epoch + timedelta(seconds=seconds_since_epoch)

    # Convert datetime object to string in the desired format
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')

def read_existing_data(file_path='test.json'):
    """Read the existing data from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def read_discord_users_from_s3(bucket, key):
    """Read the discord users from an S3 bucket into a DataFrame."""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)

    return pd.read_csv(obj['Body'], delimiter='|', names=['account_id', 'name', 'user_id', 'timestamp'])

def read_updated_nft_transactions(file_path='updated_nft_transactions.json'):
    """Read the NFT transaction data from a JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def decode_memo_base64(encoded_str):
    """Decode a base64 encoded string."""
    decoded_bytes = base64.b64decode(encoded_str)
    return decoded_bytes.decode('utf-8')


def get_market_account_name(account_id):
    mapping = {
        "0.0.1064038": "sentx",
        "0.0.690356": "zuse"
    }

    return mapping.get(account_id, "hashpack")


def extract_nft_listing_data(nft_data_with_spender):
    # Read updated NFT transactions.
    nft_transactions = read_updated_nft_transactions()

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
    # Read the NFT data into a DataFrame and filter for those with "spender" populated.
    nft_data_df = pd.DataFrame(read_existing_data())
    nft_data_with_spender = nft_data_df[nft_data_df['spender'].notna()]
    nft_data_with_spender = nft_data_with_spender.copy()
    nft_data_with_spender.loc[:, 'market_name'] = nft_data_with_spender['spender'].apply(get_market_account_name)

    # Read the Discord users from S3 into a DataFrame.
    bucket = 'lost-ones-upload32737-staging'
    key = 'public/discordAccounts/accounts.csv'
    discord_users_df = read_discord_users_from_s3(bucket, key)

    # Perform an outer join between the NFT data and the Discord users using 'account_id'.
    merged_df = nft_data_with_spender.merge(discord_users_df[['account_id', 'name']], on='account_id', how='outer')

    # Replace NaN in 'name' column with 'Unknown'.
    merged_df['name'].fillna('Unknown', inplace=True)

    # Group by account_id and name, count the NFTs, and sort the result.
    result = merged_df.groupby(['account_id', 'name']).size().reset_index(name='NFTs Listed')
    result = result.sort_values(by='NFTs Listed', ascending=False)

    #print(result.to_string(index=False))

    extract_nft_listing_data(nft_data_with_spender)


if __name__ == "__main__":
    main()
