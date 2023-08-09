import requests
import json

# Replace with your actual token ID
token_id = '0.0.2235264'

response = requests.get(f"https://mainnet-public.mirrornode.hedera.com/api/v1/tokens/{token_id}/nfts")

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print(data)
    # Check if there's any dataAnalytics in the response
    if 'nfts' in data:
        nfts = data['nfts']
        spender_nfts = [nft for nft in nfts if nft.get('spender') == "0.0.690356"]
        #print(json.dumps(spender_nfts, indent=4))
    else:
        print("No NFTs found.")
else:
    print(f"Request failed with status code {response.status_code}")
