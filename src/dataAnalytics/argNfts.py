import json

# Load data from file
with open('nftMetadata.json', 'r') as f:
    data = json.load(f)

# Process data
output = []
for item in data:
    new_item = {
        "serial_number": item["serial_number"],
        "edition": item["edition"],
        "playable": item["playable"],
        "tokenId": "0.0.2235264"  # Add this value to all objects
    }

    # Extract the "Race" attribute
    for attr in item["attributes"]:
        if attr["trait_type"] == "Race":
            new_item["race"] = attr["value"]
            break

    output.append(new_item)

# Write processed data to a new file
with open('argNfts.json', 'w') as f:
    json.dump(output, f, indent=2)
