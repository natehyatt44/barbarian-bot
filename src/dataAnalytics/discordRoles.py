import json

# Load data from file
with open('nftMetadata.json', 'r') as f:
    data = json.load(f)

# Process data
output = []
for item in data:
    new_item = {
        "serialNumber": item["serial_number"],
        "tokenId": "0.0.2235264", # Add this value to all objects
        "isZombieSpirit": 0 # Default to 0. Zombie / Spirit + 1/1's will set this to 1
    }

    # Extract the "Race" attribute
    for attr in item["attributes"]:
        if attr["trait_type"] == "Race":
            new_item["race"] = attr["value"]
        if attr["trait_type"] == "Body":
            if attr["value"] == "Zombie":
                new_item["isZombieSpirit"] = 1
            if attr["value"] == "Spirit":
                new_item["isZombieSpirit"] = 1
        if attr["trait_type"] == "Accessory":
            if attr["value"] == "Viking":
                new_item["isZombieSpirit"] = 1
            break

    output.append(new_item)

# Write processed data to a new file
with open('discordRoleHelper.json', 'w') as f:
    json.dump(output, f, indent=2)
