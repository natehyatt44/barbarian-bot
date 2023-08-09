import boto3
import json
from collections import defaultdict
from operator import itemgetter

# Initialize the client with your credentials
s3 = boto3.client('s3')

# Specify your bucket name and prefix
bucket_name = 'lost-ones-upload32737-staging'
prefix = 'public/nft-collection-cfp/analytics/cfps.json'

# Initialize the client with your credentials
s3 = boto3.client('s3')

# Specify your bucket name and the key of the file you want
bucket_name = 'lost-ones-upload32737-staging'
object_key = 'public/nft-collection-cfp/analytics/cfps.json'

# Download the object and read it as a string
file_content = s3.get_object(Bucket=bucket_name, Key=object_key)['Body'].read().decode('utf-8')

# Load the JSON string as a Python object
json_list = json.loads(file_content)

# Initialize a dictionary to store the counts
trait_counts = defaultdict(lambda: defaultdict(int))

# Iterate over all items in the list
for item in json_list:
    # Iterate over all attributes in the item
    for attr in item['attributes']:
        # Increment the count for this trait type and value
        trait_counts[attr['trait_type']][attr['value']] += 1

# Now, trait_counts is a dictionary where the keys are trait types,
# and the values are dictionaries where the keys are trait values and
# the values are counts of that trait value for the trait type.

# Print the trait types, trait values, and their counts
for trait_type, values in trait_counts.items():
    print(f'Trait type: {trait_type}')

    # Sort the trait values by their counts in ascending order
    sorted_values = sorted(values.items(), key=itemgetter(1))

    for trait_value, count in sorted_values:
        print(f'    Trait value: {trait_value}, Count: {count}')