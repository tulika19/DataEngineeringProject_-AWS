import boto3
import json

# AWS credentials and region
from config import aws_access_key_id, aws_secret_access_key, aws_region

# S3 bucket and folder information
s3_bucket_name = 'datapipeline-input-dataset'
folder_paths = ['pollutiondataset/', 'populationdataset/']  # Add your folder paths here

# Initialize S3 client
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                         region_name=aws_region)


# Function to retrieve information from a .json file
def get_json_file_info(file_path):
    response = s3_client.get_object(Bucket=s3_bucket_name, Key=file_path)
    json_data = json.loads(response['Body'].read().decode('utf-8'))

    # Extract information from JSON data
    country_name = json_data.get('country_name', 'N/A')
    data_type = json_data.get('data_type', 'N/A')
    file_size = response['ContentLength']

    return country_name, data_type, file_size


# Iterate through folder paths
for folder_path in folder_paths:
    # List objects in the folder
    response = s3_client.list_objects(Bucket=s3_bucket_name, Prefix=folder_path)

    # Iterate through files in the folder
    for obj in response.get('Contents', []):
        file_path = obj['Key']

        # Process only .json files
        if file_path.lower().endswith('.json'):
            country_name, data_type, file_size = get_json_file_info(file_path)

            # Print the extracted information
            print(f"File: {file_path}")
            print(f"Country Name: {country_name}")
            print(f"Data Type: {data_type}")
            print(f"File Size: {file_size} bytes")
            print("\n")