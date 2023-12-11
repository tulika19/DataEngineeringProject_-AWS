import boto3
import json

# AWS credentials and region
from config import aws_access_key_id, aws_secret_access_key, aws_region
# S3 bucket and folder information
s3_bucket_name = 'datapipeline-input-dataset'
folder_paths = ['solardataset/', 'winddataset/']  # Add your folder paths here
#folder_paths = ['winddataset/']

# Initialize S3 client
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                         region_name=aws_region)


# Function to retrieve information from a .tif file
def get_tif_file_info(file_path):
    response = s3_client.get_object(Bucket=s3_bucket_name, Key=file_path)
    file_size = response['ContentLength']
    # Add your logic to extract country name, type of data, etc. from the .tif file
    country_name = 'N/A'
    data_type = 'N/A'
    return country_name, data_type, file_size


# Iterate through folder paths
for folder_path in folder_paths:
    # List objects in the folder
    response = s3_client.list_objects(Bucket=s3_bucket_name, Prefix=folder_path)

    # Iterate through files in the folder
    for obj in response.get('Contents', []):
        file_path = obj['Key']

        # Process only .tif files
        if file_path.lower().endswith('.tif'):
            country_name, data_type, file_size = get_tif_file_info(file_path)

            # Print the extracted information
            print(f"File: {file_path}")
            print(f"Country Name: {country_name}")
            print(f"Data Type: {data_type}")
            print(f"File Size: {file_size} bytes")
            print("\n")