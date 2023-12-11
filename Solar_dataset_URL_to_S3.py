import requests
import boto3
from botocore.exceptions import NoCredentialsError
from config import aws_access_key_id, aws_secret_access_key, aws_region   # AWS S3 configurations


s3_bucket_name = 'datapipeline-input-dataset'
s3_folder_path = "solardataset"
s3_object_key = "afghanistan_photovoltaic_power_potential.tif"  # Object key in S3

# URL of the image
url = "https://globalsolaratlas.info/download/afghanistan"

# Specify the local file name to save the image
local_filename = "afghanistan_photovoltaic_power_potential.tif"

# Download the image using requests
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Save the image locally
    with open(local_filename, 'wb') as file:
        file.write(response.content)
    print(f"Image downloaded successfully and saved as {local_filename}")

    # Upload the image to AWS S3
    try:
        # Create an S3 client
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # Upload the file
        s3.upload_file(local_filename, s3_bucket_name, f"{s3_folder_path}/{s3_object_key}")
        print(f"Image uploaded to S3 bucket {s3_bucket_name} with object key {s3_folder_path}/{s3_object_key}")
    except NoCredentialsError:
        print("AWS credentials not available or incorrect.")
    except Exception as e:
        print(f"An error occurred while uploading the image to S3: {e}")
else:
    print(f"Failed to download the image. Status code: {response.status_code}")