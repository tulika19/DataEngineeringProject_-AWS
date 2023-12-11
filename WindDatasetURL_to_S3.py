import os
import requests
import boto3
from urllib.parse import urlparse
from config import aws_access_key_id, aws_secret_access_key, aws_region
def download_file(url, local_filename):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)


def upload_to_s3(local_filename, s3_bucket,s3_folder, s3_key, aws_access_key_id, aws_secret_access_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    s3_client.upload_file(local_filename, s3_bucket, f"{s3_folder}/{s3_key}")

    print(f"File uploaded to S3: s3://{s3_bucket}/{s3_folder}/{s3_key}")

def main():
    # Replace these with your AWS credentials and S3 bucket details

    s3_bucket_name = 'datapipeline-input-dataset'
    s3_folder_path = "winddataset"


    file_url = 'https://globalwindatlas.info/api/gis/country/AFG/wind-speed/10'

    # Parse the URL to extract the filename
    file_name = os.path.basename(urlparse(file_url).path)
    #local_filename = f'/path/to/local/{file_name}'  # Update with your desired local path
    local_filename = 'D:\Tulika\Work\LotusProject_22_nov_2023_UK\{file_name}'  # Update with your desired local path


    # Download the file
    download_file(file_url, local_filename)

    # Upload to AWS S3
    #s3_key = f'path/in/s3/{file_name}'
    s3_key = f'{file_name}'
    upload_to_s3(local_filename, s3_bucket_name,s3_folder_path, s3_key, aws_access_key_id, aws_secret_access_key)

if __name__ == "__main__":
    main()