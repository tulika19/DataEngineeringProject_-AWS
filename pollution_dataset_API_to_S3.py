# Using this code you can download from URL and stored in S3 bucket.
#Just run this script and after sucessufully done > check the dataset in S3 bucket.


import requests
import boto3
from botocore.exceptions import NoCredentialsError
from config import aws_access_key_id, aws_secret_access_key, aws_region

# Replace these placeholders with your actual AWS credentials and S3 information

s3_bucket_name ='datapipeline-input-dataset'
s3_folder_path ='pollutiondataset'

s3_object_key = "aqi_data.json"  # Object key in S3

# URL of the AQI API
api_url = "https://api.waqi.info/feed/geo:37.7749;-122.4194/?token=YOUR_AQI_API_KEY"
# Replace "YOUR_AQI_API_KEY" with your actual API key from the AQI website

# Send a GET request to the AQI API
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    api_data = response.json()

    # Save the JSON data locally
    with open("aqi_data.json", 'w') as file:
        file.write(response.text)
    print("AQI data downloaded successfully.")

    # Upload the JSON data to AWS S3
    try:
        # Create an S3 client
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # Upload the file
        s3.upload_file("aqi_data.json", s3_bucket_name, f"{s3_folder_path}/{s3_object_key}")
        print(f"AQI data uploaded to S3 bucket {s3_bucket_name} with object key {s3_folder_path}/{s3_object_key}")
    except NoCredentialsError:
        print("AWS credentials not available or incorrect.")
    except Exception as e:
        print(f"An error occurred while uploading the AQI data to S3: {e}")
else:
    print(f"Failed to download AQI data. Status code: {response.status_code}")