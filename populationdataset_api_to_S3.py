import requests
import boto3
from botocore.exceptions import NoCredentialsError
# AWS S3 configurations
from config import aws_access_key_id, aws_secret_access_key, aws_region
# Replace these placeholders with your actual AWS credentials and S3 information

s3_bucket_name ='datapipeline-input-dataset'
s3_folder_path = "populationdataset"
s3_object_key = "worldpop_data.json"  # Object key in S3

# WorldPop API details
worldpop_api_url = "https://www.worldpop.org/rest/data"
worldpop_api_key = "YOUR_API_KEY"  # If required

# Send a GET request to the WorldPop API
api_url = f"{worldpop_api_url}"
headers = {"Authorization": f"Bearer {worldpop_api_key}"} if worldpop_api_key else {}
response = requests.get(api_url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    worldpop_data = response.json()

    # Save the JSON data locally
    with open("worldpop_data.json", 'w') as file:
        file.write(response.text)
    print("WorldPop data downloaded successfully.")

    # Upload the JSON data to AWS S3
    try:
        # Create an S3 client
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # Upload the file
        s3.upload_file("worldpop_data.json", s3_bucket_name, f"{s3_folder_path}/{s3_object_key}")
        print(f"WorldPop data uploaded to S3 bucket {s3_bucket_name} with object key {s3_folder_path}/{s3_object_key}")
    except NoCredentialsError:
        print("AWS credentials not available or incorrect.")
    except Exception as e:
        print(f"An error occurred while uploading the WorldPop data to S3: {e}")
else:
    print(f"Failed to download WorldPop data. Status code: {response.status_code}")