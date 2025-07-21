import os
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

# Create a dummy file to upload
file_name = "my-test-file.txt"
try:
    with open(file_name, "w") as f:
        f.write("This is a test file for the boto3 compatibility test.\n")
        # Make the file larger than the multipart threshold to test that flow.
        # 5MB is a common default threshold.
        f.write("This line makes the file bigger.\n" * 500000)
except IOError as e:
    print(f"Error creating dummy file: {e}")
    exit(1)


# Boto3 S3 Client Configuration
# This client is configured to connect to a local Minio instance.
s3_client = boto3.client(
    's3',
    endpoint_url='http://127.0.0.1:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1' # region_name is required but can be a dummy value for Minio
)

# New bucket name as requested
bucket_name = "my-boto3-test-bucket"

try:
    # 1. Check if bucket exists, create if not
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        # If a ClientError is thrown, check if it's a 404 error.
        # If it is a 404 error, the bucket does not exist.
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == '404' or error_code == 'NoSuchBucket':
            print(f"Bucket '{bucket_name}' not found. Creating it.")
            s3_client.create_bucket(Bucket=bucket_name)
            # Wait until the bucket exists before proceeding
            waiter = s3_client.get_waiter('bucket_exists')
            waiter.wait(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            # Reraise the exception if it's not a 404, as it's an unexpected error.
            print(f"An unexpected error occurred during bucket check: {e}")
            raise

    # 2. Upload the file using upload_file, which handles multipart automatically
    print(f"Uploading '{file_name}' to bucket '{bucket_name}'...")

    # Configure the multipart upload threshold and part size to match the original script
    transfer_config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,
        multipart_chunksize=5 * 1024 * 1024,
    )

    # upload_file handles reading the file and performing multipart upload if necessary
    s3_client.upload_file(
        file_name,
        bucket_name,
        file_name, # The object name (key) in the bucket
        Config=transfer_config
    )

    # To get the ETag, we can perform a head_object call after the upload
    response = s3_client.head_object(Bucket=bucket_name, Key=file_name)
    # The ETag from boto3 often includes quotes, which we can remove
    etag = response['ETag'].strip('"')

    print(
        f"'{file_name}' is successfully uploaded as object "
        f"'{file_name}' to bucket '{bucket_name}'. "
        f"ETag: {etag}"
    )

except ClientError as exc:
    print("A Boto3 client error occurred.", exc)
except Exception as exc:
    print(f"An unexpected error occurred: {exc}")

finally:
    # Clean up the dummy file
    if os.path.exists(file_name):
        os.remove(file_name)
        print(f"Cleaned up dummy file '{file_name}'.")
