import os
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

# --- Configuration ---
# S3-Compatible Endpoint Details (for Minio)
S3_ENDPOINT_URL = 'http://127.0.0.1:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
S3_REGION = 'us-east-1' # Required for boto3, but can be a dummy value for Minio

# Bucket and Object Details
BUCKET_NAME = "my-boto3-lifecycle-bucket"
OBJECT_NAME = "my-boto3-test-file.txt"

# Local File Paths
LOCAL_UPLOAD_FILE_PATH = "upload-temp-boto3.txt"
LOCAL_DOWNLOAD_FILE_PATH = "downloaded-file-boto3.txt"

# --- Main Script ---

# Initialize Boto3 S3 client
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION
)

try:
    print("--- Starting Boto3 S3 Object Lifecycle Test ---")

    # == STEP 1: SETUP & UPLOAD ==
    # Create a dummy file to upload
    print(f"\n[1.1] Creating local file '{LOCAL_UPLOAD_FILE_PATH}' for upload...")
    with open(LOCAL_UPLOAD_FILE_PATH, "w") as f:
        f.write("This is a test file for the boto3 lifecycle test.\n")
        # Make it > 5MB to ensure multipart upload is triggered
        f.write("Testing multipart upload flow with boto3.\n" * 500000)
    print("Local file created.")

    # Create bucket if it does not exist
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"[1.2] Bucket '{BUCKET_NAME}' already exists.")
    except ClientError as e:
        # If a 404 error is caught, the bucket doesn't exist.
        if e.response['Error']['Code'] == '404':
            print(f"[1.2] Bucket '{BUCKET_NAME}' not found. Creating it.")
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            # Wait until the bucket exists to avoid race conditions
            waiter = s3_client.get_waiter('bucket_exists')
            waiter.wait(Bucket=BUCKET_NAME)
        else:
            # Re-raise the exception if it's not a 'Not Found' error
            print("An unexpected error occurred during bucket check.")
            raise

    # Configure multipart upload settings
    transfer_config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024, # 5MB
        multipart_chunksize=5 * 1024 * 1024, # 5MB
    )

    # Upload the file using upload_file, which handles multipart automatically
    print(f"[1.3] Uploading '{LOCAL_UPLOAD_FILE_PATH}' to '{BUCKET_NAME}/{OBJECT_NAME}'...")
    s3_client.upload_file(
        LOCAL_UPLOAD_FILE_PATH,
        BUCKET_NAME,
        OBJECT_NAME,
        Config=transfer_config
    )

    # Get the ETag for verification
    response = s3_client.head_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME)
    etag = response['ETag'].strip('"')
    print(f"✅ Upload successful! ETag: {etag}")


    # == STEP 2: DOWNLOAD & VERIFY ==
    print(f"\n[2.1] Downloading object to '{LOCAL_DOWNLOAD_FILE_PATH}'...")
    s3_client.download_file(BUCKET_NAME, OBJECT_NAME, LOCAL_DOWNLOAD_FILE_PATH)
    print("Download successful.")

    # Verify that the downloaded file exists on local storage
    if os.path.exists(LOCAL_DOWNLOAD_FILE_PATH):
        print(f"✅ Get successful: File '{LOCAL_DOWNLOAD_FILE_PATH}' verified on local storage.")
    else:
        print(f"❌ Get failed: File '{LOCAL_DOWNLOAD_FILE_PATH}' not found after download.")


    # == STEP 3: REMOTE CLEANUP (OBJECT & BUCKET) ==
    print(f"\n[3.1] Deleting object '{OBJECT_NAME}' from bucket '{BUCKET_NAME}'...")
    s3_client.delete_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME)
    print("✅ Remote object deleted successfully.")

    print(f"[3.2] Deleting bucket '{BUCKET_NAME}'...")
    s3_client.delete_bucket(Bucket=BUCKET_NAME)
    print("✅ Remote bucket deleted successfully.")


except ClientError as exc:
    print(f"\n❌ A Boto3 client error occurred: {exc}")

finally:
    # == STEP 4: LOCAL CLEANUP ==
    print("\n--- Starting Local Cleanup ---")
    # Clean up the original uploaded file
    if os.path.exists(LOCAL_UPLOAD_FILE_PATH):
        os.remove(LOCAL_UPLOAD_FILE_PATH)
        print(f"🧹 Cleaned up local upload file: '{LOCAL_UPLOAD_FILE_PATH}'")

    # Clean up the downloaded file
    if os.path.exists(LOCAL_DOWNLOAD_FILE_PATH):
        os.remove(LOCAL_DOWNLOAD_FILE_PATH)
        print(f"🧹 Cleaned up local download file: '{LOCAL_DOWNLOAD_FILE_PATH}'")

    print("\n--- Test Finished ---")
