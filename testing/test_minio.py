import os
from minio import Minio
from minio.error import S3Error

# --- Configuration ---
# Minio Client Details
MINIO_ENDPOINT = "127.0.0.1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Bucket and Object Details
BUCKET_NAME = "my-test-lifecycle-bucket"
OBJECT_NAME = "my-test-file.txt"

# Local File Paths
LOCAL_UPLOAD_FILE_PATH = "upload-temp.txt"
LOCAL_DOWNLOAD_FILE_PATH = "downloaded-file.txt"

# --- Main Script ---

# Initialize Minio client
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # Use HTTP for local testing
)

try:
    print("--- Starting Minio Object Lifecycle Test ---")

    # == STEP 1: SETUP & UPLOAD ==
    # Create a dummy file to upload
    print(f"\n[1.1] Creating local file '{LOCAL_UPLOAD_FILE_PATH}' for upload...")
    with open(LOCAL_UPLOAD_FILE_PATH, "w") as f:
        f.write("This is a test file for the minio-py lifecycle test.\n")
        # Make it > 5MB to ensure multipart upload is triggered
        f.write("Testing multipart upload flow.\n" * 500000)
    print("Local file created.")

    # Create bucket if it does not exist
    found = client.bucket_exists(BUCKET_NAME)
    if not found:
        print(f"[1.2] Bucket '{BUCKET_NAME}' not found. Creating it.")
        client.make_bucket(BUCKET_NAME)
    else:
        print(f"[1.2] Bucket '{BUCKET_NAME}' already exists.")

    # Upload the file using fput_object
    print(f"[1.3] Uploading '{LOCAL_UPLOAD_FILE_PATH}' to '{BUCKET_NAME}/{OBJECT_NAME}'...")
    result = client.fput_object(
        BUCKET_NAME,
        OBJECT_NAME,
        LOCAL_UPLOAD_FILE_PATH,
        part_size=5 * 1024 * 1024, # 5MB part size
    )
    print(f"✅ Upload successful! ETag: {result.etag}")

    # == STEP 2: DOWNLOAD & VERIFY ==
    print(f"\n[2.1] Downloading object to '{LOCAL_DOWNLOAD_FILE_PATH}'...")
    client.fget_object(BUCKET_NAME, OBJECT_NAME, LOCAL_DOWNLOAD_FILE_PATH)
    print("Download successful.")

    # Verify that the downloaded file exists on local storage
    if os.path.exists(LOCAL_DOWNLOAD_FILE_PATH):
        print(f"✅ Get successful: File '{LOCAL_DOWNLOAD_FILE_PATH}' verified on local storage.")
    else:
        # This part should ideally not be reached if fget_object succeeded
        print(f"❌ Get failed: File '{LOCAL_DOWNLOAD_FILE_PATH}' not found after download attempt.")


    # == STEP 3: REMOTE CLEANUP (OBJECT & BUCKET) ==
    print(f"\n[3.1] Deleting object '{OBJECT_NAME}' from bucket '{BUCKET_NAME}'...")
    client.remove_object(BUCKET_NAME, OBJECT_NAME)
    print("✅ Remote object deleted successfully.")

    print(f"[3.2] Deleting bucket '{BUCKET_NAME}'...")
    client.remove_bucket(BUCKET_NAME)
    print("✅ Remote bucket deleted successfully.")


except S3Error as exc:
    print(f"\n❌ An S3 error occurred: {exc}")

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