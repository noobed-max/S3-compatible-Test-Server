import os
from minio import Minio
from minio.error import S3Error

# Create a dummy file to upload
file_name = "my-test-file.txt"
with open(file_name, "w") as f:
    f.write("This is a test file for the minio-py fput_object compatibility test.\n")
    f.write("It's larger than the multipart threshold to test that flow.\n" * 500000) # Make it > 5MB

# Minio Client Configuration
client = Minio(
    "127.0.0.1:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False # Use HTTP for local testing
)

bucket_name = "my-test-bucket"

try:
    # 1. Check if bucket exists, create if not
    found = client.bucket_exists(bucket_name)
    if not found:
        print(f"Bucket '{bucket_name}' not found. Creating it.")
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

    # 2. Upload the file using fput_object
    print(f"Uploading '{file_name}' to bucket '{bucket_name}'...")
    result = client.fput_object(
        bucket_name, file_name, file_name,
        # Set part_size to force multipart upload for demonstration
        part_size=5 * 1024 * 1024, 
    )
    print(
        f"'{file_name}' is successfully uploaded as object "
        f"'{result.object_name}' to bucket '{bucket_name}'. "
        f"ETag: {result.etag}"
    )

except S3Error as exc:
    print("An S3 error occurred.", exc)
finally:
    # Clean up the dummy file
    if os.path.exists(file_name):
        os.remove(file_name)