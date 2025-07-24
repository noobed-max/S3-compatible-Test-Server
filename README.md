# FastAPI S3-Compatible Server

This project is a minimalist, S3-compatible object storage server built with FastAPI. The primary goal is to understand the core mechanics of the Amazon S3 API by implementing the handlers for basic operations like `PUT`, `GET`, and `DELETE` for objects and buckets.

It serves as a Minimum Viable Product (MVP) to test and understand the requests sent by popular S3 client SDKs like Boto3 (AWS) and Minio. This is not a production-ready S3 replacement, but rather a learning tool and a reference for building more robust S3-compatible services.

-----

## Features

  * **S3-Compatible API:** Implements a subset of the S3 REST API.
  * **Authentication:** Supports **AWS Signature Version 4** for secure requests.
  * **Bucket Operations:** `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListObjectsV2`.
  * **Object Operations:** `PutObject`, `GetObject`, `DeleteObject`, `HeadObject`.
  * **Multipart Uploads:** Full support for `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, and `AbortMultipartUpload`.
  * **Backend:** Uses a local filesystem for object storage (`s3_storage/`) and a SQLite database for metadata (`s3_metadata.db`).

-----

## Setup and Installation

Follow these steps to get the server running locally.

### 1\. Prerequisites

  * Python 3.8+
  * Go (only required to run the Go test client)

### 2\. Clone the Repository

```bash
git clone <your-repository-url>
cd S3-compatible-fastapiServerTest
```

### 3\. Set up Python Environment

It's recommended to use a virtual environment.

```bash
# Create a virtual environment
python -m venv venv

# Activate it
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

### 4\. Install Dependencies

Install the required Python packages from `requirements.txt`.

```bash
pip install -r requirements.txt
```

### 5\. Configure Credentials

The server needs initial credentials to create a default user. These are configured in the `OS-server/.env` file.

**File: `OS-server/.env`**

```env
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
```

You can change these values to whatever you prefer. The server will automatically create a user with these credentials on its first startup.

> **Important:** If you change these keys, you **must** update the credentials in the test client scripts (`testing/test_boto.py`, `testing/test_minio.py`, `testing/go-minio-client/main.go`) to match.

### 6\. Run the Server

Start the FastAPI server using Uvicorn. The test clients are configured to connect to port `9000`.

```bash
uvicorn OS-server.main:app --host 127.0.0.1 --port 9000 --reload
```

You should see output indicating the server is ready and has loaded the default credentials:

```
INFO:     Uvicorn running on http://127.0.0.1:9000 (Press CTRL+C to quit)
...
INFO:     Application startup complete.

Server is ready.
Default credentials for Minio Client loaded from .env file:
  Access Key: minioadmin
  Secret Key: minioadmin
```

-----

## How to Test

The `testing/` directory contains scripts that simulate a full object lifecycle: **create a large file, upload it, download it, verify it, and clean up** (delete the object and bucket).

Make sure the server is running before you execute any tests.

### Boto3 Client Test (Python)

This test uses AWS's official SDK, `boto3`, to interact with the server.

```bash
python testing/test_boto.py
```

### MinIO Client Test (Python)

This test uses MinIO's Python SDK, `minio`.

```bash
python testing/test_minio.py
```

### MinIO Client Test (Go)

This test uses MinIO's Go SDK. Make sure you have Go installed.

```bash
# Navigate to the Go client directory
cd testing/go-minio-client

# Run the test
go run main.go
```

For all tests, you should see a successful output in your terminal, and the server log will show the incoming `GET`, `PUT`, `POST`, and `DELETE` requests it's handling. 🚀