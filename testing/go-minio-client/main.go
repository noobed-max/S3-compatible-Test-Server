package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// --- Configuration ---
const (
	// Minio Client Details
	minioEndpoint    = "127.0.0.1:9000"
	minioAccessKey   = "minioadmin"
	minioSecretKey   = "minioadmin"
	useSSL           = false // Use HTTP for local testing

	// Bucket and Object Details
	bucketName   = "my-go-lifecycle-bucket"
	objectName   = "my-go-test-file.txt"

	// Local File Paths
	localUploadFilePath   = "upload-temp-go.txt"
	localDownloadFilePath = "downloaded-file-go.txt"
)

func main() {
	log.Println("--- Starting Minio Object Lifecycle Test ---")
	ctx := context.Background()

	// Initialize Minio client
	client, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalf("❌ Failed to initialize MinIO client: %v", err)
	}

	// The entire logic is wrapped in a function to easily use defer for cleanup.
	runLifecycleTest(ctx, client)

	log.Println("\n--- Test Finished ---")
}

// runLifecycleTest encapsulates the main logic to allow for deferred cleanup.
func runLifecycleTest(ctx context.Context, client *minio.Client) {
	// == STEP 1: SETUP & UPLOAD ==
	// Create a dummy file to upload and ensure it's cleaned up afterward.
	createDummyFileForUpload()
	defer cleanupLocalFile(localUploadFilePath, "upload")

	// Create bucket if it does not exist.
	createBucketIfNotExists(ctx, client)

	// Upload the file using FPutObject.
	uploadFile(ctx, client)

	// == STEP 2: DOWNLOAD & VERIFY ==
	// Download the object and ensure the local file is cleaned up.
	downloadObject(ctx, client)
	defer cleanupLocalFile(localDownloadFilePath, "download")

	// Verify the download.
	verifyDownload()

	// == STEP 3: REMOTE CLEANUP (OBJECT & BUCKET) ==
	// This cleanup happens before the local files are removed by the deferred calls.
	cleanupRemoteResources(ctx, client)
}

// createDummyFileForUpload creates a local file > 5MB to ensure multipart upload.
func createDummyFileForUpload() {
	log.Printf("\n[1.1] Creating local file '%s' for upload...", localUploadFilePath)
	file, err := os.Create(localUploadFilePath)
	if err != nil {
		log.Fatalf("❌ Failed to create local file: %v", err)
	}
	defer file.Close()

	// Create a large string to write to the file.
	// 5MB = 5 * 1024 * 1024 bytes.
	line := "This is a test file for the minio-go lifecycle test.\n"
	// Calculate how many lines are needed to exceed 5MB.
	numLines := (5*1024*1024/len(line)) + 1

	// Use strings.Builder for efficient string concatenation.
	var sb strings.Builder
	sb.Grow(numLines * len(line)) // Pre-allocate memory
	for i := 0; i < numLines; i++ {
		sb.WriteString(line)
	}

	_, err = file.WriteString(sb.String())
	if err != nil {
		log.Fatalf("❌ Failed to write to local file: %v", err)
	}
	log.Println("Local file created.")
}

// createBucketIfNotExists ensures the target bucket is available.
func createBucketIfNotExists(ctx context.Context, client *minio.Client) {
	found, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		log.Fatalf("❌ Failed to check for bucket: %v", err)
	}
	if !found {
		log.Printf("[1.2] Bucket '%s' not found. Creating it.", bucketName)
		err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			log.Fatalf("❌ Failed to create bucket: %v", err)
		}
	} else {
		log.Printf("[1.2] Bucket '%s' already exists.", bucketName)
	}
}

// uploadFile uploads the local file to the Minio bucket.
func uploadFile(ctx context.Context, client *minio.Client) {
	log.Printf("[1.3] Uploading '%s' to '%s/%s'...", localUploadFilePath, bucketName, objectName)
	info, err := client.FPutObject(ctx, bucketName, objectName, localUploadFilePath, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
		// PartSize is handled automatically by the library for files > 32MB by default,
		// but can be set explicitly if needed.
		// PartSize: 5 * 1024 * 1024,
	})
	if err != nil {
		log.Fatalf("❌ Upload failed: %v", err)
	}
	log.Printf("✅ Upload successful! ETag: %s", info.ETag)
}

// downloadObject retrieves the object from Minio and saves it locally.
func downloadObject(ctx context.Context, client *minio.Client) {
	log.Printf("\n[2.1] Downloading object to '%s'...", localDownloadFilePath)
	err := client.FGetObject(ctx, bucketName, objectName, localDownloadFilePath, minio.GetObjectOptions{})
	if err != nil {
		log.Fatalf("❌ Download failed: %v", err)
	}
	log.Println("Download successful.")
}

// verifyDownload checks if the downloaded file exists on the local filesystem.
func verifyDownload() {
	_, err := os.Stat(localDownloadFilePath)
	if os.IsNotExist(err) {
		log.Fatalf("❌ Get failed: File '%s' not found after download attempt.", localDownloadFilePath)
	} else if err != nil {
		log.Fatalf("❌ Error verifying downloaded file: %v", err)
	}
	log.Printf("✅ Get successful: File '%s' verified on local storage.", localDownloadFilePath)
}

// cleanupRemoteResources removes the object and the bucket from Minio.
func cleanupRemoteResources(ctx context.Context, client *minio.Client) {
	log.Printf("\n[3.1] Deleting object '%s' from bucket '%s'...", objectName, bucketName)
	err := client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		log.Printf("⚠️  Could not remove object: %v", err)
	} else {
		log.Println("✅ Remote object deleted successfully.")
	}

	log.Printf("[3.2] Deleting bucket '%s'...", bucketName)
	err = client.RemoveBucket(ctx, bucketName)
	if err != nil {
		log.Printf("⚠️  Could not remove bucket: %v", err)
	} else {
		log.Println("✅ Remote bucket deleted successfully.")
	}
}

// cleanupLocalFile removes a file from the local filesystem.
func cleanupLocalFile(path, fileType string) {
	log.Printf("\n--- Starting Local Cleanup for %s file ---", fileType)
	err := os.Remove(path)
	if err != nil {
		// Log as a warning, not a fatal error, to allow other cleanup to proceed.
		log.Printf("⚠️  Could not clean up local %s file '%s': %v", fileType, path, err)
	} else {
		log.Printf("🧹 Cleaned up local %s file: '%s'", fileType, path)
	}
}
