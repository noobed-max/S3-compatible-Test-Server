package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	// --- 1. Create a dummy file to upload ---
	// This section creates a large file to ensure the multipart upload mechanism is triggered.
	fileName := "my-go-test-file.txt"
	log.Printf("Creating dummy file: %s", fileName)

	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	// Use defer to ensure the file is closed and removed when the main function exits.
	defer file.Close()
	defer os.Remove(fileName)

	// Write a large amount of data to the file (> 5MB).
	// 5 * 1024 * 1024 bytes is 5MB. We'll write slightly more.
	line := "This is a test file for the minio-go FPutObject compatibility test.\n"
	// Calculate how many lines are needed to exceed 5MB
	numLines := (5 * 1024 * 1024 / len(line)) + 1 
	
	// Use strings.Builder for efficient string concatenation
	var sb strings.Builder
	for i := 0; i < numLines; i++ {
		sb.WriteString(line)
	}

	_, err = file.WriteString(sb.String())
	if err != nil {
		log.Fatalf("Failed to write to file: %v", err)
	}
	log.Printf("Successfully created dummy file larger than 5MB.")


	// --- 2. Minio Client Configuration ---
	ctx := context.Background()
	endpoint := "127.0.0.1:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false // Use HTTP for local testing

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln("Failed to initialize MinIO client:", err)
	}

	log.Printf("Successfully connected to MinIO at %s", endpoint)

	// --- 3. Check if bucket exists, create if not ---
	bucketName := "my-go-test-bucket"
	location := "us-east-1" // Default location

	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: location})
	if err != nil {
		// Check to see if we already own this bucket.
		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("Bucket '%s' already exists.\n", bucketName)
		} else {
			log.Fatalln("Error checking or creating bucket:", err)
		}
	} else {
		log.Printf("Successfully created bucket '%s'.\n", bucketName)
	}

	// --- 4. Upload the file using FPutObject ---
	// FPutObject automatically handles multipart uploads for large files.
	objectName := fileName
	filePath := "./" + fileName
	contentType := "application/octet-stream"

	log.Printf("Uploading '%s' to bucket '%s'...", objectName, bucketName)

	// Upload the file with FPutObject
	info, err := minioClient.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{
		ContentType: contentType,
		// The PartSize for multipart upload is automatically determined by the library.
		// You can override it here if needed, but it's often best to let the library handle it.
		// PartSize: 5 * 1024 * 1024,
	})
	if err != nil {
		log.Fatalln("Failed to upload file:", err)
	}

	log.Printf("Successfully uploaded '%s' of size %d bytes. ETag: %s\n", objectName, info.Size, info.ETag)
}
