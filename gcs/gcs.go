//Package gcs presents a simple API for uploading files to Google Cloud Storage.
//Two methods are exposed: Connect and Upload:
// - Connect sets up the connection to GCS and ensures that credentials and
//identification information is correctly set.
// - Upload compresses and writes file to a GCS bucket.
package gcs

import (
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"cloud.google.com/go/storage"
)

type gcsClient struct {
	projectID  string
	bucketName string
	bucket     *storage.BucketHandle
	client     *storage.Client
	ctx        context.Context
}

var singleton *gcsClient
var once sync.Once

//Connect initializes the Google Cloud Storage Client:
// - Credentials and projectID must be set as environment vars per GCS documentation
// - Creates new client based on these settings
// - Program exits if any of above checks fails.
func Connect() {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		fmt.Fprintln(os.Stderr, "GOOGLE_CLOUD_PROJECT environment variable must be set.")
		os.Exit(1)
	}

	gcsCredentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if gcsCredentials == "" {
		fmt.Fprintln(os.Stderr, "GOOGLE_APPLICATION_CREDENTIALS environment variable must be set.")
		os.Exit(1)
	}

	gcs := createClient()
	gcs.projectID = projectID
	client, err := storage.NewClient(singleton.ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to create GCS Client:", err)
		os.Exit(1)
	}
	gcs.client = client

}

//createClient instantiates singleton Google Cloud Storage Client
func createClient() *gcsClient {
	once.Do(func() {
		singleton = &gcsClient{
			ctx: context.Background(),
		}
	})
	return singleton
}

//setBucket sets bucket to pre-existing bucket or creates
//new bucket.
func setBucket(name string) error {
	bucket := singleton.client.Bucket(name)
	_, err := bucket.Attrs(singleton.ctx)
	if err != nil {
		if err == storage.ErrBucketNotExist {
			//Create Bucket
			fmt.Printf("Creating bucket %s\n", name)
			err := bucket.Create(singleton.ctx, singleton.projectID, nil)
			if err != nil {
				fmt.Println("Error creating bucket", err)
				return err
			}
		} else {
			fmt.Println(err)
			return err
		}
	}
	singleton.bucket = bucket
	return nil
}

//Upload writes file to GCS bucket
// - Gzip encodes / compresses the file before sending
// - Sets GCS object property fields content-type and
// content-encoding to 'text/plain' and 'gzip'.
func Upload(bucket string, filename string) error {
	err := setBucket(bucket)
	if err != nil {
		fmt.Println("GCS: Error setting bucket", err)
		return err
	}

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("GCS: Error reading file for upload", err)
		return err
	}

	filename = path.Base(filename)
	ext := path.Ext(filename)
	objectName := filename[0:len(filename)-len(ext)] + ".gzip"
	fmt.Printf("GCS: Object name %s\n", objectName)

	wc := singleton.bucket.Object(objectName).NewWriter(singleton.ctx)
	wc.ContentType = "text/plain"
	wc.ContentEncoding = "gzip"
	fmt.Println("GCS: Successfully created gcs writer")

	zWriter := gzip.NewWriter(wc)
	nBytes, err := zWriter.Write(data)
	if err != nil {
		fmt.Println("GCS: Error compressing file", err)
		return err
	}
	zWriter.Close()
	fmt.Printf("GCS: Wrote %d bytes\n", nBytes)

	if err := wc.Close(); err != nil {
		fmt.Println("GCS: Error on context writer close", err)
		return err
	}

	return nil
}
