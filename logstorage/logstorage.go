package logstorage

import (
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type LogStorage struct {
	s3     *s3.S3
	bucket string
}

type Config struct {
	BucketName   string `toml:"bucket_name"`
	BucketRegion string `toml:"bucket_region"`
	AwsKey       string `toml:"aws_key"`
	AwsSecret    string `toml:"aws_secret"`
}

func NewLogStorage(config *Config) *LogStorage {
	cli := newS3Client(config.AwsKey, config.AwsSecret, config.BucketRegion)
	return &LogStorage{
		s3:     cli,
		bucket: config.BucketName,
	}
}

type AwsFile struct {
	Name   string
	Size   int64
	Day    string
	Bucket int
}

func (ls *LogStorage) List(day string, bucket int) ([]AwsFile, error) {

	params := &s3.ListObjectsInput{
		Bucket: aws.String(ls.bucket),
		Prefix: aws.String(fmt.Sprintf("pixel/dt=%s/part=%d/", day, bucket)),
	}

	files := make([]AwsFile, 0, 16)
	err := ls.s3.ListObjectsPages(params,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, key := range page.Contents {
				files = append(files, AwsFile{Name: *key.Key, Size: *key.Size, Day: day, Bucket: bucket})
			}
			return true
		})
	return files, err
}

func (ls *LogStorage) Get(path string) (io.ReadCloser, error) {
	objectInput := &s3.GetObjectInput{
		Bucket: aws.String(ls.bucket),
		Key:    aws.String(path),
	}
	in, err := ls.s3.GetObject(objectInput)
	if err != nil {
		return nil, err
	}
	return in.Body, nil
}

func newS3Client(key, secretKey, region string) *s3.S3 {
	sess, err := session.NewSession()
	if err != nil {
		log.Printf("ERROR Failed to create S3 client: %v", err)
		return nil
	}
	cred := credentials.NewStaticCredentials(key, secretKey, "")
	return s3.New(sess, aws.NewConfig().WithCredentials(cred).WithRegion(region))
}
