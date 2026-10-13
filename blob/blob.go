// Package blob is the durable file-bytes backend. Per decision it supports ONLY
// S3-compatible object storage (no NATS object store). Metadata lives in the
// store's `agent_files` table; this holds the bytes.
package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Config configures the S3-compatible backend.
type S3Config struct {
	Bucket          string
	Region          string
	Endpoint        string // custom endpoint (MinIO/OSS); empty = AWS
	AccessKeyID     string
	SecretAccessKey string
	ForcePathStyle  bool
	Prefix          string
}

// Store is the S3 object store.
type Store struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3 builds an S3-compatible store.
func NewS3(ctx context.Context, cfg S3Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("blob: bucket is required")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("blob: aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	return &Store{client: client, bucket: cfg.Bucket, prefix: strings.TrimRight(cfg.Prefix, "/")}, nil
}

func (s *Store) key(name string) string {
	if s.prefix == "" {
		return name
	}
	return s.prefix + "/" + name
}

// Put stores bytes under name.
func (s *Store) Put(ctx context.Context, name string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(name)),
		Body: bytes.NewReader(data),
	})
	return err
}

// Get reads bytes; returns (nil, false) when the object is absent.
func (s *Store) Get(ctx context.Context, name string) ([]byte, bool, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(name)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Delete removes an object.
func (s *Store) Delete(ctx context.Context, name string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(name)),
	})
	return err
}

func isNotFound(err error) bool {
	// The SDK returns a typed smithy error; a NoSuchKey/404 string check is a
	// robust fallback across S3-compatible servers.
	return strings.Contains(err.Error(), "NoSuchKey") ||
		strings.Contains(err.Error(), "NotFound") ||
		strings.Contains(err.Error(), "404")
}
