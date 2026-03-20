package objsync

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Credentials struct {
	AccessKeyId     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type Endpoint struct {
	Bucket      string      `json:"bucket"`
	Prefix      string      `json:"prefix"`
	Region      string      `json:"region"`
	Host        string      `json:"host"`
	PathStyle   bool        `json:"pathStyle"`
	Credentials Credentials `json:"credentials"`
}

type Options struct {
	DeleteExtra          bool `json:"deleteExtra"`
	OverwriteOnNameMatch bool `json:"overwriteOnNameMatch"`
}

type ObjectInfo struct {
	Key  string
	Size int64
}

// NewMinioClient creates a MinIO client from Endpoint settings.
func NewMinioClient(ep Endpoint) (*minio.Client, error) {
	endpoint, secure, err := parseEndpoint(ep.Host)
	if err != nil {
		return nil, err
	}
	opts := &minio.Options{
		Secure:       secure,
		Region:       ep.Region,
		BucketLookup: minio.BucketLookupAuto,
	}
	ak := strings.TrimSpace(ep.Credentials.AccessKeyId)
	sk := strings.TrimSpace(ep.Credentials.SecretAccessKey)

	if ak != "" && sk != "" {
		opts.Creds = credentials.NewStaticV4(ak, sk, "")
	} else {
		// Anonymous access: leave credentials unset (nil) to perform unsigned requests.
		opts.Creds = nil
	}

	if ep.PathStyle {
		opts.BucketLookup = minio.BucketLookupPath
	}
	return minio.New(endpoint, opts)
}

func parseEndpoint(host string) (endpoint string, secure bool, err error) {
	h := strings.TrimSpace(host)
	if h == "" {
		return "", false, errors.New("empty endpoint host")
	}
	// Accept both raw host[:port] and full URL.
	if strings.HasPrefix(h, "http://") || strings.HasPrefix(h, "https://") {
		u, perr := url.Parse(h)
		if perr != nil {
			return "", false, fmt.Errorf("invalid endpoint url: %w", perr)
		}
		return u.Host, u.Scheme == "https", nil
	}
	// Default to HTTPS if no scheme provided.
	return strings.TrimPrefix(strings.TrimPrefix(h, "://"), "//"), true, nil
}

// ListAllObjects lists objects under bucket/prefix recursively and returns them with sizes.
func ListAllObjects(ctx context.Context, c *minio.Client, bucket, prefix string) ([]ObjectInfo, error) {
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}
	ch := c.ListObjects(ctx, bucket, opts)
	var out []ObjectInfo
	for obj := range ch {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, ObjectInfo{Key: obj.Key, Size: obj.Size})
	}
	return out, nil
}

// CopyObject streams a single object from source to destination without StatObject.
func CopyObject(ctx context.Context, src *minio.Client, dst *minio.Client, srcBucket, dstBucket, srcKey, dstKey string) error {
	reader, err := src.GetObject(ctx, srcBucket, srcKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get src %s: %w", srcKey, err)
	}
	defer reader.Close()

	// Get object metadata
	objInfo, err := reader.Stat()
	if err != nil {
		return fmt.Errorf("stat object %s: %w", srcKey, err)
	}

	// Prepare options with preserved metadata
	opts := minio.PutObjectOptions{
		ContentType:  objInfo.ContentType,
		UserMetadata: objInfo.UserMetadata,
		StorageClass: objInfo.StorageClass,
	}

	// Stream upload with unknown size (-1); content-type оставляем по умолчанию.
	if _, err := dst.PutObject(ctx, dstBucket, dstKey, reader, -1, opts); err != nil {
		return fmt.Errorf("put dst %s: %w", dstKey, err)
	}
	return nil
}

// RemoveObject deletes a single object.
func RemoveObject(ctx context.Context, c *minio.Client, bucket, key string) error {
	return c.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
}
