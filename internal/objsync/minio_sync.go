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

	// Stream upload with unknown size (-1); content-type оставляем по умолчанию.
	if _, err := dst.PutObject(ctx, dstBucket, dstKey, reader, -1, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("put dst %s: %w", dstKey, err)
	}
	return nil
}

// RemoveObject deletes a single object.
func RemoveObject(ctx context.Context, c *minio.Client, bucket, key string) error {
	return c.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
}

// EnsureBucket ensures bucket exists; creates it if missing (idempotent).
func EnsureBucket(ctx context.Context, c *minio.Client, bucket, region string) error {
	exists, err := c.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region})
}

// Sync performs a one-shot sync from src to dst using MinIO clients and options provided.
func Sync(ctx context.Context, srcEp, dstEp Endpoint, opt Options) error {
	srcClient, err := NewMinioClient(srcEp)
	if err != nil {
		return fmt.Errorf("init src client: %w", err)
	}
	dstClient, err := NewMinioClient(dstEp)
	if err != nil {
		return fmt.Errorf("init dst client: %w", err)
	}

	// Ensure destination bucket exists.
	if err := EnsureBucket(ctx, dstClient, dstEp.Bucket, dstEp.Region); err != nil {
		return fmt.Errorf("ensure dst bucket: %w", err)
	}

	srcObjs, err := ListAllObjects(ctx, srcClient, srcEp.Bucket, srcEp.Prefix)
	if err != nil {
		return fmt.Errorf("list src: %w", err)
	}
	dstObjs, err := ListAllObjects(ctx, dstClient, dstEp.Bucket, dstEp.Prefix)
	if err != nil {
		return fmt.Errorf("list dst: %w", err)
	}

	type info = ObjectInfo
	rel := func(prefix, key string) string {
		if prefix == "" {
			return key
		}
		return strings.TrimPrefix(key, prefix)
	}
	join := func(prefix, key string) string {
		if prefix == "" {
			return key
		}
		if key == "" {
			return prefix
		}
		if strings.HasSuffix(prefix, "/") || strings.HasPrefix(key, "/") {
			return prefix + key
		}
		return prefix + "/" + key
	}

	srcMap := make(map[string]info, len(srcObjs))
	for _, o := range srcObjs {
		srcMap[rel(srcEp.Prefix, o.Key)] = o
	}
	dstMap := make(map[string]info, len(dstObjs))
	for _, o := range dstObjs {
		dstMap[rel(dstEp.Prefix, o.Key)] = o
	}

	// Upload objects that are missing by name in destination.
	for rkey, so := range srcMap {
		_, exists := dstMap[rkey]
		needCopy := !exists
		if needCopy {
			srcKey := so.Key
			dstKey := join(dstEp.Prefix, rkey)
			if err := CopyObject(ctx, srcClient, dstClient, srcEp.Bucket, dstEp.Bucket, srcKey, dstKey); err != nil {
				return fmt.Errorf("copy %s -> %s: %w", srcKey, dstKey, err)
			}
		}
	}

	// Optionally delete extras in destination.
	if opt.DeleteExtra {
		for rkey, do := range dstMap {
			if _, ok := srcMap[rkey]; !ok {
				if err := RemoveObject(ctx, dstClient, dstEp.Bucket, do.Key); err != nil {
					return fmt.Errorf("delete extra %s: %w", do.Key, err)
				}
			}
		}
	}

	return nil
}
