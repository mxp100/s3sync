package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"s3sync/internal/objsync"
)

type objectInfo struct {
	Key  string
	Size int64
}

// Syncer выполняет одну задачу синхронизации на базе MinIO.
type Syncer struct {
	idx int
	job SyncJob
	ui  *ProgressUI
}

// NewSyncer создает новый синхронизатор.
func NewSyncer(idx int, job SyncJob, ui *ProgressUI) *Syncer {
	return &Syncer{idx: idx, job: job, ui: ui}
}

// Run выполняет синхронизацию для данной задачи.
func (s *Syncer) Run(ctx context.Context) error {
	s.ui.SetName(s.idx, s.job.Name)
	s.ui.SetStatus(s.idx, "initializing clients...")

	srcCli, err := newMinioClient(s.job.Source)
	if err != nil {
		s.ui.SetError(s.idx, err)
		return fmt.Errorf("init source client: %w", err)
	}
	dstCli, err := newMinioClient(s.job.Destination)
	if err != nil {
		s.ui.SetError(s.idx, err)
		return fmt.Errorf("init destination client: %w", err)
	}

	s.ui.SetStatus(s.idx, "listing source...")
	srcObjects, err := listAllObjects(ctx, srcCli, s.job.Source.Bucket, s.job.Source.Prefix)
	if err != nil {
		s.ui.SetError(s.idx, err)
		return fmt.Errorf("list source: %w", err)
	}

	s.ui.SetStatus(s.idx, "listing destination...")
	dstObjects, err := listAllObjects(ctx, dstCli, s.job.Destination.Bucket, s.job.Destination.Prefix)
	if err != nil {
		s.ui.SetError(s.idx, err)
		return fmt.Errorf("list destination: %w", err)
	}

	// План: какие объекты копировать и какие удалять
	copyList := make([]objectInfo, 0, len(srcObjects))
	neededDest := make(map[string]struct{}, len(srcObjects))
	overwrite := s.job.Options.OverwriteOnNameMatch

	for _, src := range srcObjects {
		rel := strings.TrimPrefix(src.Key, s.job.Source.Prefix)
		destKey := joinKeys(s.job.Destination.Prefix, rel)
		neededDest[destKey] = struct{}{}

		// Если overwrite включен — копируем всегда, иначе — только если объекта нет в destination.
		if !overwrite {
			if _, ok := dstObjects[destKey]; ok {
				continue
			}
		}
		copyList = append(copyList, objectInfo{Key: destKey, Size: src.Size})
	}

	deleteList := make([]string, 0)
	if s.job.Options.DeleteExtra {
		for dKey := range dstObjects {
			if _, ok := neededDest[dKey]; !ok {
				deleteList = append(deleteList, dKey)
			}
		}
	}

	totalOps := len(copyList) + len(deleteList)
	s.ui.SetPlanTotal(s.idx, totalOps)
	if totalOps == 0 {
		// Нечего делать — уже помечено как completed
		return nil
	}

	// Выполнение копирований параллельно через пул воркеров
	workers := s.job.Options.CopyWorkers
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers < 1 {
			workers = 1
		}
	}
	jobsCh := make(chan objectInfo)
	var wgCopies sync.WaitGroup

	for i := 0; i < workers; i++ {
		wgCopies.Add(1)
		go func() {
			defer wgCopies.Done()
			for dst := range jobsCh {
				select {
				case <-ctx.Done():
					return
				default:
				}
				rel := strings.TrimPrefix(dst.Key, s.job.Destination.Prefix)
				srcKey := joinKeys(s.job.Source.Prefix, rel)

				s.ui.SetStatus(s.idx, fmt.Sprintf("copy: %s", shortKey(dst.Key)))
				if err := copyObject(ctx, srcCli, dstCli, s.job.Source.Bucket, srcKey, s.job.Destination.Bucket, dst.Key); err != nil {
					s.ui.SetError(s.idx, err)
					log.Printf("[%s] copy error %s -> %s: %v", s.job.Name, srcKey, dst.Key, err)
				}
				s.ui.IncDone(s.idx)
			}
		}()
	}

	// Отправляем задачи копирования воркерам
	go func() {
		defer close(jobsCh)
		for _, dst := range copyList {
			select {
			case <-ctx.Done():
				return
			case jobsCh <- dst:
			}
		}
	}()

	// Ждем завершения копирований
	wgCopies.Wait()

	// Выполнение удалений
	if len(deleteList) > 0 {
		for _, key := range deleteList {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			s.ui.SetStatus(s.idx, fmt.Sprintf("delete: %s", shortKey(key)))
			if err := deleteObject(ctx, dstCli, s.job.Destination.Bucket, key); err != nil {
				s.ui.SetError(s.idx, err)
				log.Printf("[%s] delete error %s: %v", s.job.Name, key, err)
			}
			s.ui.IncDone(s.idx)
		}
	}

	s.ui.SetStatus(s.idx, "completed")
	return nil
}

func newMinioClient(loc Location) (*minio.Client, error) {
	// Derive endpoint and scheme from Host. If scheme is omitted, default to https.
	endpoint := loc.Host
	secure := true
	if strings.Contains(loc.Host, "://") {
		u, err := url.Parse(loc.Host)
		if err != nil {
			return nil, fmt.Errorf("parse host %q: %w", loc.Host, err)
		}
		endpoint = u.Host
		switch strings.ToLower(u.Scheme) {
		case "http":
			secure = false
		case "https":
			secure = true
		}
	}

	// Prepare transport and optionally allow self-signed certs.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if transport == nil {
		transport = &http.Transport{}
	}
	if loc.AllowSelfSigned {
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		} else {
			transport.TLSClientConfig.InsecureSkipVerify = true
		}
	}

	opts := &minio.Options{
		Secure:       secure,
		Transport:    transport,
		Region:       loc.Region,
		BucketLookup: minio.BucketLookupDNS,
	}
	if loc.PathStyle {
		opts.BucketLookup = minio.BucketLookupPath
	}
	if loc.Credentials != nil && loc.Credentials.AccessKeyID != "" {
		opts.Creds = credentials.NewStaticV4(loc.Credentials.AccessKeyID, loc.Credentials.SecretAccessKey, "")
	}

	return minio.New(endpoint, opts)
}

func listAllObjects(ctx context.Context, cli *minio.Client, bucket, prefix string) (map[string]objectInfo, error) {
	items, err := objsync.ListAllObjects(ctx, cli, bucket, prefix)
	if err != nil {
		return nil, err
	}
	out := make(map[string]objectInfo, len(items))
	for _, it := range items {
		// Пропустим "псевдопапки" если вдруг такие встретятся (обычно minio-go их не возвращает)
		if strings.HasSuffix(it.Key, "/") && it.Size == 0 {
			continue
		}
		out[it.Key] = objectInfo{Key: it.Key, Size: it.Size}
	}
	return out, nil
}

func copyObject(ctx context.Context, srcCli, dstCli *minio.Client, srcBucket, srcKey, dstBucket, dstKey string) error {
	return objsync.CopyObject(ctx, srcCli, dstCli, srcBucket, dstBucket, srcKey, dstKey)
}

func deleteObject(ctx context.Context, cli *minio.Client, bucket, key string) error {
	return objsync.RemoveObject(ctx, cli, bucket, key)
}

func joinKeys(prefix, rel string) string {
	if prefix == "" {
		return strings.TrimPrefix(rel, "/")
	}
	p := strings.TrimSuffix(prefix, "/")
	r := strings.TrimPrefix(rel, "/")
	return p + "/" + r
}

func shortKey(key string) string {
	if len(key) <= 48 {
		return key
	}
	return "..." + key[len(key)-45:]
}
