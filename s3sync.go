package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/minio/minio-go/v7"

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

	// Выполнение копирований
	for _, dst := range copyList {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
	ep := objsync.Endpoint{
		Bucket:    loc.Bucket,
		Prefix:    loc.Prefix,
		Region:    loc.Region,
		Host:      loc.Host,
		PathStyle: loc.PathStyle,
	}
	if loc.Credentials != nil {
		ep.Credentials.AccessKeyId = loc.Credentials.AccessKeyID
		ep.Credentials.SecretAccessKey = loc.Credentials.SecretAccessKey
	}
	return objsync.NewMinioClient(ep)
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
