package diff

import (
	"context"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
)

// ChangeType categorises the type of diff.
type ChangeType string

const (
	ChangeCreated ChangeType = "created"
	ChangeUpdated ChangeType = "updated"
	ChangeDeleted ChangeType = "deleted"
)

// Entry captures a single diff record.
type Entry struct {
	EntityType string
	EntityID   string
	ChangeType ChangeType
	Diff       map[string]any
}

// Writer persists diff entries to Directus in chunks.
type Writer struct {
	client     *directus.Client
	collection string
	chunkSize  int
	skip       bool
	entries    []Entry
}

// Options configure the diff writer.
type Options struct {
	Collection string
	ChunkSize  int
	Skip       bool
}

// NewWriter constructs a diff writer.
func NewWriter(client *directus.Client, opts Options) *Writer {
	collection := opts.Collection
	if collection == "" {
		if env := strings.TrimSpace(os.Getenv("SC_DIFF_COLLECTION")); env != "" {
			collection = env
		} else {
			collection = "diffs"
		}
	}
	chunkSize := opts.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 100
	}
	return &Writer{
		client:     client,
		collection: collection,
		chunkSize:  chunkSize,
		skip:       opts.Skip,
	}
}

// AddChange enqueues a diff entry. Returns true when the diff is stored.
func (w *Writer) AddChange(entry Entry) bool {
	if w.skip {
		return false
	}
	if entry.Diff == nil || len(entry.Diff) == 0 {
		return false
	}
	w.entries = append(w.entries, entry)
	return true
}

// Flush writes accumulated entries to Directus.
func (w *Writer) Flush(ctx context.Context, buildID string) error {
	if w.skip || len(w.entries) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for len(w.entries) > 0 {
		batchSize := w.chunkSize
		if batchSize > len(w.entries) {
			batchSize = len(w.entries)
		}
		batch := w.entries[:batchSize]
		payload := make([]map[string]any, 0, len(batch))
		for _, entry := range batch {
			payload = append(payload, map[string]any{
				"build":        buildID,
				"entity_type":  entry.EntityType,
				"entity_id":    entry.EntityID,
				"change_type":  string(entry.ChangeType),
				"diff":         entry.Diff,
				"date_created": now,
			})
		}
		if _, err := w.client.CreateMany(ctx, w.collection, payload); err != nil {
			return err
		}
		w.entries = w.entries[batchSize:]
	}
	return nil
}

// Compute returns a diff payload for the specified fields.
func Compute(before map[string]any, after map[string]any, fields []string) map[string]any {
	if before == nil && after == nil {
		return nil
	}
	if before == nil {
		return map[string]any{
			"after": pickFields(after, fields),
		}
	}
	if after == nil {
		return map[string]any{
			"before": pickFields(before, fields),
		}
	}
	changes := map[string]map[string]any{}
	for _, field := range fields {
		prev, okPrev := before[field]
		next, okNext := after[field]
		if !okPrev && !okNext {
			continue
		}
		if !reflect.DeepEqual(prev, next) {
			changes[field] = map[string]any{
				"before": valueOrNull(prev),
				"after":  valueOrNull(next),
			}
		}
	}
	if len(changes) == 0 {
		return nil
	}
	result := map[string]any{}
	for field, value := range changes {
		result[field] = value
	}
	return result
}

func pickFields(source map[string]any, fields []string) map[string]any {
	if source == nil {
		return map[string]any{}
	}
	result := map[string]any{}
	for _, field := range fields {
		if value, ok := source[field]; ok {
			result[field] = value
		}
	}
	return result
}

func valueOrNull(value any) any {
	if value == nil {
		return nil
	}
	return value
}
