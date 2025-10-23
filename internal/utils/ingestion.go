package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
)

// IngestionRun mirrors the Directus tracking entity used by the TypeScript pipeline.
type IngestionRun struct {
	client  *directus.Client
	runID   string
	stats   map[string]any
	started bool
}

// NewIngestionRun attaches a Directus client.
func NewIngestionRun(client *directus.Client) *IngestionRun {
	return &IngestionRun{
		client: client,
		stats:  map[string]any{},
	}
}

const ingestionCollection = "ingestion_runs"

func isoNow() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// Start creates the ingestion run entry.
func (r *IngestionRun) Start(ctx context.Context, buildID string, extra map[string]any) error {
	if r.started {
		return nil
	}
	payload := map[string]any{
		"build":      buildID,
		"state":      "running",
		"stats_json": map[string]any{},
		"log":        nil,
		"started_at": isoNow(),
	}
	for k, v := range extra {
		payload[k] = v
	}
	created, err := r.client.CreateOne(ctx, ingestionCollection, payload)
	if err != nil {
		return err
	}
	id := normalizeIngestionID(created["id"])
	if id == "" {
		return err
	}
	r.runID = id
	r.started = true
	r.stats = map[string]any{}
	Logger().Info("Ingestion run started", "ingestion_run_id", id, "build_id", buildID)
	return nil
}

func normalizeIngestionID(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	case nil:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

// UpdateStats merges incremental statistics.
func (r *IngestionRun) UpdateStats(ctx context.Context, partial map[string]any) error {
	if !r.started || r.runID == "" {
		return nil
	}
	for k, v := range partial {
		r.stats[k] = v
	}
	_, err := r.client.UpdateOne(ctx, ingestionCollection, r.runID, map[string]any{
		"stats_json": r.stats,
	})
	return err
}

// FinishSuccess marks the run as finished successfully.
func (r *IngestionRun) FinishSuccess(ctx context.Context) error {
	if !r.started || r.runID == "" {
		return nil
	}
	_, err := r.client.UpdateOne(ctx, ingestionCollection, r.runID, map[string]any{
		"state":       "success",
		"stats_json":  r.stats,
		"finished_at": isoNow(),
	})
	if err == nil {
		Logger().Info("Ingestion run finished successfully", "ingestion_run_id", r.runID)
	}
	return err
}

// FinishFail marks the run as failed and records the error payload.
func (r *IngestionRun) FinishFail(ctx context.Context, failure error) error {
	if !r.started || r.runID == "" {
		return nil
	}
	logPayload := ""
	if failure != nil {
		logPayload = failure.Error()
	}
	_, err := r.client.UpdateOne(ctx, ingestionCollection, r.runID, map[string]any{
		"state":       "failed",
		"stats_json":  r.stats,
		"log":         logPayload,
		"finished_at": isoNow(),
	})
	if err == nil {
		args := []any{"ingestion_run_id", r.runID}
		if failure != nil {
			args = append(args, "error", failure)
		}
		Logger().Error("Ingestion run failed", args...)
	}
	return err
}
