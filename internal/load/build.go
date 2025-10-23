package load

import (
	"context"
	"time"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

func ensureBuild(ctx context.Context, client *directus.Client, channel model.Channel, version string, metadata buildMetadata) (buildRecord, error) {
	filter := map[string]any{
		"channel":      map[string]any{"_eq": channel},
		"game_version": map[string]any{"_eq": version},
	}
	rows, err := client.ReadByQuery(ctx, "builds", map[string]any{
		"filter": filter,
		"limit":  1,
		"fields": []string{"id", "status", "build_hash", "released_at"},
	})
	if err != nil {
		return buildRecord{}, err
	}
	if len(rows) > 0 {
		row := rows[0]
		record := buildRecord{
			ID:         toString(row["id"]),
			Status:     normalizeString(row["status"]),
			Hash:       normalizeString(row["build_hash"]),
			ReleasedAt: normalizeString(row["released_at"]),
		}
		patch := map[string]any{}
		if record.Status != "pending" {
			patch["status"] = "pending"
			record.Status = "pending"
		}
		if metadata.Hash != "" && metadata.Hash != record.Hash {
			patch["build_hash"] = metadata.Hash
			record.Hash = metadata.Hash
		}
		if metadata.ReleasedAt != "" && metadata.ReleasedAt != record.ReleasedAt {
			patch["released_at"] = metadata.ReleasedAt
			record.ReleasedAt = metadata.ReleasedAt
		}
		if len(patch) > 0 {
			if _, err := client.UpdateOne(ctx, "builds", record.ID, patch); err != nil {
				return buildRecord{}, err
			}
		}
		return record, nil
	}
	payload := map[string]any{
		"channel":      channel,
		"game_version": version,
		"status":       "pending",
		"build_hash":   nil,
		"released_at":  nil,
		"ingested":     nil,
	}
	if metadata.Hash != "" {
		payload["build_hash"] = metadata.Hash
	}
	if metadata.ReleasedAt != "" {
		payload["released_at"] = metadata.ReleasedAt
	}
	created, err := client.CreateOne(ctx, "builds", payload)
	if err != nil {
		return buildRecord{}, err
	}
	return buildRecord{
		ID:         toString(created["id"]),
		Status:     "pending",
		Hash:       metadata.Hash,
		ReleasedAt: metadata.ReleasedAt,
	}, nil
}

func markBuildIngested(ctx context.Context, client *directus.Client, buildID string) error {
	payload := map[string]any{
		"status":   "ingested",
		"ingested": time.Now().UTC().Format(time.RFC3339Nano),
	}
	_, err := client.UpdateOne(ctx, "builds", buildID, payload)
	return err
}
