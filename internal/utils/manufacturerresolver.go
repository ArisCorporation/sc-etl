package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
)

// ManufacturerResolver resolves manufacturer IDs from Directus and creates missing ones.
type ManufacturerResolver struct {
	client *directus.Client
	cache  map[string]manufacturerRow
	warmed bool
}

type manufacturerRow struct {
	ID      string
	Code    string
	Name    string
	Content string
}

const manufacturerCollection = "companies"

// NewManufacturerResolver constructs a resolver backed by Directus.
func NewManufacturerResolver(client *directus.Client) *ManufacturerResolver {
	return &ManufacturerResolver{
		client: client,
		cache:  map[string]manufacturerRow{},
	}
}

// Warmup preloads existing manufacturer rows.
func (r *ManufacturerResolver) Warmup(ctx context.Context) error {
	if r.warmed {
		return nil
	}
	const limit = 200
	offset := 0
	for {
		query := map[string]any{
			"fields": []string{"id", "code", "slug", "name", "content"},
			"limit":  limit,
			"offset": offset,
		}
		rows, err := r.client.ReadByQuery(ctx, manufacturerCollection, query)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			code := firstString(row["code"])
			if code == "" {
				code = firstString(row["slug"])
			}
			if code == "" {
				continue
			}
			normalized, err := normalizeManufacturerCode(code)
			if err != nil {
				Logger().Warn("Skipping manufacturer with invalid code", "id", row["id"], "error", err.Error())
				continue
			}
			r.cache[normalized] = manufacturerRow{
				ID:      fmt.Sprint(row["id"]),
				Code:    normalized,
				Name:    firstString(row["name"]),
				Content: firstString(row["content"]),
			}
		}
		if len(rows) < limit {
			break
		}
		offset += limit
	}
	r.warmed = true
	return nil
}

func (r *ManufacturerResolver) ensureWarm(ctx context.Context) error {
	if r.warmed {
		return nil
	}
	return r.Warmup(ctx)
}

// ResolveID returns an existing manufacturer id or creates one.
func (r *ManufacturerResolver) ResolveID(ctx context.Context, code string, details map[string]string) (string, error) {
	if err := r.ensureWarm(ctx); err != nil {
		return "", err
	}
	normalized, err := normalizeManufacturerCode(code)
	if err != nil {
		return "", err
	}
	if existing, ok := r.cache[normalized]; ok {
		patch := map[string]any{}
		if name := details["name"]; name != "" && name != existing.Name {
			patch["name"] = name
			existing.Name = name
		}
		if description := details["description"]; description != "" && description != existing.Content {
			patch["content"] = description
			existing.Content = description
		}
		if len(patch) > 0 {
			if _, err := r.client.UpdateOne(ctx, manufacturerCollection, existing.ID, patch); err != nil {
				return "", err
			}
			r.cache[normalized] = existing
		}
		return existing.ID, nil
	}

	payload := map[string]any{
		"code":    normalized,
		"name":    defaultString(details["name"], normalized),
		"content": defaultString(details["description"], ""),
		"status":  "published",
	}
	created, err := r.client.CreateOne(ctx, manufacturerCollection, payload)
	if err != nil {
		return "", err
	}
	id := fmt.Sprint(created["id"])
	r.cache[normalized] = manufacturerRow{
		ID:      id,
		Code:    normalized,
		Name:    fmt.Sprint(created["name"]),
		Content: fmt.Sprint(created["content"]),
	}
	Logger().Info("Created missing manufacturer", "code", normalized, "id", id)
	return id, nil
}

func normalizeManufacturerCode(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("manufacturer code cannot be empty")
	}
	return strings.ToUpper(trimmed), nil
}

func firstString(values ...any) string {
	for _, value := range values {
		if s, ok := value.(string); ok {
			s = strings.TrimSpace(s)
			if s != "" {
				return s
			}
		}
	}
	return ""
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
