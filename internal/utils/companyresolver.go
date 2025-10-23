package utils

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
)

// CompanyResolver lazily loads company records from Directus for code lookups.
type CompanyResolver struct {
	client     *directus.Client
	collection string
	cache      map[string]string
	warmed     bool
}

const defaultCompanyCollection = "sc_companies"

// NewCompanyResolver constructs a resolver using the provided Directus client.
func NewCompanyResolver(client *directus.Client, collection string) *CompanyResolver {
	if collection == "" {
		collection = strings.TrimSpace(envOrDefault("SC_COMPANY_COLLECTION", defaultCompanyCollection))
	}
	return &CompanyResolver{
		client:     client,
		collection: collection,
		cache:      map[string]string{},
	}
}

// Warmup preloads company codes from Directus.
func (r *CompanyResolver) Warmup(ctx context.Context) error {
	if r.warmed {
		return nil
	}
	const limit = 200
	offset := 0
	for {
		query := map[string]any{
			"fields": []string{"id", "code"},
			"limit":  limit,
			"offset": offset,
		}
		rows, err := r.client.ReadByQuery(ctx, r.collection, query)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			code, _ := row["code"].(string)
			if code == "" {
				continue
			}
			normalized, err := normalizeCompanyCode(code)
			if err != nil {
				Logger().Warn("Skipping company with invalid code", "collection", r.collection, "id", row["id"], "error", err.Error())
				continue
			}
			id := fmt.Sprint(row["id"])
			r.cache[normalized] = id
		}
		if len(rows) < limit {
			break
		}
		offset += limit
	}
	r.warmed = true
	return nil
}

func (r *CompanyResolver) ensureWarm(ctx context.Context) error {
	if r.warmed {
		return nil
	}
	return r.Warmup(ctx)
}

// ResolveID returns the Directus id for a company code or errors when missing.
func (r *CompanyResolver) ResolveID(ctx context.Context, code string) (string, error) {
	id, err := r.LookupID(ctx, code)
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", fmt.Errorf("company with code %s not found in %s", code, r.collection)
	}
	return id, nil
}

// LookupID returns the Directus id for a company code without raising when missing.
func (r *CompanyResolver) LookupID(ctx context.Context, code string) (string, error) {
	if err := r.ensureWarm(ctx); err != nil {
		return "", err
	}
	normalized, err := normalizeCompanyCode(code)
	if err != nil {
		Logger().Warn("Failed to normalise company code during lookup", "collection", r.collection, "code", code, "error", err.Error())
		return "", nil
	}
	return r.cache[normalized], nil
}

func normalizeCompanyCode(code string) (string, error) {
	trimmed := strings.TrimSpace(code)
	if trimmed == "" {
		return "", fmt.Errorf("company code cannot be empty")
	}
	return strings.ToUpper(trimmed), nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
