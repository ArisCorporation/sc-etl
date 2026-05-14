package load

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

func TestSyncShipVariantsClearsStaleRSIThumbnailWhenRSIRefDisappears(t *testing.T) {
	existingVariants := []map[string]any{
		{
			"id":           "variant-1",
			"hull":         map[string]any{"id": "hull-gladius"},
			"name":         "Gladius Pirate",
			"variant_code": "PIRATE",
			"external_refs": []any{
				map[string]any{"source": "SC_DATA", "id": "AEG_GLADIUS_PIRATE"},
				map[string]any{"source": "RSI", "id": "60"},
			},
			"stats": map[string]any{},
			"thumbnail": map[string]any{
				"id":          "file-old",
				"description": "RSI matrix source: https://robertsspaceindustries.com/media/gladius-base.jpg",
			},
		},
	}

	var patchBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/items/ship_variants":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": existingVariants})
		case r.Method == http.MethodPatch && r.URL.Path == "/items/ship_variants/variant-1":
			if err := json.NewDecoder(r.Body).Decode(&patchBody); err != nil {
				t.Fatalf("decode patch body: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "variant-1"}})
		case r.URL.Path == "/versions" || len(r.URL.Path) > 9 && r.URL.Path[:9] == "/versions":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "ver-1"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := directus.NewClient(server.URL, "test-token")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.HTTPClient = server.Client()

	b := &builder{
		ctx:      context.Background(),
		client:   client,
		rsiMedia: map[string]rsiMedia{"188": {Thumbnail: "https://robertsspaceindustries.com/media/gladius-pirate.jpg"}},
		collections: collections{
			ShipVariants: "ship_variants",
		},
	}

	hullIDs := map[string]string{
		"AEG_GLADIUS": "hull-gladius",
	}
	variantCode := "PIRATE"
	variants := []model.NormalizedShipVariantV2{
		{
			ExternalID:   "AEG_GLADIUS_PIRATE",
			ShipExternal: "AEG_GLADIUS",
			Name:         "Gladius Pirate",
			VariantCode:  &variantCode,
			ExternalRefs: []model.NormalizedExternalReference{
				{Source: "SC_DATA", ID: "AEG_GLADIUS_PIRATE"},
			},
		},
	}

	if _, err := b.syncShipVariants(variants, nil, hullIDs, "VTEST", false); err != nil {
		t.Fatalf("sync ship variants: %v", err)
	}
	if patchBody == nil {
		t.Fatal("expected update payload")
	}
	if thumbnail, exists := patchBody["thumbnail"]; !exists || thumbnail != nil {
		t.Fatalf("expected thumbnail to be cleared, got %#v", patchBody["thumbnail"])
	}
	refs, ok := patchBody["external_refs"].([]any)
	if !ok {
		t.Fatalf("expected external_refs array, got %#v", patchBody["external_refs"])
	}
	if len(refs) != 1 {
		t.Fatalf("expected exactly one external ref after cleanup, got %#v", refs)
	}
	ref, ok := refs[0].(map[string]any)
	if !ok {
		t.Fatalf("expected external ref object, got %#v", refs[0])
	}
	if ref["source"] != "SC_DATA" || ref["id"] != "AEG_GLADIUS_PIRATE" {
		t.Fatalf("unexpected external ref payload: %#v", ref)
	}
}
