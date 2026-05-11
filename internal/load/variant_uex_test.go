package load

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

func TestBuildVariantRefKeysExcludesUEX(t *testing.T) {
	refs := []model.NormalizedExternalReference{
		{Source: "SC_DATA", ID: "DRAK_CUTTER_BASE"},
		{Source: "UEX", ID: "57"},
		{Source: "RSI", ID: "242"},
	}
	keys := buildVariantRefKeys(refs)
	for _, k := range keys {
		if k == "UEX:57" {
			t.Error("buildVariantRefKeys must not include UEX refs")
		}
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 keys (SC_DATA + RSI), got %d: %v", len(keys), keys)
	}
}

// TestSyncShipVariantsUEXCollision verifies that variants sharing the same
// UEX external ref (vehicle-level ID) are not conflated: each distinct
// variant (by hull + variant_code) must be created independently.
func TestSyncShipVariantsUEXCollision(t *testing.T) {
	// Existing Directus state: one variant already loaded with UEX id "57".
	existingVariants := []map[string]any{
		{
			"id":           "variant-scout",
			"hull":         map[string]any{"id": "hull-cutter"},
			"name":         "Cutter Scout",
			"variant_code": "SCOUT",
			"external_refs": []any{
				map[string]any{"source": "SC_DATA", "id": "DRAK_CUTTER_SCOUT"},
				map[string]any{"source": "UEX", "id": "57"},
			},
			"stats":     map[string]any{},
			"thumbnail": nil,
		},
	}

	var createCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/items/ship_variants":
			data, _ := json.Marshal(map[string]any{"data": existingVariants})
			_, _ = w.Write(data)
		case r.Method == http.MethodPost && r.URL.Path == "/items/ship_variants":
			createCount.Add(1)
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			name, _ := body["name"].(string)
			_, _ = w.Write([]byte(`{"data":{"id":"new-` + name + `"}}`))
		case r.URL.Path == "/versions" || len(r.URL.Path) > 9 && r.URL.Path[:9] == "/versions":
			// Accept all version-related calls silently.
			_, _ = w.Write([]byte(`{"data":{"id":"ver-1"}}`))
		default:
			t.Logf("unexpected request: %s %s", r.Method, r.URL.Path)
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
		rsiMedia: map[string]rsiMedia{},
		collections: collections{
			ShipVariants: "ship_variants",
		},
	}

	hullIDs := map[string]string{
		"DRAK_CUTTER": "hull-cutter",
	}

	uexID := "57"
	newVariants := []model.NormalizedShipVariantV2{
		{
			ExternalID:   "DRAK_CUTTER_BASE",
			ShipExternal: "DRAK_CUTTER",
			Name:         "Cutter",
			VariantCode:  strPtr("BASE"),
			ExternalRefs: []model.NormalizedExternalReference{
				{Source: "SC_DATA", ID: "DRAK_CUTTER_BASE"},
				{Source: "UEX", ID: uexID},
			},
		},
		{
			ExternalID:   "DRAK_CUTTER_RAMBLER",
			ShipExternal: "DRAK_CUTTER",
			Name:         "Cutter Rambler",
			VariantCode:  strPtr("RAMBLER"),
			ExternalRefs: []model.NormalizedExternalReference{
				{Source: "SC_DATA", ID: "DRAK_CUTTER_RAMBLER"},
				{Source: "UEX", ID: uexID},
			},
		},
	}

	ids, err := b.syncShipVariants(newVariants, nil, hullIDs, "VTEST", false)
	if err != nil {
		t.Fatalf("sync ship variants: %v", err)
	}

	// Both new variants must be created, not silently merged into the existing Scout.
	if got := createCount.Load(); got != 2 {
		t.Errorf("expected 2 create calls, got %d", got)
	}
	if _, ok := ids["DRAK_CUTTER_BASE"]; !ok {
		t.Error("DRAK_CUTTER_BASE missing from returned IDs")
	}
	if _, ok := ids["DRAK_CUTTER_RAMBLER"]; !ok {
		t.Error("DRAK_CUTTER_RAMBLER missing from returned IDs")
	}
}

func strPtr(s string) *string { return &s }
