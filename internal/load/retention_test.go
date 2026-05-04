package load

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
)

func TestSyncShipsRetainsUnmatchedHullRecords(t *testing.T) {
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/items/ship_hulls":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"hull-1","name":"Aurora","external_refs":[{"source":"sc","id":"MISC_AURORA"}],"paints":[]}]}`))
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			t.Fatalf("unexpected delete request: %s %s", r.Method, r.URL.Path)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := directus.NewClient(server.URL, "test-token")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.HTTPClient = server.Client()

	b := &builder{
		ctx:    context.Background(),
		client: client,
		collections: collections{
			Ships: "ship_hulls",
		},
	}

	if _, err := b.syncShips(nil, func(string) (string, error) { return "", nil }, nil, "VTEST", false); err != nil {
		t.Fatalf("sync ships: %v", err)
	}
	if got := deleteCalls.Load(); got != 0 {
		t.Fatalf("expected no delete calls, got %d", got)
	}
}

func TestSyncShipVariantsRetainsUnmatchedVariantRecords(t *testing.T) {
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/items/ship_variants":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"variant-1","hull":{"id":"hull-1"},"name":"Aurora MR","variant_code":"MR","external_refs":[{"source":"sc","id":"MISC_AURORA_MR"}],"stats":{}}]}`))
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			t.Fatalf("unexpected delete request: %s %s", r.Method, r.URL.Path)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
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

	if _, err := b.syncShipVariants(nil, nil, nil, "VTEST", false); err != nil {
		t.Fatalf("sync ship variants: %v", err)
	}
	if got := deleteCalls.Load(); got != 0 {
		t.Fatalf("expected no delete calls, got %d", got)
	}
}
