package transform

import (
	"testing"

	"github.com/ArisCorporation/sc-goetl/internal/lib"
)

func TestEnsureVariantBuilderUsesDisplayOverrideFromGrouping(t *testing.T) {
	grouping := parseShipGrouping(map[string]any{
		"hulls": map[string]any{
			"ARGO_MPUV": map[string]any{
				"manufacturer":         "ARGO",
				"name":                 "Mpuv",
				"combine_variant_code": true,
				"variants": map[string]any{
					"1T": map[string]any{
						"match":   "ARGO_MPUV_1T",
						"display": "Tractor",
					},
				},
			},
			"DRAK_DRAGONFLY": map[string]any{
				"manufacturer":         "DRAK",
				"name":                 "Dragonfly",
				"combine_variant_code": true,
				"variants": map[string]any{
					"Yellow": map[string]any{
						"match":   "DRAK_Dragonfly_Yellow",
						"display": "Yellowjacket",
					},
				},
			},
			"ANVL_HORNET": map[string]any{
				"manufacturer":         "ANVL",
				"name":                 "Hornet",
				"combine_variant_code": true,
				"variants": map[string]any{
					"F7CS": map[string]any{
						"match":   "ANVL_Hornet_F7CS",
						"display": "F7CS Ghost",
					},
					"F7CS MK2": map[string]any{
						"match":   "ANVL_Hornet_F7CS_Mk2",
						"display": "F7CS Ghost MK2",
					},
				},
			},
		},
	})

	mpuv := ensureVariantBuilder(map[string]*variantBuilder{}, grouping, "ARGO_MPUV", lib.CanonicalVariantCode("1T"))
	if mpuv == nil {
		t.Fatal("expected MPUV variant builder")
	}
	if mpuv.Name != "Mpuv Tractor" {
		t.Fatalf("expected MPUV display name override, got %q", mpuv.Name)
	}

	dragonfly := ensureVariantBuilder(map[string]*variantBuilder{}, grouping, "DRAK_DRAGONFLY", lib.CanonicalVariantCode("YELLOW"))
	if dragonfly == nil {
		t.Fatal("expected Dragonfly variant builder")
	}
	if dragonfly.Name != "Dragonfly Yellowjacket" {
		t.Fatalf("expected Dragonfly display name override, got %q", dragonfly.Name)
	}

	hornet := ensureVariantBuilder(map[string]*variantBuilder{}, grouping, "ANVL_HORNET", lib.CanonicalVariantCode("F7CS"))
	if hornet == nil {
		t.Fatal("expected Hornet F7CS variant builder")
	}
	if hornet.Name != "Hornet F7CS Ghost" {
		t.Fatalf("expected Hornet Ghost display name override, got %q", hornet.Name)
	}

	hornetMk2 := ensureVariantBuilder(map[string]*variantBuilder{}, grouping, "ANVL_HORNET", lib.CanonicalVariantCode("F7CS_MK2"))
	if hornetMk2 == nil {
		t.Fatal("expected Hornet F7CS MK2 variant builder")
	}
	if hornetMk2.Name != "Hornet F7CS Ghost MK2" {
		t.Fatalf("expected Hornet Ghost MK2 display name override, got %q", hornetMk2.Name)
	}
}

func TestVariantAssignmentDisplayOverrideWinsOverSourceVariantName(t *testing.T) {
	grouping := parseShipGrouping(map[string]any{
		"hulls": map[string]any{
			"ANVL_HORNET": map[string]any{
				"manufacturer":         "ANVL",
				"name":                 "Hornet",
				"combine_variant_code": true,
				"variants": map[string]any{
					"F7CS MK2": map[string]any{
						"match":   "ANVL_Hornet_F7CS_Mk2",
						"display": "F7CS Ghost MK2",
					},
				},
			},
		},
	})

	assignment, ok := grouping.LookupVariant("ANVL_HORNET", "F7CS_MK2")
	if !ok {
		t.Fatal("expected Hornet F7CS MK2 assignment")
	}

	vb := ensureVariantBuilder(map[string]*variantBuilder{}, grouping, "ANVL_HORNET", lib.CanonicalVariantCode("F7CS_MK2"))
	if vb == nil {
		t.Fatal("expected Hornet F7CS MK2 variant builder")
	}

	// Simulate the raw source name overwrite that happens during transform ingestion.
	vb.Name = "Hornet F7CS MK2"
	vb.Name = variantAssignmentDisplayName(grouping, assignment)

	if vb.Name != "Hornet F7CS Ghost MK2" {
		t.Fatalf("expected grouping display override to win over source name, got %q", vb.Name)
	}
}
