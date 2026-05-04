package transform

import "testing"

func TestMatchRSIMatrixEntryPrefersVariantSpecificEntry(t *testing.T) {
	hulls := map[string]*hullBuilder{
		"AEG_SABRE": {
			Key:         "AEG_SABRE",
			Name:        "Sabre",
			CompanyCode: "AEG",
		},
	}
	variant := &variantBuilder{
		HullKey:     "AEG_SABRE",
		VariantCode: "COMET",
		Name:        "Sabre COMET",
	}
	matchers := buildRSIMatrixMatchers([]rsiMatrixEntry{
		testRSIMatrixEntry(98, "AEGS", "Sabre", "/pledge/ships/sabre/Sabre"),
		testRSIMatrixEntry(120, "AEGS", "Sabre Comet", "/pledge/ships/sabre/Sabre-Comet"),
	})

	entry, ok := matchRSIMatrixEntry(variant, hulls, matchers)
	if !ok {
		t.Fatal("expected RSI match")
	}
	if entry.ID != 120 {
		t.Fatalf("expected Sabre Comet entry, got %d", entry.ID)
	}
}

func TestMatchRSIMatrixEntryMatchesMkVariantByTokenBag(t *testing.T) {
	hulls := map[string]*hullBuilder{
		"ANVL_HORNET_F7A": {
			Key:         "ANVL_HORNET_F7A",
			Name:        "Hornet F7a",
			CompanyCode: "ANVL",
		},
	}
	variant := &variantBuilder{
		HullKey:     "ANVL_HORNET_F7A",
		VariantCode: "MK2",
		Name:        "Hornet F7a MK2",
	}
	matchers := buildRSIMatrixMatchers([]rsiMatrixEntry{
		testRSIMatrixEntry(269, "ANVL", "F7A Hornet Mk I", "/pledge/ships/anvil-hornet/F7A-Hornet-Mk-I"),
		testRSIMatrixEntry(37, "ANVL", "F7A Hornet Mk II", "/pledge/ships/anvil-hornet-mkii/F7A-Hornet-Mk-II"),
	})

	entry, ok := matchRSIMatrixEntry(variant, hulls, matchers)
	if !ok {
		t.Fatal("expected RSI match")
	}
	if entry.ID != 37 {
		t.Fatalf("expected F7A Hornet Mk II entry, got %d", entry.ID)
	}
}

func TestMatchRSIMatrixEntryUsesTokenSubsetFallback(t *testing.T) {
	hulls := map[string]*hullBuilder{
		"ANVL_HORNET": {
			Key:         "ANVL_HORNET",
			Name:        "Hornet",
			CompanyCode: "ANVL",
		},
	}
	variant := &variantBuilder{
		HullKey:     "ANVL_HORNET",
		VariantCode: "F7CM_HEARTSEEKER",
		Name:        "Hornet F7CM HEARTSEEKER",
	}
	matchers := buildRSIMatrixMatchers([]rsiMatrixEntry{
		testRSIMatrixEntry(15, "ANVL", "F7C-M Super Hornet Mk I", "/pledge/ships/anvil-hornet/F7C-M-Super-Hornet-Mk-I"),
		testRSIMatrixEntry(177, "ANVL", "F7C-M Super Hornet Heartseeker Mk I", "/pledge/ships/anvil-hornet/F7C-M-Super-Hornet-Heartseeker-Mk-I"),
	})

	entry, ok := matchRSIMatrixEntry(variant, hulls, matchers)
	if !ok {
		t.Fatal("expected RSI match")
	}
	if entry.ID != 177 {
		t.Fatalf("expected Heartseeker entry, got %d", entry.ID)
	}
}

func TestMatchRSIMatrixEntryBaseVariantPrefersSmallestExtraSet(t *testing.T) {
	hulls := map[string]*hullBuilder{
		"ANVL_CARRACK": {
			Key:         "ANVL_CARRACK",
			Name:        "Carrack",
			CompanyCode: "ANVL",
		},
	}
	variant := &variantBuilder{
		HullKey:     "ANVL_CARRACK",
		VariantCode: "BASE",
		Name:        "Carrack",
	}
	matchers := buildRSIMatrixMatchers([]rsiMatrixEntry{
		testRSIMatrixEntry(62, "ANVL", "Carrack", "/pledge/ships/carrack/Carrack"),
		testRSIMatrixEntry(204, "ANVL", "Carrack w/C8X", "/pledge/ships/carrack/Carrack-W-C8X"),
		testRSIMatrixEntry(206, "ANVL", "Carrack Expedition", "/pledge/ships/carrack/Carrack-Expedition"),
	})

	entry, ok := matchRSIMatrixEntry(variant, hulls, matchers)
	if !ok {
		t.Fatal("expected RSI match")
	}
	if entry.ID != 62 {
		t.Fatalf("expected base Carrack entry, got %d", entry.ID)
	}
}

func testRSIMatrixEntry(id int, manufacturer, name, url string) rsiMatrixEntry {
	entry := rsiMatrixEntry{
		ID:   id,
		Name: name,
		URL:  url,
	}
	entry.Manufacturer.Code = manufacturer
	return entry
}
