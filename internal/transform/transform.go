package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ArisCorporation/sc-goetl/internal/lib"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

// Result wraps both legacy and v2 normalized payloads.
type Result struct {
	Legacy        model.NormalizedDataBundle
	V2            model.NormalizedBundleV2
	RawDir        string
	NormalizedDir string
	Config        Config
}

// Run normalizes the raw Star Citizen data into the bundle consumed by loaders.
func Run(ctx context.Context, dataRoot string, channel model.Channel, version string) (*Result, error) {
	channelKey := string(channel)
	rawDir := filepath.Join(dataRoot, "raw", channelKey, version)
	normalizedDir := filepath.Join(dataRoot, "normalized", channelKey, version)

	config := LoadConfig(nil)

	manuRows, err := readJSONArray(filepath.Join(rawDir, "manufacturers.json"))
	if err != nil {
		return nil, fmt.Errorf("read manufacturers: %w", err)
	}
	shipRows, err := readJSONArray(filepath.Join(rawDir, "ships.json"))
	if err != nil {
		return nil, fmt.Errorf("read ships: %w", err)
	}
	variantRows, err := readJSONArray(filepath.Join(rawDir, "ship_variants.json"))
	if err != nil {
		variantRows = nil
	}
	hardpointRows, err := readJSONArray(filepath.Join(rawDir, "hardpoints.json"))
	if err != nil {
		hardpointRows = nil
	}
	itemRows, err := readJSONArray(filepath.Join(rawDir, "items.json"))
	if err != nil {
		return nil, fmt.Errorf("read items: %w", err)
	}
	itemStatsRows, err := readJSONArray(filepath.Join(rawDir, "item_stats.json"))
	if err != nil {
		itemStatsRows = nil
	}
	shipStatsRows, err := readJSONArray(filepath.Join(rawDir, "ship_stats.json"))
	if err != nil {
		shipStatsRows = nil
	}
	installedRows, err := readJSONArray(filepath.Join(rawDir, "installed_items.json"))
	if err != nil {
		installedRows = nil
	}

	manufacturers := normalizeManufacturers(manuRows)
	manufacturerByCode := map[string]model.NormalizedManufacturer{}
	for _, manufacturer := range manufacturers {
		manufacturerByCode[strings.ToUpper(manufacturer.Code)] = manufacturer
	}

	ships := normalizeShips(shipRows, manufacturerByCode)
	shipByExternalID := map[string]model.NormalizedShip{}
	for _, ship := range ships {
		shipByExternalID[ship.ExternalID] = ship
	}

	variants := normalizeVariants(variantRows, shipByExternalID)
	variantByExternal := map[string]model.NormalizedShipVariant{}
	for _, variant := range variants {
		variantByExternal[variant.ExternalID] = variant
	}

	hardpoints := normalizeHardpoints(hardpointRows, variantByExternal)
	items := normalizeItems(itemRows, config.AllowedItemTypes)
	itemByExternal := map[string]model.NormalizedItem{}
	for _, item := range items {
		itemByExternal[item.ExternalID] = item
	}

	itemStats := normalizeItemStats(itemStatsRows, itemByExternal)
	if len(itemStats) == 0 {
		itemStats = buildItemStatsFallback(itemRows, itemByExternal)
	}
	shipStats := normalizeShipStats(shipStatsRows, variantByExternal)
	installedItems := normalizeInstalledItems(installedRows, variantByExternal, itemByExternal)
	locales := []model.NormalizedLocaleEntry{}

	grouping := LoadShipGrouping()

	fallbackStats, err := buildShipStatsFallback(rawDir, grouping, shipRows)
	if err != nil {
		utils.Logger().Warn("Failed to build ship stats fallback", "error", err)
	}
	if len(shipStats) == 0 && len(fallbackStats) > 0 {
		utils.Logger().Info("Using ship stats fallback", "count", len(fallbackStats))
		shipStats = fallbackStats
	} else if len(shipStats) > 0 && len(fallbackStats) > 0 {
		merged := mergeShipStatsFallback(shipStats, fallbackStats)
		if merged > 0 {
			utils.Logger().Info("Merged fallback ship stats fields", "updated_variants", merged)
		}
	}

	if len(hardpoints) == 0 || len(installedItems) == 0 {
		fallbackHardpoints, fallbackInstalled, err := buildLoadoutFallback(rawDir, grouping, shipRows, itemByExternal)
		if err != nil {
			utils.Logger().Warn("Failed to build loadout fallback data", "error", err)
		} else {
			if len(hardpoints) == 0 && len(fallbackHardpoints) > 0 {
				utils.Logger().Info("Using loadout fallback for hardpoints", "count", len(fallbackHardpoints))
				hardpoints = fallbackHardpoints
			}
			if len(installedItems) == 0 && len(fallbackInstalled) > 0 {
				utils.Logger().Info("Using loadout fallback for installed items", "count", len(fallbackInstalled))
				installedItems = fallbackInstalled
			}
		}
	}

	legacy := model.NormalizedDataBundle{
		Manufacturers:  manufacturers,
		Ships:          ships,
		ShipVariants:   variants,
		Items:          items,
		Hardpoints:     hardpoints,
		ItemStats:      itemStats,
		ShipStats:      shipStats,
		InstalledItems: installedItems,
		Locales:        locales,
	}

	v2 := buildV2Bundle(ctx, channel, version, manufacturers, ships, variants, items, itemStats, hardpoints, shipStats, grouping, shipRows, variantRows, config.HardpointsAsCollection)

	if err := utils.EnsureDir(normalizedDir); err != nil {
		return nil, fmt.Errorf("ensure normalized dir: %w", err)
	}
	writeTasks := []struct {
		Name string
		Data any
	}{
		{"manufacturers.json", legacy.Manufacturers},
		{"ships.json", legacy.Ships},
		{"ship_variants.json", legacy.ShipVariants},
		{"items.json", legacy.Items},
		{"hardpoints.json", legacy.Hardpoints},
		{"item_stats.json", legacy.ItemStats},
		{"ship_stats.json", legacy.ShipStats},
		{"installed_items.json", legacy.InstalledItems},
		{"locales.json", legacy.Locales},
		{"companies.v2.json", v2.Companies},
		{"ships.v2.json", v2.Ships},
		{"ship_variants.v2.json", v2.ShipVariants},
		{"items.v2.json", v2.Items},
		{"hardpoints.v2.json", v2.Hardpoints},
	}
	for _, task := range writeTasks {
		path := filepath.Join(normalizedDir, task.Name)
		if err := utils.WriteJSON(path, task.Data); err != nil {
			return nil, fmt.Errorf("write %s: %w", task.Name, err)
		}
	}

	return &Result{
		Legacy:        legacy,
		V2:            v2,
		RawDir:        rawDir,
		NormalizedDir: normalizedDir,
		Config:        config,
	}, nil
}

func normalizeManufacturers(rows []map[string]any) []model.NormalizedManufacturer {
	result := make([]model.NormalizedManufacturer, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		code := uppercase(firstNonEmpty(getString(row, "code"), getString(row, "Code")))
		name := firstNonEmpty(getString(row, "name"), getString(row, "Name"))
		description := firstNonEmpty(getString(row, "description"), getString(row, "Description"), getString(row, "content"))
		external := firstNonEmpty(getString(row, "external_id"), code, getString(row, "id"))
		if external == "" {
			external = code
		}
		if external == "" {
			external = name
		}
		if external == "" {
			continue
		}
		external = strings.ToUpper(external)
		if _, exists := seen[external]; exists {
			continue
		}
		seen[external] = struct{}{}
		manufacturer := model.NormalizedManufacturer{
			ExternalID: external,
			Code:       code,
			Name:       name,
		}
		if description != "" {
			manufacturer.Description = &description
		}
		if dataSource := getString(row, "data_source"); dataSource != "" {
			manufacturer.DataSource = &dataSource
		}
		if manufacturer.Code == "" {
			manufacturer.Code = external
		}
		result = append(result, manufacturer)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ExternalID < result[j].ExternalID
	})
	return result
}

func normalizeShips(rows []map[string]any, manufacturers map[string]model.NormalizedManufacturer) []model.NormalizedShip {
	result := make([]model.NormalizedShip, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		id := firstNonEmpty(getString(row, "external_id"), getString(row, "id"), getString(row, "UUID"), getString(row, "ClassName"))
		if id == "" {
			continue
		}
		external := strings.ToUpper(id)
		if _, exists := seen[external]; exists {
			continue
		}
		seen[external] = struct{}{}
		name := firstNonEmpty(getString(row, "name"), getString(row, "Name"))
		className := deriveShipClass(row)
		description := firstNonEmpty(getString(row, "description"), getString(row, "Description"))
		sizeValue := getString(row, "size")
		if sizeValue == "" {
			sizeValue = getString(row, "Size")
		}
		manufacturerCode := uppercase(firstNonEmpty(getString(row, "manufacturer_code"), getString(row, "manufacturer_id")))
		if manufacturerCode == "" {
			if nested, ok := row["manufacturer"].(map[string]any); ok {
				manufacturerCode = uppercase(firstNonEmpty(getString(nested, "code"), getString(nested, "Code")))
			}
		}
		if manufacturerCode == "" {
			manufacturerCode = "UNKNOWN"
		}
		ship := model.NormalizedShip{
			ExternalID:       external,
			Name:             name,
			Class:            className,
			ManufacturerCode: manufacturerCode,
		}
		if sizeValue != "" {
			sizeValue = strings.TrimSpace(sizeValue)
			ship.Size = &sizeValue
		}
		if description != "" {
			ship.Description = &description
		}
		result = append(result, ship)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ExternalID < result[j].ExternalID
	})
	return result
}

func normalizeVariants(rows []map[string]any, ships map[string]model.NormalizedShip) []model.NormalizedShipVariant {
	result := []model.NormalizedShipVariant{}
	seen := map[string]struct{}{}
	for _, row := range rows {
		external := firstNonEmpty(getString(row, "external_id"), getString(row, "id"))
		if external == "" {
			continue
		}
		external = strings.ToUpper(external)
		if _, exists := seen[external]; exists {
			continue
		}
		seen[external] = struct{}{}
		shipID := firstNonEmpty(getString(row, "ship_external_id"), getString(row, "ship_id"))
		shipID = strings.ToUpper(shipID)
		if shipID == "" {
			continue
		}
		variantCode := getString(row, "variant_code")
		thumbnail := getString(row, "thumbnail")
		description := getString(row, "description")
		name := getString(row, "name")
		variant := model.NormalizedShipVariant{
			ExternalID:     external,
			ShipExternalID: shipID,
		}
		if variantCode != "" {
			variantCode = strings.ToUpper(variantCode)
			variant.VariantCode = &variantCode
		}
		if name != "" {
			variant.Name = &name
		}
		if thumbnail != "" {
			variant.Thumbnail = &thumbnail
		}
		if description != "" {
			variant.Description = &description
		}
		result = append(result, variant)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ExternalID < result[j].ExternalID
	})
	return result
}

func normalizeHardpoints(rows []map[string]any, variants map[string]model.NormalizedShipVariant) []model.NormalizedHardpoint {
	result := []model.NormalizedHardpoint{}
	for _, row := range rows {
		external := firstNonEmpty(getString(row, "external_id"), getString(row, "id"), getString(row, "code"))
		if external == "" {
			continue
		}
		external = strings.ToUpper(external)
		variantID := firstNonEmpty(getString(row, "ship_variant_external_id"), getString(row, "ship_variant_id"))
		variantID = strings.ToUpper(variantID)
		if variantID == "" {
			continue
		}
		if _, ok := variants[variantID]; !ok && len(variants) > 0 {
			continue
		}
		code := firstNonEmpty(getString(row, "code"), getString(row, "name"))
		category := firstNonEmpty(getString(row, "category"), getString(row, "type"))
		if code == "" {
			code = external
		}
		if category == "" {
			category = "Unknown"
		}
		hardpoint := model.NormalizedHardpoint{
			ExternalID:            external,
			ShipVariantExternalID: variantID,
			Code:                  code,
			Category:              category,
		}
		if position := getString(row, "position"); position != "" {
			hardpoint.Position = &position
		}
		if size := getNumber(row, "size"); !math.IsNaN(size) {
			sizeInt := int(size)
			hardpoint.Size = &sizeInt
		}
		if gimballed := getString(row, "gimballed"); gimballed != "" {
			boolValue := strings.EqualFold(gimballed, "true") || gimballed == "1"
			hardpoint.Gimballed = &boolValue
		}
		if powered := getString(row, "powered"); powered != "" {
			boolValue := strings.EqualFold(powered, "true") || powered == "1"
			hardpoint.Powered = &boolValue
		}
		result = append(result, hardpoint)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ExternalID < result[j].ExternalID
	})
	return result
}

func normalizeItems(rows []map[string]any, allowed map[string]struct{}) []model.NormalizedItem {
	result := []model.NormalizedItem{}
	seen := map[string]struct{}{}
	for _, row := range rows {
		external := firstNonEmpty(getString(row, "external_id"), getString(row, "id"), getString(row, "reference"), getString(row, "className"))
		if external == "" {
			continue
		}
		external = strings.ToUpper(external)
		if _, exists := seen[external]; exists {
			continue
		}
		if !shouldIncludeItem(row, allowed) {
			continue
		}
		seen[external] = struct{}{}
		typeName := firstNonEmpty(strings.ToUpper(getString(row, "type")), strings.ToUpper(getString(row, "Type")), resolveItemTypeToken(row))
		if typeName == "" {
			continue
		}
		name := firstNonEmpty(getString(row, "name"), getString(row, "Name"))
		if name == "" {
			name = external
		}
		manufacturerCode := uppercase(firstNonEmpty(getString(row, "manufacturer"), getString(row, "manufacturer_code"), nestedManufacturerCode(row)))
		item := model.NormalizedItem{
			ExternalID: external,
			Type:       typeName,
			Name:       name,
		}
		if manufacturerCode != "" {
			item.ManufacturerCode = &manufacturerCode
		}
		if subtype := itemSubtypeFromRow(row); subtype != "" {
			item.Subtype = &subtype
		}
		if grade := getString(row, "grade", "Grade"); grade != "" {
			item.Grade = &grade
		}
		if classValue := getString(row, "class", "Class"); classValue != "" {
			item.Class = &classValue
		}
		if sizeValue := getNumber(row, "size", "Size"); !math.IsNaN(sizeValue) {
			sizeInt := int(sizeValue)
			item.Size = &sizeInt
		}
		if description := firstNonEmpty(getString(row, "description"), getString(row, "Description")); description != "" {
			item.Description = &description
		}
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ExternalID < result[j].ExternalID
	})
	return result
}

func normalizeItemStats(rows []map[string]any, items map[string]model.NormalizedItem) []model.NormalizedItemStat {
	result := []model.NormalizedItemStat{}
	for _, row := range rows {
		external := strings.ToUpper(firstNonEmpty(getString(row, "item_external_id"), getString(row, "item_id")))
		if external == "" {
			continue
		}
		if _, ok := items[external]; !ok && len(items) > 0 {
			continue
		}
		stats := map[string]any{}
		if payload, ok := row["stats"].(map[string]any); ok {
			stats = payload
		}
		entry := model.NormalizedItemStat{
			ItemExternalID: external,
			Stats:          stats,
		}
		if price := getNumber(row, "price_auec"); !math.IsNaN(price) {
			value := price
			entry.PriceAUEC = &value
		}
		if availability := getString(row, "availability"); availability != "" {
			entry.Availability = &availability
		}
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ItemExternalID < result[j].ItemExternalID
	})
	return result
}

func buildItemStatsFallback(rawItems []map[string]any, items map[string]model.NormalizedItem) []model.NormalizedItemStat {
	result := []model.NormalizedItemStat{}
	if len(rawItems) == 0 || len(items) == 0 {
		return result
	}
	for _, row := range rawItems {
		if row == nil {
			continue
		}
		stdItem := getMap(row, "stdItem", "StdItem")
		candidates := []string{
			getString(row, "external_id"),
			getString(row, "id"),
			getString(row, "reference"),
			getString(row, "className"),
			getString(row, "itemName"),
		}
		if stdItem != nil {
			candidates = append(candidates, getString(stdItem, "UUID"), getString(stdItem, "Name"))
		}
		external := strings.ToUpper(firstNonEmpty(candidates...))
		if external == "" {
			continue
		}
		if _, ok := items[external]; !ok {
			continue
		}
		payload := map[string]any{}
		if stdItem != nil && len(stdItem) > 0 {
			payload["stdItem"] = stdItem
		}
		if classification, ok := getRawValue(row, "classification", "Classification"); ok {
			payload["classification"] = classification
		}
		if tags, ok := getRawValue(row, "tags", "Tags"); ok {
			payload["tags"] = tags
		}
		if len(payload) == 0 {
			continue
		}
		result = append(result, model.NormalizedItemStat{
			ItemExternalID: external,
			Stats:          payload,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ItemExternalID < result[j].ItemExternalID
	})
	return result
}

func normalizeShipStats(rows []map[string]any, variants map[string]model.NormalizedShipVariant) []model.NormalizedShipStat {
	result := []model.NormalizedShipStat{}
	for _, row := range rows {
		variantID := strings.ToUpper(firstNonEmpty(getString(row, "ship_variant_external_id"), getString(row, "ship_variant_id")))
		if variantID == "" {
			continue
		}
		if _, ok := variants[variantID]; !ok && len(variants) > 0 {
			continue
		}
		stats := map[string]any{}
		if payload, ok := row["stats"].(map[string]any); ok {
			stats = payload
		}
		entry := model.NormalizedShipStat{
			ShipVariantExternalID: variantID,
			Stats:                 stats,
		}
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ShipVariantExternalID < result[j].ShipVariantExternalID
	})
	return result
}

func mergeShipStatsFallback(primary []model.NormalizedShipStat, fallback []model.NormalizedShipStat) int {
	if len(primary) == 0 || len(fallback) == 0 {
		return 0
	}
	fallbackMap := map[string]map[string]any{}
	for _, entry := range fallback {
		if entry.ShipVariantExternalID == "" || entry.Stats == nil {
			continue
		}
		fallbackMap[entry.ShipVariantExternalID] = entry.Stats
	}
	updated := 0
	for idx := range primary {
		payload, ok := fallbackMap[primary[idx].ShipVariantExternalID]
		if !ok || len(payload) == 0 {
			continue
		}
		if primary[idx].Stats == nil {
			primary[idx].Stats = map[string]any{}
		}
		changed := false
		for key, value := range payload {
			if _, exists := primary[idx].Stats[key]; exists {
				continue
			}
			primary[idx].Stats[key] = value
			changed = true
		}
		if changed {
			updated++
		}
	}
	return updated
}

func normalizeInstalledItems(rows []map[string]any, variants map[string]model.NormalizedShipVariant, items map[string]model.NormalizedItem) []model.NormalizedInstalledItem {
	result := []model.NormalizedInstalledItem{}
	for _, row := range rows {
		variantID := strings.ToUpper(firstNonEmpty(getString(row, "ship_variant_external_id"), getString(row, "ship_variant_id")))
		itemID := strings.ToUpper(firstNonEmpty(getString(row, "item_external_id"), getString(row, "item_id")))
		if variantID == "" || itemID == "" {
			continue
		}
		if _, ok := variants[variantID]; !ok && len(variants) > 0 {
			continue
		}
		if _, ok := items[itemID]; !ok && len(items) > 0 {
			continue
		}
		quantity := 1
		if q := getNumber(row, "quantity"); !math.IsNaN(q) {
			quantity = int(q)
			if quantity <= 0 {
				quantity = 1
			}
		}
		entry := model.NormalizedInstalledItem{
			ShipVariantExternalID: variantID,
			ItemExternalID:        itemID,
			Quantity:              quantity,
		}
		if hardpoint := getString(row, "hardpoint_external_id"); hardpoint != "" {
			hardpoint = strings.ToUpper(hardpoint)
			entry.HardpointExternalID = &hardpoint
		}
		if profile := getString(row, "profile"); profile != "" {
			entry.Profile = &profile
		}
		if livery := getString(row, "livery"); livery != "" {
			entry.Livery = &livery
		}
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		lhs := result[i].ShipVariantExternalID + ":" + result[i].ItemExternalID
		rhs := result[j].ShipVariantExternalID + ":" + result[j].ItemExternalID
		return lhs < rhs
	})
	return result
}

const primaryRefSource = "SC_DATA"

type refCollector map[string]map[string]struct{}

func newRefCollector() refCollector {
	return refCollector{}
}

func (r refCollector) add(source, id string) {
	source = strings.TrimSpace(source)
	id = strings.TrimSpace(id)
	if source == "" || id == "" {
		return
	}
	if r == nil {
		return
	}
	bucket, ok := r[source]
	if !ok {
		bucket = map[string]struct{}{}
		r[source] = bucket
	}
	bucket[id] = struct{}{}
}

func (r refCollector) merge(other refCollector) {
	for source, ids := range other {
		for id := range ids {
			r.add(source, id)
		}
	}
}

func (r refCollector) list() []model.NormalizedExternalReference {
	if len(r) == 0 {
		return []model.NormalizedExternalReference{}
	}
	sources := make([]string, 0, len(r))
	for source := range r {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	result := make([]model.NormalizedExternalReference, 0, len(sources))
	for _, source := range sources {
		ids := r[source]
		values := make([]string, 0, len(ids))
		for id := range ids {
			values = append(values, id)
		}
		sort.Strings(values)
		for _, id := range values {
			result = append(result, model.NormalizedExternalReference{Source: source, ID: id})
		}
	}
	return result
}

type hullBuilder struct {
	Key         string
	Name        string
	CompanyCode string
	Refs        refCollector
	Paints      map[string]struct{}
}

type variantBuilder struct {
	ExternalID   string
	HullKey      string
	VariantCode  string
	Name         string
	Refs         refCollector
	Stats        model.ShipVariantStatsV2
	Thumbnail    *string
	ReleasePatch *string
}

func ensureHullBuilder(builders map[string]*hullBuilder, grouping *ShipGrouping, key string) *hullBuilder {
	key = strings.TrimSpace(strings.ToUpper(key))
	if key == "" {
		return nil
	}
	if builder, ok := builders[key]; ok {
		return builder
	}
	builder := &hullBuilder{
		Key:    key,
		Refs:   newRefCollector(),
		Paints: map[string]struct{}{},
	}
	if grouping != nil {
		if def, ok := grouping.GetHull(key); ok {
			if builder.Name == "" {
				builder.Name = def.Name
			}
			if builder.CompanyCode == "" && def.Manufacturer != "" {
				builder.CompanyCode = strings.ToUpper(def.Manufacturer)
			}
		}
	}
	builders[key] = builder
	return builder
}

func ensureVariantBuilder(builders map[string]*variantBuilder, grouping *ShipGrouping, hullKey string, code lib.CanonicalVariantCode) *variantBuilder {
	hullKey = strings.TrimSpace(strings.ToUpper(hullKey))
	if hullKey == "" {
		return nil
	}
	canonical := canonicalVariantID(hullKey, code)
	if builder, ok := builders[canonical]; ok {
		return builder
	}
	builder := &variantBuilder{
		ExternalID:  canonical,
		HullKey:     hullKey,
		VariantCode: string(code),
		Refs:        newRefCollector(),
	}
	if grouping != nil {
		if hull, ok := grouping.GetHull(hullKey); ok {
			if hull.CombineVariantCode {
				if strings.EqualFold(string(code), "BASE") {
					builder.Name = lib.CanonicalVariantName(hull.Name, "BASE")
				} else {
					builder.Name = lib.CanonicalVariantName(hull.Name, code)
				}
			} else {
				displayName := ""
				if assignment, ok := grouping.LookupVariant(hullKey, string(code)); ok {
					displayName = strings.TrimSpace(assignment.DisplayVariantCode)
				}
				if displayName != "" {
					builder.Name = displayName
				} else {
					builder.Name = strings.TrimSpace(strings.ToUpper(string(code)))
				}
				if builder.Name == "" {
					builder.Name = "BASE"
				}
			}
		}
	}
	if builder.Name == "" {
		builder.Name = lib.CanonicalVariantName(hullKey, code)
	}
	builders[canonical] = builder
	return builder
}

func canonicalVariantID(hullKey string, code lib.CanonicalVariantCode) string {
	hull := strings.TrimSpace(strings.ToUpper(hullKey))
	if hull == "" {
		return ""
	}
	canonical := string(code)
	if canonical == "" {
		canonical = "BASE"
	}
	return hull + "_" + strings.ToUpper(canonical)
}

type uexVehicle struct {
	ID          int    `json:"id"`
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	NameFull    string `json:"name_full"`
	CompanyName string `json:"company_name"`
}

type uexVehicleResponse struct {
	Data []uexVehicle `json:"data"`
}

func fetchUEXVehicles(ctx context.Context, url string) ([]uexVehicle, error) {
	if strings.TrimSpace(url) == "" {
		url = "https://api.uexcorp.uk/2.0/vehicles"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}

	var payload uexVehicleResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Data, nil
}

func normalizeUEXLookupKey(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	buf := strings.Builder{}
	for _, r := range value {
		switch {
		case r >= 'A' && r <= 'Z':
			buf.WriteRune(r)
		case r >= '0' && r <= '9':
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

func stripMkSuffix(key string) string {
	if key == "" {
		return ""
	}
	for {
		idx := strings.LastIndex(key, "MK")
		if idx <= 0 || idx+2 >= len(key) {
			return key
		}
		suffix := key[idx+2:]
		if suffix == "" {
			return key
		}
		valid := true
		for _, r := range suffix {
			if r == 'I' || r == 'V' || r == 'X' || (r >= '0' && r <= '9') {
				continue
			}
			valid = false
			break
		}
		if !valid {
			return key
		}
		key = key[:idx]
	}
}

func vehicleLookupKeys(vehicle uexVehicle) []string {
	keys := map[string]struct{}{}
	add := func(value string) {
		if key := normalizeUEXLookupKey(value); key != "" {
			keys[key] = struct{}{}
			if trimmed := stripMkSuffix(key); trimmed != "" {
				keys[trimmed] = struct{}{}
			}
		}
	}
	add(vehicle.Slug)
	add(vehicle.Name)
	add(vehicle.NameFull)
	if vehicle.CompanyName != "" && vehicle.Name != "" {
		add(vehicle.CompanyName + " " + vehicle.Name)
		add(vehicle.CompanyName + " " + vehicle.NameFull)
	}
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func variantLookupKeys(variant *variantBuilder, hulls map[string]*hullBuilder) []string {
	if variant == nil {
		return nil
	}
	keys := map[string]struct{}{}
	add := func(value string) {
		if key := normalizeUEXLookupKey(value); key != "" {
			keys[key] = struct{}{}
			if trimmed := stripMkSuffix(key); trimmed != "" {
				keys[trimmed] = struct{}{}
			}
		}
	}

	hullName := variant.HullKey
	if hb := hulls[variant.HullKey]; hb != nil && strings.TrimSpace(hb.Name) != "" {
		hullName = hb.Name
	}
	hullCode := variant.HullKey
	add(variant.Name)
	add(hullName)
	if strings.TrimSpace(variant.VariantCode) != "" {
		add(variant.VariantCode)
		add(hullName + " " + variant.VariantCode)
		add(variant.VariantCode + " " + hullName)
		add(variant.VariantCode + " " + hullCode)
	}
	add(variant.ExternalID)
	if hb := hulls[variant.HullKey]; hb != nil {
		if hb.CompanyCode != "" && strings.TrimSpace(variant.VariantCode) != "" {
			add(hb.CompanyCode + " " + variant.VariantCode + " " + hullName)
		}
	}

	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func mergeUEXExternalRefs(ctx context.Context, hullBuilders map[string]*hullBuilder, variantBuilders map[string]*variantBuilder) {
	vehicles, err := fetchUEXVehicles(ctx, "")
	if err != nil {
		utils.Logger().Warn("Failed to fetch UEX vehicles", "error", err)
		return
	}
	index := map[string]uexVehicle{}
	for _, vehicle := range vehicles {
		if vehicle.ID <= 0 {
			continue
		}
		for _, key := range vehicleLookupKeys(vehicle) {
			if key == "" {
				continue
			}
			if _, exists := index[key]; !exists {
				index[key] = vehicle
			}
		}
	}

	matched := 0
	for _, variant := range variantBuilders {
		if variant == nil {
			continue
		}
		for _, key := range variantLookupKeys(variant, hullBuilders) {
			if vehicle, ok := index[key]; ok {
				variant.Refs.add("UEX", fmt.Sprintf("%d", vehicle.ID))
				matched++
				break
			}
		}
	}
	utils.Logger().Info("UEX external refs merged", "matches", matched, "variants", len(variantBuilders), "vehicles", len(vehicles))
}

type rsiMatrixEntry struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	URL          string `json:"url"`
	Manufacturer struct {
		Code string `json:"code"`
		Name string `json:"name"`
	} `json:"manufacturer"`
}

type rsiMatrixPayload struct {
	Data []rsiMatrixEntry `json:"data"`
}

func loadRSIMatrix() ([]rsiMatrixEntry, error) {
	bytes, err := os.ReadFile("matrix.json")
	if err != nil {
		return nil, err
	}
	var payload rsiMatrixPayload
	if err := json.Unmarshal(bytes, &payload); err != nil {
		return nil, err
	}
	return payload.Data, nil
}

func rsiLookupKeys(entry rsiMatrixEntry) []string {
	keys := map[string]struct{}{}
	add := func(value string) {
		if key := normalizeUEXLookupKey(value); key != "" {
			keys[key] = struct{}{}
			if trimmed := stripMkSuffix(key); trimmed != "" {
				keys[trimmed] = struct{}{}
			}
		}
	}
	add(entry.Name)
	add(entry.Manufacturer.Code + " " + entry.Name)
	add(entry.Manufacturer.Name + " " + entry.Name)

	slug := strings.TrimSpace(entry.URL)
	if slug != "" {
		parts := strings.Split(strings.Trim(slug, "/"), "/")
		if len(parts) > 0 {
			last := parts[len(parts)-1]
			last = strings.ReplaceAll(last, "-", " ")
			add(last)
			add(entry.Manufacturer.Code + " " + last)
		}
	}
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func mergeRSIExternalRefs(hullBuilders map[string]*hullBuilder, variantBuilders map[string]*variantBuilder) {
	rows, err := loadRSIMatrix()
	if err != nil {
		utils.Logger().Warn("Failed to load RSI matrix", "error", err)
		return
	}
	index := map[string]rsiMatrixEntry{}
	for _, entry := range rows {
		if entry.ID <= 0 {
			continue
		}
		for _, key := range rsiLookupKeys(entry) {
			if key == "" {
				continue
			}
			if _, exists := index[key]; !exists {
				index[key] = entry
			}
		}
	}
	matched := 0
	for _, variant := range variantBuilders {
		if variant == nil {
			continue
		}
		for _, key := range variantLookupKeys(variant, hullBuilders) {
			if entry, ok := index[key]; ok {
				variant.Refs.add("RSI", fmt.Sprintf("%d", entry.ID))
				matched++
				break
			}
		}
	}
	utils.Logger().Info("RSI external refs merged", "matches", matched, "variants", len(variantBuilders), "matrix_rows", len(rows))
}

func canonicalizeHullBuilders(builders map[string]*hullBuilder, grouping *ShipGrouping) (map[string]*hullBuilder, map[string]string) {
	if len(builders) == 0 {
		return builders, map[string]string{}
	}
	canonical := map[string]*hullBuilder{}
	aliases := map[string]string{}
	for _, builder := range builders {
		if builder == nil {
			continue
		}
		canonicalKey := strings.TrimSpace(strings.ToUpper(builder.Key))
		if canonicalKey == "" {
			continue
		}
		if grouping != nil {
			candidates := []string{canonicalKey}
			for _, ref := range builder.Refs.list() {
				if ref.ID != "" {
					candidates = append(candidates, ref.ID)
				}
			}
			if assignment, ok := grouping.LookupShipID(candidates...); ok && strings.TrimSpace(assignment.HullKey) != "" {
				canonicalKey = strings.TrimSpace(strings.ToUpper(assignment.HullKey))
			} else if def, ok := grouping.GetHull(canonicalKey); ok && strings.TrimSpace(def.HullKey) != "" {
				canonicalKey = strings.TrimSpace(strings.ToUpper(def.HullKey))
			}
		}
		aliases[canonicalKey] = canonicalKey
		target, exists := canonical[canonicalKey]
		if !exists {
			target = &hullBuilder{
				Key:    canonicalKey,
				Refs:   newRefCollector(),
				Paints: map[string]struct{}{},
			}
		}
		aliases[strings.TrimSpace(strings.ToUpper(builder.Key))] = canonicalKey
		if target.Name == "" && strings.TrimSpace(builder.Name) != "" {
			target.Name = builder.Name
		}
		if target.CompanyCode == "" && strings.TrimSpace(builder.CompanyCode) != "" {
			target.CompanyCode = builder.CompanyCode
		}
		if target.Paints == nil {
			target.Paints = map[string]struct{}{}
		}
		for paint := range builder.Paints {
			target.Paints[paint] = struct{}{}
		}
		target.Refs.merge(builder.Refs)
		for _, ref := range builder.Refs.list() {
			if ref.ID != "" {
				aliases[strings.TrimSpace(strings.ToUpper(ref.ID))] = canonicalKey
			}
		}
		canonical[canonicalKey] = target
	}
	return canonical, aliases
}

func canonicalizeVariantBuilders(builders map[string]*variantBuilder, grouping *ShipGrouping, hullBuilders map[string]*hullBuilder, hullAliases map[string]string) map[string]*variantBuilder {
	if len(builders) == 0 {
		return builders
	}
	canonical := map[string]*variantBuilder{}
	for _, builder := range builders {
		if builder == nil {
			continue
		}
		originalHull := strings.TrimSpace(strings.ToUpper(builder.HullKey))
		if originalHull == "" {
			continue
		}
		canonicalHull := originalHull
		if mapped, ok := hullAliases[originalHull]; ok && mapped != "" {
			canonicalHull = mapped
		} else if grouping != nil {
			if assignment, ok := grouping.LookupShipVariantID(builder.ExternalID); ok && strings.TrimSpace(assignment.HullKey) != "" {
				canonicalHull = strings.TrimSpace(strings.ToUpper(assignment.HullKey))
			} else {
				candidates := []string{originalHull}
				for _, ref := range builder.Refs.list() {
					if ref.ID != "" {
						candidates = append(candidates, ref.ID)
					}
				}
				if assignment, ok := grouping.LookupShipID(candidates...); ok && strings.TrimSpace(assignment.HullKey) != "" {
					canonicalHull = strings.TrimSpace(strings.ToUpper(assignment.HullKey))
				}
			}
		}
		code := lib.CanonicalVariantCode(builder.VariantCode)
		canonicalID := canonicalVariantID(canonicalHull, code)
		target, exists := canonical[canonicalID]
		if !exists {
			target = &variantBuilder{
				ExternalID:  canonicalID,
				HullKey:     canonicalHull,
				VariantCode: string(code),
				Refs:        newRefCollector(),
			}
		}
		if target.Name == "" && strings.TrimSpace(builder.Name) != "" {
			target.Name = builder.Name
		}
		if target.Thumbnail == nil && builder.Thumbnail != nil {
			target.Thumbnail = builder.Thumbnail
		}
		if target.ReleasePatch == nil && builder.ReleasePatch != nil {
			target.ReleasePatch = builder.ReleasePatch
		}
		if isEmptyVariantStats(target.Stats) && !isEmptyVariantStats(builder.Stats) {
			target.Stats = builder.Stats
		}
		target.Refs.merge(builder.Refs)
		canonical[canonicalID] = target
	}
	return canonical
}

func isEmptyVariantStats(stats model.ShipVariantStatsV2) bool {
	return stats.Length == nil &&
		stats.Width == nil &&
		stats.Height == nil &&
		stats.Mass == nil &&
		stats.CargoCapacity == nil &&
		stats.Crew == nil &&
		stats.Performance == nil &&
		stats.Propulsion == nil &&
		stats.Defence == nil &&
		len(stats.Insurance) == 0 &&
		len(stats.Raw) == 0 &&
		len(stats.Hardpoints) == 0 &&
		len(stats.Additional) == 0
}

func filterHullBuildersByGrouping(builders map[string]*hullBuilder, grouping *ShipGrouping) map[string]*hullBuilder {
	if grouping == nil || len(builders) == 0 {
		return builders
	}
	filtered := map[string]*hullBuilder{}
	for key, builder := range builders {
		if builder == nil {
			continue
		}
		if _, ok := grouping.GetHull(key); !ok {
			continue
		}
		filtered[key] = builder
	}
	return filtered
}

func filterVariantBuildersByGrouping(builders map[string]*variantBuilder, grouping *ShipGrouping) map[string]*variantBuilder {
	if grouping == nil || len(builders) == 0 {
		return builders
	}
	filtered := map[string]*variantBuilder{}
	for key, builder := range builders {
		if builder == nil {
			continue
		}
		code := strings.TrimSpace(strings.ToUpper(builder.VariantCode))
		if code == "" {
			code = "BASE"
		}
		if _, ok := grouping.LookupVariant(builder.HullKey, code); !ok {
			continue
		}
		filtered[key] = builder
	}
	return filtered
}

func canonicalVariantCode(code string) lib.CanonicalVariantCode {
	if strings.TrimSpace(code) == "" {
		return "BASE"
	}
	return lib.CanonicalVariantCode(sanitizeVariantCode(code))
}

func addStringToSet(set map[string]struct{}, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	set[value] = struct{}{}
}

func collectRawShipReferences(grouping *ShipGrouping, hullBuilders map[string]*hullBuilder, variantBuilders map[string]*variantBuilder, rows []map[string]any) {
	if len(rows) == 0 {
		return
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		candidateValues := []string{
			toString(row["id"]),
			toString(row["ID"]),
			toString(row["uuid"]),
			toString(row["UUID"]),
			toString(row["ClassName"]),
			toString(row["class_name"]),
			toString(row["Name"]),
			toString(row["name"]),
		}
		var assignment VariantAssignment
		foundVariant := false
		if grouping != nil {
			for _, candidate := range candidateValues {
				if candidate == "" {
					continue
				}
				if match, ok := grouping.LookupShipVariantID(candidate); ok {
					assignment = match
					foundVariant = true
					break
				}
			}
		}
		var hullAssignment VariantAssignment
		foundHull := false
		if grouping != nil {
			for _, candidate := range candidateValues {
				if candidate == "" {
					continue
				}
				if match, ok := grouping.LookupShipID(candidate); ok {
					hullAssignment = match
					foundHull = true
					break
				}
			}
		}
		var hb *hullBuilder
		if foundVariant {
			hb = ensureHullBuilder(hullBuilders, grouping, assignment.HullKey)
			vb := ensureVariantBuilder(variantBuilders, grouping, assignment.HullKey, assignment.VariantCode)
			if vb != nil {
				addRawShipRowRefs(vb, row)
			}
		} else if foundHull {
			hb = ensureHullBuilder(hullBuilders, grouping, hullAssignment.HullKey)
		}
		if hb != nil {
			addRawShipRowRefsToHull(hb, row)
		}
	}
}

func addRawShipRowRefsToHull(builder *hullBuilder, row map[string]any) {
	if builder == nil || row == nil {
		return
	}
	if builder.Name == "" {
		if name := strings.TrimSpace(toString(row["Name"])); name != "" {
			builder.Name = name
		}
	}
	if builder.CompanyCode == "" {
		if manu, ok := row["manufacturer"].(map[string]any); ok {
			if code := strings.TrimSpace(toString(manu["code"])); code != "" {
				builder.CompanyCode = strings.ToUpper(code)
			}
		}
		if manu, ok := row["Manufacturer"].(map[string]any); ok && builder.CompanyCode == "" {
			if code := strings.TrimSpace(toString(manu["Code"])); code != "" {
				builder.CompanyCode = strings.ToUpper(code)
			}
		}
	}
	if id := strings.TrimSpace(toString(row["id"])); id != "" {
		builder.Refs.add("raw:ships.id", id)
	}
	if uuid := strings.TrimSpace(toString(row["UUID"])); uuid != "" {
		builder.Refs.add("raw:ships.UUID", uuid)
	}
	if className := strings.TrimSpace(toString(row["ClassName"])); className != "" {
		builder.Refs.add("raw:ships.ClassName", className)
	}
	if name := strings.TrimSpace(toString(row["Name"])); name != "" {
		builder.Refs.add("raw:ships.Name", name)
	}
	if primary := strings.TrimSpace(toString(row["primary"])); primary != "" {
		builder.Refs.add("raw:ships.primary", primary)
	}
}

func addRawShipRowRefs(builder *variantBuilder, row map[string]any) {
	if builder == nil || row == nil {
		return
	}
	if builder.Name == "" {
		if name := strings.TrimSpace(toString(row["Name"])); name != "" {
			builder.Name = name
		}
	}
	if id := strings.TrimSpace(toString(row["id"])); id != "" {
		builder.Refs.add("raw:ships.id", id)
	}
	if uuid := strings.TrimSpace(toString(row["UUID"])); uuid != "" {
		builder.Refs.add("raw:ships.UUID", uuid)
	}
	if className := strings.TrimSpace(toString(row["ClassName"])); className != "" {
		builder.Refs.add("raw:ships.ClassName", className)
	}
	if name := strings.TrimSpace(toString(row["Name"])); name != "" {
		builder.Refs.add("raw:ships.Name", name)
	}
	if primary := strings.TrimSpace(toString(row["primary"])); primary != "" {
		builder.Refs.add("raw:ships.primary", primary)
	}
}

func collectRawVariantReferences(grouping *ShipGrouping, hullBuilders map[string]*hullBuilder, variantBuilders map[string]*variantBuilder, rows []map[string]any) {
	if len(rows) == 0 {
		return
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		rawID := strings.TrimSpace(toString(row["id"]))
		rawShipID := strings.TrimSpace(toString(row["ship_id"]))
		variantCode := strings.TrimSpace(toString(row["variant_code"]))
		name := strings.TrimSpace(toString(row["name"]))

		var assignment VariantAssignment
		found := false
		if grouping != nil && rawID != "" {
			if match, ok := grouping.LookupShipVariantID(rawID); ok {
				assignment = match
				found = true
			}
		}
		if !found && grouping != nil && rawShipID != "" {
			if hullMatch, ok := grouping.LookupShipID(rawShipID); ok {
				candidates := []string{}
				if variantCode != "" {
					candidates = append(candidates, variantCode)
				}
				if name != "" {
					candidates = append(candidates, name)
				}
				if rawID != "" {
					candidates = append(candidates, rawID)
				}
				if len(candidates) > 0 {
					if match, ok := grouping.LookupVariant(hullMatch.HullKey, candidates...); ok {
						assignment = match
						found = true
					}
				}
			}
		}

		hullKey := strings.ToUpper(rawShipID)
		if found {
			hullKey = assignment.HullKey
		}
		if hullKey == "" {
			continue
		}
		hb := ensureHullBuilder(hullBuilders, grouping, hullKey)
		var vb *variantBuilder
		if found {
			vb = ensureVariantBuilder(variantBuilders, grouping, assignment.HullKey, assignment.VariantCode)
		} else {
			vb = ensureVariantBuilder(variantBuilders, grouping, hullKey, canonicalVariantCode(variantCode))
			if vb == nil {
				vb = ensureVariantBuilder(variantBuilders, grouping, hullKey, canonicalVariantCode("BASE"))
			}
		}
		if vb == nil {
			continue
		}
		if builderName := strings.TrimSpace(name); builderName != "" && vb.Name == "" {
			vb.Name = builderName
		}
		if variantCode != "" && vb.VariantCode == "" {
			vb.VariantCode = sanitizeVariantCode(variantCode)
		}
		if rawID != "" {
			vb.Refs.add("raw:ship_variants.id", rawID)
		}
		if rawShipID != "" {
			if hb != nil {
				hb.Refs.add("raw:ship_variants.ship_id", rawShipID)
			}
			vb.Refs.add("raw:ship_variants.ship_id", rawShipID)
		}
		if variantCode != "" {
			vb.Refs.add("raw:ship_variants.code", variantCode)
		}
		if release := strings.TrimSpace(toString(row["release_patch"])); release != "" {
			releaseCopy := release
			vb.ReleasePatch = &releaseCopy
		}
		if name != "" {
			vb.Refs.add("raw:ship_variants.name", name)
		}
	}
}

func buildV2Bundle(ctx context.Context, channel model.Channel, version string, manufacturers []model.NormalizedManufacturer, ships []model.NormalizedShip, variants []model.NormalizedShipVariant, items []model.NormalizedItem, itemStats []model.NormalizedItemStat, hardpoints []model.NormalizedHardpoint, shipStats []model.NormalizedShipStat, grouping *ShipGrouping, rawShips []map[string]any, rawVariants []map[string]any, hardpointsAsCollection bool) model.NormalizedBundleV2 {
	companyMap := map[string]model.NormalizedCompanyV2{}
	for _, manufacturer := range manufacturers {
		company := model.NormalizedCompanyV2{
			Code: manufacturer.Code,
		}
		if manufacturer.Name != "" {
			name := manufacturer.Name
			company.Name = &name
		}
		companyMap[manufacturer.Code] = company
	}
	companies := make([]model.NormalizedCompanyV2, 0, len(companyMap))
	for _, company := range companyMap {
		companies = append(companies, company)
	}
	sort.Slice(companies, func(i, j int) bool {
		return companies[i].Code < companies[j].Code
	})

	shipSizes := map[string]string{}
	for _, ship := range ships {
		if ship.Size == nil {
			continue
		}
		size := strings.TrimSpace(*ship.Size)
		if size == "" {
			continue
		}
		hullKey := strings.ToUpper(strings.TrimSpace(ship.ExternalID))
		if hullKey == "" {
			continue
		}
		shipSizes[hullKey] = size
	}

	variantSizes := map[string]string{}
	for _, variant := range variants {
		variantKey := strings.ToUpper(strings.TrimSpace(variant.ExternalID))
		hullKey := strings.ToUpper(strings.TrimSpace(variant.ShipExternalID))
		if variantKey == "" || hullKey == "" {
			continue
		}
		if size, ok := shipSizes[hullKey]; ok && size != "" {
			variantSizes[variantKey] = size
		}
	}

	shipStatsMap := map[string]model.ShipVariantStatsV2{}
	for _, stat := range shipStats {
		raw := cloneAnyMap(stat.Stats)
		if raw == nil {
			raw = map[string]any{}
		}
		variantKey := strings.ToUpper(strings.TrimSpace(stat.ShipVariantExternalID))
		if size, ok := variantSizes[variantKey]; ok && size != "" {
			if _, exists := raw["size"]; !exists {
				raw["size"] = size
			}
		}
		shipStatsMap[stat.ShipVariantExternalID] = model.ShipVariantStatsV2{
			Raw: raw,
		}
	}

	hardpointsByVariant := map[string][]model.NormalizedHardpointV2{}
	for _, hardpoint := range hardpoints {
		entry := model.NormalizedHardpointV2{
			ExternalID:          hardpoint.ExternalID,
			ShipVariantExternal: hardpoint.ShipVariantExternalID,
			Code:                hardpoint.Code,
			Category:            hardpoint.Category,
			Position:            hardpoint.Position,
			Size:                hardpoint.Size,
			Gimballed:           hardpoint.Gimballed,
			Powered:             hardpoint.Powered,
			Seats:               hardpoint.Seats,
		}
		hardpointsByVariant[hardpoint.ShipVariantExternalID] = append(hardpointsByVariant[hardpoint.ShipVariantExternalID], entry)
	}
	for key, bucket := range hardpointsByVariant {
		sort.Slice(bucket, func(i, j int) bool {
			return bucket[i].ExternalID < bucket[j].ExternalID
		})
		hardpointsByVariant[key] = bucket
	}

	hullBuilders := map[string]*hullBuilder{}
	variantBuilders := map[string]*variantBuilder{}

	for _, ship := range ships {
		hb := ensureHullBuilder(hullBuilders, grouping, ship.ExternalID)
		if hb == nil {
			continue
		}
		if ship.Name != "" {
			hb.Name = ship.Name
		}
		if ship.ManufacturerCode != "" {
			hb.CompanyCode = ship.ManufacturerCode
		}
		hb.Refs.add(primaryRefSource, ship.ExternalID)
	}

	for _, variant := range variants {
		hullKey := strings.ToUpper(strings.TrimSpace(variant.ShipExternalID))
		if hullKey == "" {
			continue
		}
		code := ""
		if variant.VariantCode != nil {
			code = strings.TrimSpace(strings.ToUpper(*variant.VariantCode))
		}
		if code == "" {
			code = "BASE"
		}
		vb := ensureVariantBuilder(variantBuilders, grouping, hullKey, canonicalVariantCode(code))
		if vb == nil {
			continue
		}
		vb.Refs.add(primaryRefSource, variant.ExternalID)
		if variant.Name != nil && strings.TrimSpace(*variant.Name) != "" {
			vb.Name = *variant.Name
		}
		if code != "" {
			vb.VariantCode = sanitizeVariantCode(code)
		}
		if variant.Thumbnail != nil {
			vb.Thumbnail = variant.Thumbnail
		}
		if stats, ok := shipStatsMap[variant.ExternalID]; ok {
			vb.Stats = stats
		}
		hb := ensureHullBuilder(hullBuilders, grouping, hullKey)
		if hb != nil {
			hb.Refs.add(primaryRefSource, hullKey)
		}
	}

	if grouping != nil {
		for _, assignment := range grouping.Entries() {
			hb := ensureHullBuilder(hullBuilders, grouping, assignment.HullKey)
			if hb != nil {
				if hb.Name == "" {
					hb.Name = assignment.Name
				}
				if hb.CompanyCode == "" && assignment.Manufacturer != "" {
					hb.CompanyCode = strings.ToUpper(assignment.Manufacturer)
				}
				hb.Refs.add(primaryRefSource, assignment.HullKey)
			}
			vb := ensureVariantBuilder(variantBuilders, grouping, assignment.HullKey, assignment.VariantCode)
			if vb != nil {
				if len(assignment.Names) > 0 && strings.TrimSpace(assignment.Names[0]) != "" {
					vb.Name = assignment.Names[0]
				}
				vb.Refs.add(primaryRefSource, vb.ExternalID)
			}
		}
	}

	collectRawShipReferences(grouping, hullBuilders, variantBuilders, rawShips)
	collectRawVariantReferences(grouping, hullBuilders, variantBuilders, rawVariants)

	var hullAliases map[string]string
	hullBuilders, hullAliases = canonicalizeHullBuilders(hullBuilders, grouping)

	if len(variantSizes) > 0 && len(hullAliases) > 0 {
		normalized := map[string]string{}
		for key, size := range variantSizes {
			normalized[strings.ToUpper(key)] = size
			parts := strings.SplitN(strings.ToUpper(key), "_", 2)
			if len(parts) == 2 {
				if canonical, ok := hullAliases[parts[0]]; ok && canonical != "" {
					newKey := canonical + "_" + parts[1]
					if _, exists := normalized[newKey]; !exists {
						normalized[newKey] = size
					}
				}
			}
		}
		variantSizes = normalized
	}

	variantBuilders = canonicalizeVariantBuilders(variantBuilders, grouping, hullBuilders, hullAliases)
	if grouping != nil {
		hullBuilders = filterHullBuildersByGrouping(hullBuilders, grouping)
		variantBuilders = filterVariantBuildersByGrouping(variantBuilders, grouping)
	}

	mergeUEXExternalRefs(ctx, hullBuilders, variantBuilders)
	mergeRSIExternalRefs(hullBuilders, variantBuilders)

	shipV2 := make([]model.NormalizedShipV2, 0, len(hullBuilders))
	for key, builder := range hullBuilders {
		name := builder.Name
		if name == "" {
			name = key
		}
		entry := model.NormalizedShipV2{
			ExternalID:  key,
			Name:        name,
			CompanyCode: builder.CompanyCode,
			ExternalRefs: func() []model.NormalizedExternalReference {
				builder.Refs.add(primaryRefSource, key)
				refs := builder.Refs.list()
				return refs
			}(),
		}
		if len(builder.Paints) > 0 {
			paints := make([]string, 0, len(builder.Paints))
			for paint := range builder.Paints {
				paints = append(paints, paint)
			}
			sort.Strings(paints)
			entry.Paints = paints
		}
		shipV2 = append(shipV2, entry)
	}
	sort.Slice(shipV2, func(i, j int) bool {
		return shipV2[i].ExternalID < shipV2[j].ExternalID
	})

	variantV2 := make([]model.NormalizedShipVariantV2, 0, len(variantBuilders))
	for key, builder := range variantBuilders {
		hb := hullBuilders[builder.HullKey]
		hullName := builder.HullKey
		if hb != nil && strings.TrimSpace(hb.Name) != "" {
			hullName = hb.Name
		}
		name := builder.Name
		if name == "" {
			name = lib.CanonicalVariantName(hullName, lib.CanonicalVariantCode(builder.VariantCode))
		}
		stats := builder.Stats
		if stat, ok := shipStatsMap[key]; ok {
			stats = stat
		}
		if size, ok := variantSizes[key]; ok && size != "" {
			if stats.Raw == nil {
				stats.Raw = map[string]any{}
			}
			if _, exists := stats.Raw["size"]; !exists {
				stats.Raw["size"] = size
			}
		}
		if !hardpointsAsCollection {
			if bucket, ok := hardpointsByVariant[key]; ok && len(bucket) > 0 {
				stats.Hardpoints = append([]model.NormalizedHardpointV2(nil), bucket...)
			}
		}
		builder.Refs.add(primaryRefSource, key)
		refs := builder.Refs.list()
		variantCodeValue := sanitizeVariantCode(builder.VariantCode)
		if variantCodeValue == "" {
			variantCodeValue = "BASE"
		}
		variant := model.NormalizedShipVariantV2{
			ExternalID:   key,
			ShipExternal: builder.HullKey,
			Name:         name,
			ExternalRefs: refs,
			Stats:        stats,
		}
		codeValue := strings.ToUpper(variantCodeValue)
		variant.VariantCode = &codeValue
		if builder.Thumbnail != nil {
			variant.Thumbnail = builder.Thumbnail
		}
		if builder.ReleasePatch != nil {
			variant.ReleasePatch = builder.ReleasePatch
		}
		detection := lib.DetectEditionOrLivery(name, lib.CanonicalVariantCode(codeValue))
		if detection.EditionCode != "" {
			codeOverride := detection.EditionCode
			variant.VariantCode = &codeOverride
		}
		if detection.Livery != nil {
			variant.ReleasePatch = detection.Livery
		}
		variantV2 = append(variantV2, variant)
	}
	sort.Slice(variantV2, func(i, j int) bool {
		return variantV2[i].ExternalID < variantV2[j].ExternalID
	})

	statsByItem := map[string]map[string]any{}
	for _, stat := range itemStats {
		key := strings.ToUpper(strings.TrimSpace(stat.ItemExternalID))
		if key == "" {
			continue
		}
		payload := cloneAnyMap(stat.Stats)
		if payload == nil {
			payload = map[string]any{}
		}
		if stat.PriceAUEC != nil {
			payload["price_auec"] = *stat.PriceAUEC
		}
		if stat.Availability != nil {
			if availability := strings.TrimSpace(*stat.Availability); availability != "" {
				payload["availability"] = availability
			}
		}
		statsByItem[key] = payload
	}

	itemV2 := make([]model.NormalizedItemV2, 0, len(items))
	for _, item := range items {
		itemKey := strings.ToUpper(strings.TrimSpace(item.ExternalID))
		stats := cloneAnyMap(statsByItem[itemKey])
		if stats == nil {
			stats = map[string]any{}
		}
		entry := model.NormalizedItemV2{
			ExternalID:   item.ExternalID,
			Name:         item.Name,
			Type:         item.Type,
			Stats:        stats,
			ExternalRefs: []model.NormalizedExternalReference{},
		}
		if item.ManufacturerCode != nil {
			entry.CompanyCode = item.ManufacturerCode
		}
		if item.Subtype != nil {
			subtype := *item.Subtype
			entry.Subtype = &subtype
		}
		if item.Size != nil {
			size := *item.Size
			entry.Size = &size
		}
		if item.Grade != nil {
			grade := *item.Grade
			entry.Grade = &grade
		}
		if item.Class != nil {
			classValue := *item.Class
			entry.Class = &classValue
		}
		if item.Description != nil {
			description := *item.Description
			entry.Description = &description
		}
		itemV2 = append(itemV2, entry)
	}
	sort.Slice(itemV2, func(i, j int) bool {
		return itemV2[i].ExternalID < itemV2[j].ExternalID
	})

	var hardpointV2 []model.NormalizedHardpointV2
	if hardpointsAsCollection {
		variantKeys := make([]string, 0, len(hardpointsByVariant))
		for key := range hardpointsByVariant {
			variantKeys = append(variantKeys, key)
		}
		sort.Strings(variantKeys)
		hardpointV2 = make([]model.NormalizedHardpointV2, 0, len(hardpoints))
		for _, variantKey := range variantKeys {
			hardpointV2 = append(hardpointV2, hardpointsByVariant[variantKey]...)
		}
	}

	return model.NormalizedBundleV2{
		Channel:      channel,
		Version:      version,
		Companies:    companies,
		Ships:        shipV2,
		ShipVariants: variantV2,
		Items:        itemV2,
		Hardpoints:   hardpointV2,
	}
}

func shipNameByID(ships []model.NormalizedShip, externalID string) string {
	for _, ship := range ships {
		if strings.EqualFold(ship.ExternalID, externalID) {
			return ship.Name
		}
	}
	return externalID
}

func cloneAnyMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	if len(source) == 0 {
		return map[string]any{}
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
