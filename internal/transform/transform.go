package transform

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"

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
}

// Run normalizes the raw Star Citizen data into the bundle consumed by loaders.
func Run(ctx context.Context, dataRoot string, channel model.Channel, version string) (*Result, error) {
	_ = ctx
	channelKey := string(channel)
	rawDir := filepath.Join(dataRoot, "raw", channelKey, version)
	normalizedDir := filepath.Join(dataRoot, "normalized", channelKey, version)

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
	items := normalizeItems(itemRows)
	itemByExternal := map[string]model.NormalizedItem{}
	for _, item := range items {
		itemByExternal[item.ExternalID] = item
	}

	itemStats := normalizeItemStats(itemStatsRows, itemByExternal)
	shipStats := normalizeShipStats(shipStatsRows, variantByExternal)
	installedItems := normalizeInstalledItems(installedRows, variantByExternal, itemByExternal)
	locales := []model.NormalizedLocaleEntry{}

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

	v2 := buildV2Bundle(channel, version, manufacturers, ships, variants, items, hardpoints, shipStats)

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

func normalizeItems(rows []map[string]any) []model.NormalizedItem {
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
		seen[external] = struct{}{}
		typeName := firstNonEmpty(getString(row, "type"), getString(row, "Type"))
		if typeName == "" {
			continue
		}
		name := firstNonEmpty(getString(row, "name"), getString(row, "Name"))
		if name == "" {
			continue
		}
		manufacturerCode := uppercase(firstNonEmpty(getString(row, "manufacturer"), getString(row, "manufacturer_code")))
		item := model.NormalizedItem{
			ExternalID: external,
			Type:       strings.ToUpper(typeName),
			Name:       name,
		}
		if manufacturerCode != "" {
			item.ManufacturerCode = &manufacturerCode
		}
		if subtype := getString(row, "subtype"); subtype != "" {
			item.Subtype = &subtype
		}
		if grade := getString(row, "grade"); grade != "" {
			item.Grade = &grade
		}
		if classValue := getString(row, "class"); classValue != "" {
			item.Class = &classValue
		}
		if sizeValue := getNumber(row, "size"); !math.IsNaN(sizeValue) {
			sizeInt := int(sizeValue)
			item.Size = &sizeInt
		}
		if description := getString(row, "description"); description != "" {
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

func buildV2Bundle(channel model.Channel, version string, manufacturers []model.NormalizedManufacturer, ships []model.NormalizedShip, variants []model.NormalizedShipVariant, items []model.NormalizedItem, hardpoints []model.NormalizedHardpoint, shipStats []model.NormalizedShipStat) model.NormalizedBundleV2 {
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

	shipV2 := make([]model.NormalizedShipV2, 0, len(ships))
	for _, ship := range ships {
		entry := model.NormalizedShipV2{
			ExternalID:  ship.ExternalID,
			Name:        ship.Name,
			CompanyCode: ship.ManufacturerCode,
		}
		entry.ExternalRefs = []model.NormalizedExternalReference{}
		shipV2 = append(shipV2, entry)
	}
	sort.Slice(shipV2, func(i, j int) bool {
		return shipV2[i].ExternalID < shipV2[j].ExternalID
	})

	shipStatsMap := map[string]model.ShipVariantStatsV2{}
	for _, stat := range shipStats {
		shipStatsMap[stat.ShipVariantExternalID] = model.ShipVariantStatsV2{
			Raw: stat.Stats,
		}
	}

	variantV2 := make([]model.NormalizedShipVariantV2, 0, len(variants))
	for _, variant := range variants {
		variantCode := ""
		if variant.VariantCode != nil {
			variantCode = *variant.VariantCode
		}
		variantName := ""
		if variant.Name != nil {
			variantName = *variant.Name
		}
		if variantName == "" {
			variantName = lib.CanonicalVariantName(shipNameByID(ships, variant.ShipExternalID), lib.CanonicalVariantCode(variantCode))
		}
		assignmentName := variantName
		v2 := model.NormalizedShipVariantV2{
			ExternalID:   variant.ExternalID,
			ShipExternal: variant.ShipExternalID,
			Name:         assignmentName,
			ExternalRefs: []model.NormalizedExternalReference{},
			Stats:        shipStatsMap[variant.ExternalID],
		}
		code := lib.CanonicalVariantCode(variantCode)
		if variantCode != "" {
			v2.VariantCode = &variantCode
		}
		detection := lib.DetectEditionOrLivery(variantName, code)
		if detection.EditionCode != "" {
			codeValue := detection.EditionCode
			v2.VariantCode = &codeValue
		}
		if detection.Livery != nil {
			v2.ReleasePatch = detection.Livery
		}
		variantV2 = append(variantV2, v2)
	}
	sort.Slice(variantV2, func(i, j int) bool {
		return variantV2[i].ExternalID < variantV2[j].ExternalID
	})

	itemV2 := make([]model.NormalizedItemV2, 0, len(items))
	for _, item := range items {
		entry := model.NormalizedItemV2{
			ExternalID:   item.ExternalID,
			Name:         item.Name,
			Type:         item.Type,
			Stats:        map[string]any{},
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

	hardpointV2 := make([]model.NormalizedHardpointV2, 0, len(hardpoints))
	for _, hardpoint := range hardpoints {
		entry := model.NormalizedHardpointV2{
			ExternalID:          hardpoint.ExternalID,
			ShipVariantExternal: hardpoint.ShipVariantExternalID,
			Code:                hardpoint.Code,
			Category:            hardpoint.Category,
		}
		entry.Position = hardpoint.Position
		entry.Size = hardpoint.Size
		entry.Gimballed = hardpoint.Gimballed
		entry.Powered = hardpoint.Powered
		entry.Seats = hardpoint.Seats
		hardpointV2 = append(hardpointV2, entry)
	}
	sort.Slice(hardpointV2, func(i, j int) bool {
		return hardpointV2[i].ExternalID < hardpointV2[j].ExternalID
	})

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
