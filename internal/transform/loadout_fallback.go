package transform

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/lib"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

// shipRecord captures aggregated ship metadata alongside optional loadout entries.
type shipRecord struct {
	ExternalID string
	Ship       map[string]any
	Loadout    []map[string]any
}

type loadoutInstallAggregate struct {
	ShipVariant string
	ItemID      string
	Hardpoint   string
	Quantity    int
	Profile     string
	Livery      string
}

type loadoutVariantMeta struct {
	VariantID string
	Profile   string
	Livery    string
}

var zeroUUIDPattern = regexp.MustCompile(`^0{8}-0{4}-0{4}-0{4}-0{12}$`)

func buildLoadoutFallback(rawDir string, grouping *ShipGrouping, aggregatedShips []map[string]any, items map[string]model.NormalizedItem) ([]model.NormalizedHardpoint, []model.NormalizedInstalledItem, error) {
	records, err := readShipRecordsWithLoadouts(rawDir, aggregatedShips)
	if err != nil {
		return nil, nil, err
	}
	if len(records) == 0 {
		return nil, nil, nil
	}
	hardpointMap := map[string]model.NormalizedHardpoint{}
	installMap := map[string]*loadoutInstallAggregate{}
	itemSet := makeItemSet(items)
	missingItems := map[string]struct{}{}
	for _, record := range records {
		if len(record.Loadout) == 0 {
			continue
		}
		assignment, ok := resolveRecordAssignment(record, grouping)
		if !ok {
			continue
		}
		meta := resolveLoadoutVariantMeta(record, assignment, grouping)
		if meta.VariantID == "" {
			continue
		}
		processLoadoutEntries(record.Loadout, meta.VariantID, hardpointMap, installMap, itemSet, meta.Profile, meta.Livery, missingItems)
	}
	hardpoints := flattenHardpointMap(hardpointMap)
	installed := flattenInstallMap(installMap)
	return hardpoints, installed, nil
}

func makeItemSet(items map[string]model.NormalizedItem) map[string]struct{} {
	if len(items) == 0 {
		return map[string]struct{}{}
	}
	result := map[string]struct{}{}
	for key := range items {
		normalized := strings.ToUpper(strings.TrimSpace(key))
		if normalized == "" {
			continue
		}
		result[normalized] = struct{}{}
	}
	return result
}

func flattenHardpointMap(hardpoints map[string]model.NormalizedHardpoint) []model.NormalizedHardpoint {
	if len(hardpoints) == 0 {
		return nil
	}
	result := make([]model.NormalizedHardpoint, 0, len(hardpoints))
	for _, hp := range hardpoints {
		result = append(result, hp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ExternalID < result[j].ExternalID
	})
	return result
}

func flattenInstallMap(installs map[string]*loadoutInstallAggregate) []model.NormalizedInstalledItem {
	if len(installs) == 0 {
		return nil
	}
	result := make([]model.NormalizedInstalledItem, 0, len(installs))
	for _, entry := range installs {
		item := model.NormalizedInstalledItem{
			ShipVariantExternalID: entry.ShipVariant,
			ItemExternalID:        entry.ItemID,
			Quantity:              entry.Quantity,
		}
		if entry.Hardpoint != "" {
			hp := entry.Hardpoint
			item.HardpointExternalID = &hp
		}
		if entry.Profile != "" {
			value := entry.Profile
			item.Profile = &value
		}
		if entry.Livery != "" {
			value := entry.Livery
			item.Livery = &value
		}
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		a := result[i]
		b := result[j]
		if a.ShipVariantExternalID != b.ShipVariantExternalID {
			return a.ShipVariantExternalID < b.ShipVariantExternalID
		}
		if a.ItemExternalID != b.ItemExternalID {
			return a.ItemExternalID < b.ItemExternalID
		}
		aHP := ""
		if a.HardpointExternalID != nil {
			aHP = *a.HardpointExternalID
		}
		bHP := ""
		if b.HardpointExternalID != nil {
			bHP = *b.HardpointExternalID
		}
		if aHP != bHP {
			return aHP < bHP
		}
		aProfile := ""
		if a.Profile != nil {
			aProfile = *a.Profile
		}
		bProfile := ""
		if b.Profile != nil {
			bProfile = *b.Profile
		}
		if aProfile != bProfile {
			return aProfile < bProfile
		}
		aLivery := ""
		if a.Livery != nil {
			aLivery = *a.Livery
		}
		bLivery := ""
		if b.Livery != nil {
			bLivery = *b.Livery
		}
		return aLivery < bLivery
	})
	return result
}

func resolveLoadoutVariantMeta(record shipRecord, assignment VariantAssignment, grouping *ShipGrouping) loadoutVariantMeta {
	meta := loadoutVariantMeta{}
	if grouping == nil {
		return meta
	}
	hullKey := strings.TrimSpace(strings.ToUpper(assignment.HullKey))
	if hullKey == "" {
		return meta
	}
	variantCode := assignment.VariantCode
	var editionOverride lib.CanonicalVariantCode
	if lib.IsEditionVariantCode(variantCode) {
		editionOverride = variantCode
		variantCode = lib.CanonicalVariantCode("BASE")
	}
	meta.VariantID = canonicalVariantID(hullKey, variantCode)
	displayName := deriveDisplayName(record)
	candidates := resolveConfigurationCandidates(record, displayName)
	configurationCode := grouping.ResolveConfiguration(assignment, candidates...)
	forced := lib.CanonicalVariantCode(configurationCode)
	if forced == "" && editionOverride != "" {
		forced = editionOverride
	}
	detection := lib.DetectEditionOrLivery(displayName, forced)
	meta.Profile = strings.ToUpper(strings.TrimSpace(detection.EditionCode))
	if detection.Livery != nil {
		meta.Livery = strings.TrimSpace(*detection.Livery)
	}
	return meta
}

func deriveDisplayName(record shipRecord) string {
	if record.Ship == nil {
		return record.ExternalID
	}
	candidates := []string{
		optionalString(record.Ship["name"]),
		optionalString(record.Ship["Name"]),
		optionalString(record.Ship["ClassName"]),
		record.ExternalID,
	}
	for _, candidate := range candidates {
		if value := strings.TrimSpace(candidate); value != "" {
			return value
		}
	}
	return record.ExternalID
}

func resolveConfigurationCandidates(record shipRecord, displayName string) []string {
	candidates := []string{}
	appendCandidate := func(value string) {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			candidates = append(candidates, trimmed)
		}
	}
	appendCandidate(record.ExternalID)
	if record.Ship != nil {
		appendCandidate(optionalString(record.Ship["ClassName"]))
		appendCandidate(optionalString(record.Ship["UUID"]))
		appendCandidate(optionalString(record.Ship["Name"]))
		appendCandidate(optionalString(record.Ship["name"]))
	}
	appendCandidate(displayName)
	return candidates
}

func readShipRecordsWithLoadouts(rawDir string, aggregatedShips []map[string]any) ([]shipRecord, error) {
	records := map[string]*shipRecord{}
	for _, row := range aggregatedShips {
		if row == nil {
			continue
		}
		external := firstNonEmpty(
			getString(row, "id"),
			getString(row, "uuid"),
			getString(row, "UUID"),
			getString(row, "ClassName"),
			getString(row, "Name"),
		)
		if external == "" {
			continue
		}
		key := strings.ToUpper(external)
		copyRow := map[string]any{}
		for k, v := range row {
			copyRow[k] = v
		}
		records[key] = &shipRecord{
			ExternalID: key,
			Ship:       copyRow,
			Loadout:    nil,
		}
	}

	shipsDir := filepath.Join(rawDir, "ships")
	dirEntries, err := os.ReadDir(shipsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return flattenShipRecords(records), nil
		}
		return nil, fmt.Errorf("read ships directory: %w", err)
	}
	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		if strings.HasSuffix(strings.ToLower(name), "-raw.json") {
			continue
		}
		basePath := filepath.Join(shipsDir, name)
		basePayload, err := utils.ReadJSONGeneric(basePath)
		if err != nil {
			utils.Logger().Warn("Failed to read ship file", "file", basePath, "error", err)
			continue
		}

		rawPath := strings.TrimSuffix(basePath, ".json") + "-raw.json"
		loadout := []map[string]any{}
		if exists, err := utils.PathExists(rawPath); err == nil && exists {
			rawBytes, err := utils.ReadJSONBytes(rawPath)
			if err != nil {
				utils.Logger().Warn("Failed to read ship raw file", "file", rawPath, "error", err)
			} else {
				var rawPayload map[string]any
				if err := json.Unmarshal(rawBytes, &rawPayload); err != nil {
					utils.Logger().Warn("Failed to parse ship raw file", "file", rawPath, "error", err)
				} else {
					loadout = toMapArray(getArray(rawPayload, "Loadout"))
					if scVehicle, ok := rawPayload["ScVehicle"].(map[string]any); ok {
						for key, value := range scVehicle {
							if _, exists := basePayload[key]; !exists {
								basePayload[key] = value
							}
						}
					}
					if rawVehicle := extractVehicleDefinition(rawPayload); rawVehicle != "" {
						basePayload["vehicleDefinition"] = rawVehicle
					}
				}
			}
		}

		candidates := []string{
			getString(basePayload, "id"),
			getString(basePayload, "UUID"),
			getString(basePayload, "ClassName"),
			getString(basePayload, "Name"),
			strings.TrimSuffix(name, ".json"),
		}
		external := firstNonEmpty(candidates...)
		if external == "" {
			utils.Logger().Warn("Ship loadout file missing identifier", "file", basePath)
			continue
		}
		key := strings.ToUpper(external)
		record, ok := records[key]
		if !ok {
			record = &shipRecord{
				ExternalID: key,
			}
			records[key] = record
		}
		record.Ship = mergeShipMaps(record.Ship, basePayload)
		if len(loadout) > 0 {
			record.Loadout = loadout
		}
	}
	return flattenShipRecords(records), nil
}

func extractVehicleDefinition(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	if nested, ok := raw["Raw"].(map[string]any); ok {
		if entity, ok := nested["Entity"].(map[string]any); ok {
			if components, ok := entity["Components"].(map[string]any); ok {
				if vehicle, ok := components["VehicleComponentParams"].(map[string]any); ok {
					if value := strings.TrimSpace(toString(vehicle["vehicleDefinition"])); value != "" {
						return value
					}
				}
			}
		}
	}
	if vehicle, ok := raw["VehicleComponentParams"].(map[string]any); ok {
		if value := strings.TrimSpace(toString(vehicle["vehicleDefinition"])); value != "" {
			return value
		}
	}
	return ""
}

func mergeShipMaps(existing map[string]any, next map[string]any) map[string]any {
	if existing == nil {
		existing = map[string]any{}
	}
	for key, value := range next {
		existing[key] = value
	}
	return existing
}

func flattenShipRecords(records map[string]*shipRecord) []shipRecord {
	if len(records) == 0 {
		return nil
	}
	result := make([]shipRecord, 0, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}
		result = append(result, *record)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ExternalID < result[j].ExternalID
	})
	return result
}

func resolveRecordAssignment(record shipRecord, grouping *ShipGrouping) (VariantAssignment, bool) {
	if grouping == nil {
		return VariantAssignment{}, false
	}
	candidates := []string{record.ExternalID}
	if record.Ship != nil {
		for _, key := range []string{"id", "ID", "uuid", "UUID", "ClassName", "Name", "vehicleDefinition"} {
			if value := strings.TrimSpace(toString(record.Ship[key])); value != "" {
				candidates = append(candidates, value)
			}
		}
		if manufacturer, ok := record.Ship["manufacturer"].(map[string]any); ok {
			if code := strings.TrimSpace(toString(manufacturer["code"])); code != "" {
				candidates = append(candidates, code)
			}
		}
		if manufacturer, ok := record.Ship["Manufacturer"].(map[string]any); ok {
			if code := strings.TrimSpace(toString(manufacturer["Code"])); code != "" {
				candidates = append(candidates, code)
			}
		}
	}
	return grouping.LookupShipID(candidates...)
}

func processLoadoutEntries(entries []map[string]any, variantID string, output map[string]model.NormalizedHardpoint, installs map[string]*loadoutInstallAggregate, knownItems map[string]struct{}, profile string, livery string, missingItems map[string]struct{}) {
	if len(entries) == 0 {
		return
	}
	type stackEntry struct {
		entry map[string]any
		path  []string
	}
	stack := make([]stackEntry, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		stack = append(stack, stackEntry{entry: entry, path: nil})
	}
	index := 0
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]

		portName := derivePortName(current.entry, index)
		index++
		pathTokens := append([]string{}, current.path...)
		pathTokens = append(pathTokens, portName)
		path := strings.Join(pathTokens, "/")
		externalID := fmt.Sprintf("%s:%s", variantID, path)
		key := strings.ToUpper(externalID)

		if _, exists := output[key]; !exists {
			category := deriveAttachCategory(current.entry)
			sizePtr := deriveAttachSize(current.entry)
			hardpoint := model.NormalizedHardpoint{
				ExternalID:            externalID,
				ShipVariantExternalID: variantID,
				Code:                  portName,
				Category:              category,
			}
			if sizePtr != nil {
				hardpoint.Size = sizePtr
			}
			output[key] = hardpoint
		}

		if itemRef := deriveItemReference(current.entry); itemRef != "" {
			itemID := strings.ToUpper(itemRef)
			if _, ok := knownItems[itemID]; ok {
				mapKey := makeInstallKey(variantID, itemID, externalID, profile, livery)
				entry := installs[mapKey]
				if entry == nil {
					entry = &loadoutInstallAggregate{
						ShipVariant: variantID,
						ItemID:      itemID,
						Hardpoint:   externalID,
						Profile:     profile,
						Livery:      livery,
					}
				}
				entry.Quantity++
				installs[mapKey] = entry
			} else if itemID != "" {
				missKey := variantID + "|" + itemID
				if _, seen := missingItems[missKey]; !seen {
					utils.Logger().Warn("Loadout references unknown item", "variant", variantID, "item", itemID)
					missingItems[missKey] = struct{}{}
				}
			}
		}

		children := toMapArray(getArray(current.entry, "entries"))
		if len(children) == 0 {
			continue
		}
		for _, child := range children {
			if child == nil {
				continue
			}
			stack = append(stack, stackEntry{
				entry: child,
				path:  append([]string{}, pathTokens...),
			})
		}
	}
}

func makeInstallKey(variantID, itemID, hardpointID, profile, livery string) string {
	return variantID + "|" + itemID + "|" + hardpointID + "|" + profile + "|" + livery
}

func deriveItemReference(entry map[string]any) string {
	if entry == nil {
		return ""
	}
	candidates := []string{}
	if itemMap, ok := entry["Item"].(map[string]any); ok {
		candidates = append(candidates,
			optionalString(itemMap["__ref"]),
			optionalString(itemMap["classReference"]),
		)
	}
	candidates = append(candidates, optionalString(entry["classReference"]))
	for _, candidate := range candidates {
		value := strings.TrimSpace(candidate)
		if value == "" {
			continue
		}
		if zeroUUIDPattern.MatchString(value) {
			continue
		}
		return value
	}
	return ""
}

func derivePortName(entry map[string]any, index int) string {
	if entry == nil {
		return fmt.Sprintf("slot_%d", index)
	}
	if value := strings.TrimSpace(optionalString(entry["portName"])); value != "" {
		return value
	}
	if value := strings.TrimSpace(optionalString(entry["className"])); value != "" {
		return value
	}
	if value := strings.TrimSpace(optionalString(entry["classReference"])); value != "" {
		return value
	}
	return fmt.Sprintf("slot_%d", index)
}

func deriveAttachCategory(entry map[string]any) string {
	if entry == nil {
		return "Unknown"
	}
	itemMap, _ := entry["Item"].(map[string]any)
	components, _ := itemMap["Components"].(map[string]any)
	attachParams, _ := components["SAttachableComponentParams"].(map[string]any)
	attachDef, _ := attachParams["AttachDef"].(map[string]any)
	if value := strings.TrimSpace(optionalString(attachDef["Type"])); value != "" {
		return value
	}
	if value := strings.TrimSpace(optionalString(attachDef["SubType"])); value != "" {
		return value
	}
	if value := strings.TrimSpace(optionalString(entry["className"])); value != "" {
		return value
	}
	return "Unknown"
}

func deriveAttachSize(entry map[string]any) *int {
	if entry == nil {
		return nil
	}
	itemMap, _ := entry["Item"].(map[string]any)
	components, _ := itemMap["Components"].(map[string]any)
	attachParams, _ := components["SAttachableComponentParams"].(map[string]any)
	attachDef, _ := attachParams["AttachDef"].(map[string]any)
	number := optionalNumber(attachDef["Size"])
	if math.IsNaN(number) {
		return nil
	}
	size := int(math.Round(number))
	if size <= 0 {
		return nil
	}
	return &size
}
