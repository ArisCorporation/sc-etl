package load

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/transform"
)

type resolvedConfiguration struct {
	ConfigurationID   string
	ConfigurationCode string
	VariantExternal   string
	ShipVariantIDs    []string
	Profiles          []string
	IsBase            bool
}

func (b *builder) syncShipConfigurations(grouping *transform.ShipGrouping, variants []model.NormalizedShipVariantV2, variantIDs map[string]string) ([]resolvedConfiguration, error) {
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.ShipConfigurations, []string{"id", "ship_variant", "ship_variant.id", "code", "name"}, nil)
	if err != nil {
		return nil, err
	}
	existingByVariant := map[string]map[string]map[string]any{}
	for _, row := range rows {
		variantID := extractID(row["ship_variant"])
		if variantID == "" {
			continue
		}
		code := strings.ToUpper(normalizeString(row["code"]))
		if code == "" {
			continue
		}
		bucket := existingByVariant[variantID]
		if bucket == nil {
			bucket = map[string]map[string]any{}
			existingByVariant[variantID] = bucket
		}
		bucket[code] = row
	}

	resolved := []resolvedConfiguration{}
	var toDelete []string

	for _, variant := range variants {
		external := strings.ToUpper(strings.TrimSpace(variant.ExternalID))
		if external == "" {
			continue
		}
		variantID := variantIDs[external]
		if variantID == "" {
			continue
		}
		variantExisting := existingByVariant[variantID]
		if variantExisting == nil {
			variantExisting = map[string]map[string]any{}
			existingByVariant[variantID] = variantExisting
		}

		definitions := buildConfigurationsForVariant(grouping, variant)
		for _, def := range definitions {
			code := strings.ToUpper(def.Code)
			if code == "" {
				continue
			}
			name := strings.TrimSpace(def.Name)
			if name == "" {
				name = humanizeCode(code)
			}
			row, exists := variantExisting[code]
			var configID string
			if exists {
				existingName := normalizeString(row["name"])
				if existingName != name {
					if _, err := b.client.UpdateOne(b.ctx, b.collections.ShipConfigurations, toString(row["id"]), map[string]any{"name": name}); err != nil {
						return nil, fmt.Errorf("update ship_configuration %s: %w", toString(row["id"]), err)
					}
				}
				configID = toString(row["id"])
				delete(variantExisting, code)
			} else {
				payload := map[string]any{
					"ship_variant": variantID,
					"code":         code,
					"name":         name,
				}
				created, err := b.client.CreateOne(b.ctx, b.collections.ShipConfigurations, payload)
				if err != nil {
					return nil, fmt.Errorf("create ship_configuration for %s: %w", external, err)
				}
				configID = toString(created["id"])
			}
			resolved = append(resolved, resolvedConfiguration{
				ConfigurationID:   configID,
				ConfigurationCode: code,
				VariantExternal:   external,
				ShipVariantIDs:    def.ShipVariantIDs,
				Profiles:          def.Profiles,
				IsBase:            def.IsBase,
			})
		}
		for _, row := range variantExisting {
			toDelete = append(toDelete, toString(row["id"]))
		}
	}

	if len(toDelete) > 0 {
		for _, batch := range chunkStrings(toDelete, 100) {
			if err := b.client.DeleteMany(b.ctx, b.collections.ShipConfigurations, batch); err != nil {
				return nil, err
			}
		}
	}

	return resolved, nil
}

func (b *builder) syncShipConfigurationHardpoints(configs []resolvedConfiguration, installed []model.NormalizedInstalledItem, hardpointIDMap map[string]string, itemIDMap map[string]string) error {
	existingRows, err := fetchAllRows(b.ctx, b.client, b.collections.ShipConfigurationHP, []string{"id", "configuration", "configuration.id", "hardpoint", "hardpoint.id", "item", "item.id", "quantity"}, nil)
	if err != nil {
		return err
	}
	existingMap := map[string]map[string]any{}
	for _, row := range existingRows {
		configID := extractID(row["configuration"])
		if configID == "" {
			configID = extractID(row["configuration.id"])
		}
		hardpointID := extractID(row["hardpoint"])
		if hardpointID == "" {
			hardpointID = extractID(row["hardpoint.id"])
		}
		itemID := extractID(row["item"])
		if itemID == "" {
			itemID = extractID(row["item.id"])
		}
		if configID == "" || hardpointID == "" || itemID == "" {
			continue
		}
		key := configurationHPKey(configID, hardpointID, itemID)
		existingMap[key] = row
	}

	configByVariant := map[string][]resolvedConfiguration{}
	for _, config := range configs {
		key := strings.ToUpper(config.VariantExternal)
		configByVariant[key] = append(configByVariant[key], config)
	}

	desired := map[string]struct {
		Configuration string
		Hardpoint     string
		Item          string
		Quantity      int
	}{}

	for _, inst := range installed {
		variant := strings.ToUpper(strings.TrimSpace(inst.ShipVariantExternalID))
		configsForVariant, ok := configByVariant[variant]
		if !ok || len(configsForVariant) == 0 {
			continue
		}
		if inst.HardpointExternalID == nil {
			continue
		}
		hardpointID := hardpointIDMap[strings.ToUpper(strings.TrimSpace(*inst.HardpointExternalID))]
		if hardpointID == "" {
			continue
		}
		itemID := itemIDMap[strings.ToUpper(strings.TrimSpace(inst.ItemExternalID))]
		if itemID == "" {
			continue
		}
		quantity := inst.Quantity
		if quantity <= 0 {
			quantity = 1
		}
		profile := ""
		if inst.Profile != nil {
			profile = strings.ToUpper(strings.TrimSpace(*inst.Profile))
		}
		matched := selectConfigurations(configsForVariant, variant, profile)
		for _, cfg := range matched {
			key := configurationHPKey(cfg.ConfigurationID, hardpointID, itemID)
			entry := desired[key]
			if entry.Configuration == "" {
				entry.Configuration = cfg.ConfigurationID
				entry.Hardpoint = hardpointID
				entry.Item = itemID
			}
			entry.Quantity += quantity
			desired[key] = entry
		}
	}

	var toCreate []map[string]any
	var toUpdate []map[string]any
	var toDelete []string

	for key, payload := range desired {
		if existing, ok := existingMap[key]; ok {
			existingQuantity, _ := extractInt(existing["quantity"])
			existingQty := 0
			if existingQuantity != nil {
				existingQty = *existingQuantity
			}
			if existingQty != payload.Quantity {
				toUpdate = append(toUpdate, map[string]any{"id": existing["id"], "quantity": payload.Quantity})
			}
			delete(existingMap, key)
		} else {
			toCreate = append(toCreate, map[string]any{
				"configuration": payload.Configuration,
				"hardpoint":     payload.Hardpoint,
				"item":          payload.Item,
				"quantity":      payload.Quantity,
			})
		}
	}

	for _, row := range existingMap {
		toDelete = append(toDelete, toString(row["id"]))
	}

	if err := bulkDelete(b.ctx, b.client, b.collections.ShipConfigurationHP, toDelete); err != nil {
		return err
	}
	if err := bulkCreate(b.ctx, b.client, b.collections.ShipConfigurationHP, toCreate); err != nil {
		return err
	}
	if err := bulkUpdate(b.ctx, b.client, b.collections.ShipConfigurationHP, toUpdate); err != nil {
		return err
	}
	return nil
}

func configurationHPKey(configurationID, hardpointID, itemID string) string {
	return configurationID + "|" + hardpointID + "|" + itemID
}

func chunkStrings(values []string, size int) [][]string {
	if size <= 0 {
		size = 100
	}
	var chunks [][]string
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunk := append([]string(nil), values[start:end]...)
		chunks = append(chunks, chunk)
	}
	return chunks
}

func bulkDelete(ctx context.Context, client *directus.Client, collection string, ids []string) error {
	for _, batch := range chunkStrings(ids, 100) {
		if len(batch) == 0 {
			continue
		}
		if err := client.DeleteMany(ctx, collection, batch); err != nil {
			return err
		}
	}
	return nil
}

func bulkCreate(ctx context.Context, client *directus.Client, collection string, payloads []map[string]any) error {
	for _, batch := range chunkPayloads(payloads, 100) {
		if len(batch) == 0 {
			continue
		}
		if _, err := client.CreateMany(ctx, collection, batch); err != nil {
			return err
		}
	}
	return nil
}

func bulkUpdate(ctx context.Context, client *directus.Client, collection string, payloads []map[string]any) error {
	for _, batch := range chunkPayloads(payloads, 100) {
		if len(batch) == 0 {
			continue
		}
		if _, err := client.UpdateMany(ctx, collection, batch); err != nil {
			return err
		}
	}
	return nil
}

func chunkPayloads(values []map[string]any, size int) [][]map[string]any {
	if size <= 0 {
		size = 100
	}
	var chunks [][]map[string]any
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunk := append([]map[string]any(nil), values[start:end]...)
		chunks = append(chunks, chunk)
	}
	return chunks
}

func fetchHardpointIDMap(ctx context.Context, client *directus.Client, collection string) (map[string]string, error) {
	rows, err := fetchAllRows(ctx, client, collection, []string{"id", "external_id"}, nil)
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	for _, row := range rows {
		external := strings.ToUpper(normalizeString(row["external_id"]))
		if external == "" {
			continue
		}
		id := toString(row["id"])
		if existingID, ok := result[external]; ok {
			if preferHardpointID(id, existingID) {
				result[external] = id
			}
			continue
		}
		result[external] = id
	}
	return result, nil
}

type configurationDefinition struct {
	Code           string
	Name           string
	ShipVariantIDs []string
	Profiles       []string
	IsBase         bool
}

func buildConfigurationsForVariant(grouping *transform.ShipGrouping, variant model.NormalizedShipVariantV2) []configurationDefinition {
	result := []configurationDefinition{}
	variantExternal := strings.ToUpper(strings.TrimSpace(variant.ExternalID))
	name := strings.TrimSpace(variant.Name)
	if name == "" {
		name = humanizeCode("BASE")
	}
	result = append(result, configurationDefinition{
		Code:           "BASE",
		Name:           name,
		ShipVariantIDs: []string{variantExternal},
		Profiles:       nil,
		IsBase:         true,
	})

	if grouping == nil {
		return result
	}
	assignment, ok := grouping.LookupVariant(variant.ShipExternal, stringFromPtr(variant.VariantCode))
	if !ok {
		assignment, ok = grouping.LookupShipVariantID(variant.ExternalID)
	}
	if !ok {
		assignment, ok = grouping.LookupShipID(variant.ShipExternal, variant.ExternalID)
	}
	if !ok {
		return result
	}
	for _, config := range assignment.Configurations {
		code := strings.ToUpper(strings.TrimSpace(config.Code))
		if code == "" || code == "BASE" {
			continue
		}
		cfgName := strings.TrimSpace(config.Name)
		if cfgName == "" {
			cfgName = humanizeCode(code)
		}
		shipVariantIDs := config.ShipVariantIDs
		if len(shipVariantIDs) == 0 {
			shipVariantIDs = []string{variant.ExternalID}
		}
		result = append(result, configurationDefinition{
			Code:           code,
			Name:           cfgName,
			ShipVariantIDs: normalizeIDList(shipVariantIDs),
			Profiles:       normalizeIDList(config.Profiles),
			IsBase:         false,
		})
	}
	return result
}

func selectConfigurations(configs []resolvedConfiguration, variant string, profile string) []*resolvedConfiguration {
	var matches []*resolvedConfiguration
	variant = strings.ToUpper(strings.TrimSpace(variant))
	profile = strings.ToUpper(strings.TrimSpace(profile))
	for i := range configs {
		cfg := &configs[i]
		if cfg.IsBase {
			continue
		}
		if !matchesVariant(cfg.ShipVariantIDs, variant) {
			continue
		}
		if len(cfg.Profiles) > 0 {
			if profile == "" || !containsString(cfg.Profiles, profile) {
				continue
			}
		}
		matches = append(matches, cfg)
	}
	if len(matches) > 0 {
		return matches
	}
	for i := range configs {
		cfg := &configs[i]
		if !cfg.IsBase {
			continue
		}
		if matchesVariant(cfg.ShipVariantIDs, variant) {
			return []*resolvedConfiguration{cfg}
		}
	}
	return nil
}

func humanizeCode(code string) string {
	code = strings.ReplaceAll(strings.TrimSpace(strings.ToLower(code)), "_", " ")
	if code == "" {
		return ""
	}
	runes := []rune(code)
	capitalize := true
	for i, r := range runes {
		if capitalize && unicode.IsLetter(r) {
			runes[i] = unicode.ToUpper(r)
			capitalize = false
		} else if r == ' ' {
			capitalize = true
		}
	}
	return string(runes)
}

func normalizeIDList(values []string) []string {
	set := map[string]struct{}{}
	result := []string{}
	for _, value := range values {
		v := strings.ToUpper(strings.TrimSpace(value))
		if v == "" {
			continue
		}
		if _, ok := set[v]; ok {
			continue
		}
		set[v] = struct{}{}
		result = append(result, v)
	}
	return result
}

func matchesVariant(candidates []string, variant string) bool {
	if len(candidates) == 0 {
		return true
	}
	variant = strings.ToUpper(strings.TrimSpace(variant))
	for _, candidate := range candidates {
		if strings.ToUpper(strings.TrimSpace(candidate)) == variant {
			return true
		}
	}
	return false
}

func containsString(list []string, candidate string) bool {
	candidate = strings.ToUpper(strings.TrimSpace(candidate))
	for _, value := range list {
		if strings.ToUpper(strings.TrimSpace(value)) == candidate {
			return true
		}
	}
	return false
}
