package transform

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/lib"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

// VariantAssignment mirrors the TypeScript structure.
type VariantAssignment struct {
	HullKey        string
	Manufacturer   string
	Name           string
	VariantCode    lib.CanonicalVariantCode
	Names          []string
	Editions       []string
	MatchIDs       []string
	ShipVariantIDs []string
	Configurations []VariantConfiguration
}

// VariantConfiguration stores per-hull configuration metadata.
type VariantConfiguration struct {
	Code           string
	Name           string
	Match          string
	ShipVariantIDs []string
	Profiles       []string
}

// ShipGrouping provides lookup utilities for variant metadata.
type ShipGrouping struct {
	hulls                  map[string]HullDefinition
	canonicalVariants      map[string]VariantAssignment
	perHullVariants        map[string]map[string]VariantAssignment
	shipIDAssignments      map[string]VariantAssignment
	shipVariantAssignments map[string]VariantAssignment
}

func emptyShipGrouping() *ShipGrouping {
	return &ShipGrouping{
		hulls:                  map[string]HullDefinition{},
		canonicalVariants:      map[string]VariantAssignment{},
		perHullVariants:        map[string]map[string]VariantAssignment{},
		shipIDAssignments:      map[string]VariantAssignment{},
		shipVariantAssignments: map[string]VariantAssignment{},
	}
}

// HullDefinition describes a hull entry.
type HullDefinition struct {
	HullKey      string
	Manufacturer string
	Name         string
}

// LoadShipGrouping loads configuration from schemas/ship-groups.json.
func LoadShipGrouping() *ShipGrouping {
	path := filepath.Join("schemas", "ship-groups.json")
	bytes, err := os.ReadFile(path)
	if err != nil {
		utils.Logger().Warn("Ship grouping configuration missing", "error", err)
		return emptyShipGrouping()
	}
	var raw map[string]any
	if err := json.Unmarshal(bytes, &raw); err != nil {
		utils.Logger().Warn("Failed to parse ship grouping configuration", "error", err)
		return emptyShipGrouping()
	}
	grouping := &ShipGrouping{
		hulls:                  map[string]HullDefinition{},
		canonicalVariants:      map[string]VariantAssignment{},
		perHullVariants:        map[string]map[string]VariantAssignment{},
		shipIDAssignments:      map[string]VariantAssignment{},
		shipVariantAssignments: map[string]VariantAssignment{},
	}
	rawHulls, _ := raw["hulls"].(map[string]any)
	for rawHullKey, rawHullValue := range rawHulls {
		hullKey := sanitizeHullKey(rawHullKey)
		rawConfig, _ := rawHullValue.(map[string]any)
		manufacturer := toString(rawConfig["manufacturer"])
		if manufacturer == "" {
			parts := strings.SplitN(hullKey, "_", 2)
			if len(parts) > 0 {
				manufacturer = parts[0]
			} else {
				manufacturer = "UNKNOWN"
			}
		}
		name := toString(rawConfig["name"])
		if name == "" {
			name = hullKey
		}
		hullDef := HullDefinition{
			HullKey:      hullKey,
			Manufacturer: manufacturer,
			Name:         name,
		}
		grouping.hulls[hullKey] = hullDef
		grouping.perHullVariants[hullKey] = map[string]VariantAssignment{}

		rawVariants, _ := rawConfig["variants"].(map[string]any)
		for rawVariantKey, rawVariantValue := range rawVariants {
			variantCode := sanitizeVariantCode(rawVariantKey)
			normalized := normalizeVariant(rawVariantValue)
			assignment := VariantAssignment{
				HullKey:        hullDef.HullKey,
				Manufacturer:   hullDef.Manufacturer,
				Name:           hullDef.Name,
				VariantCode:    variantCode,
				Names:          normalized.Names,
				Editions:       normalized.Editions,
				MatchIDs:       normalized.MatchIDs,
				ShipVariantIDs: normalized.ShipVariantIDs,
				Configurations: normalized.Configurations,
			}
			grouping.registerVariant(hullKey, assignment, normalized)
		}
	}
	return grouping
}

func sanitizeHullKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "HULL"
	}
	return sanitizeToken(value)
}

func sanitizeVariantCode(value string) lib.CanonicalVariantCode {
	value = strings.TrimSpace(value)
	if value == "" {
		return "BASE"
	}
	return lib.CanonicalVariantCode(sanitizeToken(value))
}

func sanitizeToken(value string) string {
	value = strings.ToUpper(value)
	builder := strings.Builder{}
	lastUnderscore := false
	for _, r := range value {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			builder.WriteRune('_')
			lastUnderscore = true
		}
	}
	result := builder.String()
	result = strings.Trim(result, "_")
	result = strings.ReplaceAll(result, "__", "_")
	return result
}

func toString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	case float64:
		return strings.TrimSpace(fmt.Sprintf("%.0f", v))
	case int, int64:
		return strings.TrimSpace(fmt.Sprint(v))
	default:
		return ""
	}
}

type normalizedVariantConfig struct {
	MatchIDs       []string
	ShipVariantIDs []string
	VariantCodes   []lib.CanonicalVariantCode
	Names          []string
	Editions       []string
	Configurations []VariantConfiguration
}

func normalizeVariant(input any) normalizedVariantConfig {
	switch value := input.(type) {
	case string:
		if value == "" {
			return normalizedVariantConfig{}
		}
		return normalizedVariantConfig{
			MatchIDs: []string{value},
		}
	case []any:
		match := []string{}
		for _, entry := range value {
			if s := toString(entry); s != "" {
				match = append(match, s)
			}
		}
		return normalizedVariantConfig{
			MatchIDs: match,
		}
	case map[string]any:
		matchIDs := collectStrings(value["match"], value["match_id"], value["id"], value["ids"], value["matches"])
		if len(matchIDs) == 0 {
			if configs, ok := value["configurations"].(map[string]any); ok && len(configs) > 0 {
				for _, configValue := range configs {
					if entry, ok := configValue.(map[string]any); ok {
						if match := toString(entry["match"]); match != "" {
							matchIDs = append(matchIDs, match)
						}
					}
				}
			}
		}
		shipVariantIDs := collectStrings(value["ship_variant_ids"], value["shipVariantIds"])
		variantCodes := []lib.CanonicalVariantCode{}
		for _, raw := range collectStrings(value["ship_variant_codes"], value["shipVariantCodes"], value["codes"], value["variant_codes"], value["aliases"]) {
			variantCodes = append(variantCodes, sanitizeVariantCode(raw))
		}
		names := collectStrings(value["names"], value["display_names"])
		editions := collectStrings(value["editions"], value["profiles"])

		configs := []VariantConfiguration{}
		if rawConfigs, ok := value["configurations"].(map[string]any); ok {
			for rawCode, configValue := range rawConfigs {
				code := sanitizeToken(rawCode)
				if code == "" {
					code = sanitizeToken(toString(configValue))
				}
				if code == "" {
					continue
				}
				if entry, ok := configValue.(map[string]any); ok {
					name := toString(entry["name"])
					if name == "" {
						name = toString(entry["label"])
					}
					if name == "" {
						name = toString(entry["display"])
					}
					if name == "" {
						name = rawCode
					}
					match := toString(entry["match"])
					if match == "" {
						match = toString(entry["class"])
					}
					if match == "" {
						match = toString(entry["id"])
					}
					configShipVariantIDs := collectStrings(entry["ship_variant_ids"], entry["shipVariantIds"], entry["ids"])
					profiles := collectStrings(entry["profiles"], entry["profile"])
					configs = append(configs, VariantConfiguration{
						Code:           code,
						Name:           name,
						Match:          match,
						ShipVariantIDs: configShipVariantIDs,
						Profiles:       profiles,
					})
				} else {
					match := toString(configValue)
					if match == "" {
						match = rawCode
					}
					configs = append(configs, VariantConfiguration{
						Code:  code,
						Name:  match,
						Match: match,
					})
				}
			}
		}
		return normalizedVariantConfig{
			MatchIDs:       dedupeStrings(matchIDs),
			ShipVariantIDs: dedupeStrings(shipVariantIDs),
			VariantCodes:   variantCodes,
			Names:          dedupeStrings(names),
			Editions:       dedupeStrings(editions),
			Configurations: configs,
		}
	default:
		return normalizedVariantConfig{}
	}
}

func collectStrings(values ...any) []string {
	set := []string{}
	for _, value := range values {
		switch v := value.(type) {
		case string:
			if s := strings.TrimSpace(v); s != "" {
				set = append(set, s)
			}
		case []any:
			for _, entry := range v {
				if s := toString(entry); s != "" {
					set = append(set, s)
				}
			}
		case []string:
			for _, entry := range v {
				if s := strings.TrimSpace(entry); s != "" {
					set = append(set, s)
				}
			}
		}
	}
	return set
}

func normalizeLookupKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.ToLower(value)
}

func (g *ShipGrouping) registerVariant(hullKey string, assignment VariantAssignment, config normalizedVariantConfig) {
	canonicalKey := hullKey + ":" + string(assignment.VariantCode)
	if existing, ok := g.canonicalVariants[canonicalKey]; ok && !assignmentsEqual(existing, assignment) {
		utils.Logger().Warn("Ship grouping config collision on canonical variant", "hull", hullKey, "variant", assignment.VariantCode)
		return
	}
	g.canonicalVariants[canonicalKey] = assignment

	variantLookup := g.perHullVariants[hullKey]
	if variantLookup == nil {
		variantLookup = map[string]VariantAssignment{}
		g.perHullVariants[hullKey] = variantLookup
	}

	variantCodes := map[lib.CanonicalVariantCode]struct{}{
		assignment.VariantCode: {},
	}
	for _, alias := range config.VariantCodes {
		variantCodes[alias] = struct{}{}
	}
	for code := range variantCodes {
		if existing, ok := variantLookup[string(code)]; ok && !assignmentsEqual(existing, assignment) {
			utils.Logger().Warn("Ship grouping variant code collision", "hull", hullKey, "variant", assignment.VariantCode, "code", code)
			continue
		}
		variantLookup[string(code)] = assignment
	}

	matchIDs := config.MatchIDs
	if len(matchIDs) == 0 {
		matchIDs = []string{string(assignment.VariantCode)}
	}
	for _, id := range matchIDs {
		g.registerShipID(id, assignment)
	}
	for _, id := range config.ShipVariantIDs {
		g.registerShipVariantID(id, assignment)
	}
	for _, id := range assignment.ShipVariantIDs {
		g.registerShipVariantID(id, assignment)
	}
	for _, variantConfig := range assignment.Configurations {
		for _, id := range variantConfig.ShipVariantIDs {
			g.registerShipVariantID(id, assignment)
		}
	}
}

func (g *ShipGrouping) registerShipID(id string, assignment VariantAssignment) {
	key := normalizeLookupKey(id)
	if key == "" {
		return
	}
	if existing, ok := g.shipIDAssignments[key]; ok && !assignmentsEqual(existing, assignment) {
		utils.Logger().Warn("Ship grouping ship identifier collision", "id", id, "hull", assignment.HullKey, "variant", assignment.VariantCode)
		return
	}
	g.shipIDAssignments[key] = assignment
}

func (g *ShipGrouping) registerShipVariantID(id string, assignment VariantAssignment) {
	key := normalizeLookupKey(id)
	if key == "" {
		return
	}
	if existing, ok := g.shipVariantAssignments[key]; ok && !assignmentsEqual(existing, assignment) {
		utils.Logger().Warn("Ship grouping ship_variant identifier collision", "id", id, "hull", assignment.HullKey, "variant", assignment.VariantCode)
		return
	}
	g.shipVariantAssignments[key] = assignment
}

func assignmentsEqual(a, b VariantAssignment) bool {
	return a.HullKey == b.HullKey && a.VariantCode == b.VariantCode
}

// GetHull returns a hull definition.
func (g *ShipGrouping) GetHull(hullKey string) (HullDefinition, bool) {
	def, ok := g.hulls[sanitizeHullKey(hullKey)]
	return def, ok
}

// LookupShipID resolves a ship by id candidates.
func (g *ShipGrouping) LookupShipID(candidates ...string) (VariantAssignment, bool) {
	for _, candidate := range candidates {
		key := normalizeLookupKey(candidate)
		if key == "" {
			continue
		}
		if assignment, ok := g.shipIDAssignments[key]; ok {
			return assignment, true
		}
	}
	return VariantAssignment{}, false
}

// LookupShipVariantID resolves a variant by identifier.
func (g *ShipGrouping) LookupShipVariantID(id string) (VariantAssignment, bool) {
	key := normalizeLookupKey(id)
	if key == "" {
		return VariantAssignment{}, false
	}
	assignment, ok := g.shipVariantAssignments[key]
	return assignment, ok
}

// LookupVariant resolves a variant for the given hull and candidate codes.
func (g *ShipGrouping) LookupVariant(hullKey string, candidateCodes ...string) (VariantAssignment, bool) {
	lookup := g.perHullVariants[sanitizeHullKey(hullKey)]
	if lookup == nil {
		return VariantAssignment{}, false
	}
	for _, candidate := range candidateCodes {
		normalized := sanitizeVariantCode(candidate)
		if assignment, ok := lookup[string(normalized)]; ok {
			return assignment, true
		}
	}
	return VariantAssignment{}, false
}

// Entries exposes canonical assignments.
func (g *ShipGrouping) Entries() []VariantAssignment {
	result := make([]VariantAssignment, 0, len(g.canonicalVariants))
	for _, assignment := range g.canonicalVariants {
		result = append(result, assignment)
	}
	return result
}

// ResolveConfiguration finds a configuration code for provided candidates.
func (g *ShipGrouping) ResolveConfiguration(assignment VariantAssignment, candidates ...string) string {
	if len(assignment.Configurations) == 0 {
		return ""
	}
	normalized := []string{}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(strings.ToLower(candidate))
		if candidate != "" {
			normalized = append(normalized, candidate)
		}
	}
	if len(normalized) == 0 {
		return ""
	}
	for _, configuration := range assignment.Configurations {
		targets := map[string]struct{}{}
		addTarget := func(value string) {
			if value == "" {
				return
			}
			targets[strings.TrimSpace(strings.ToLower(value))] = struct{}{}
		}
		addTarget(configuration.Match)
		addTarget(configuration.Name)
		addTarget(configuration.Code)
		for _, profile := range configuration.Profiles {
			addTarget(profile)
		}
		for _, variantID := range configuration.ShipVariantIDs {
			addTarget(variantID)
		}
		for _, candidate := range normalized {
			if _, ok := targets[candidate]; ok {
				return configuration.Code
			}
		}
	}
	return ""
}
