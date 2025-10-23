package transform

import (
	"encoding/json"
	"os"
	"strings"
)

// Config holds runtime options for the transformation pipeline.
type Config struct {
	AllowedItemTypes       map[string]struct{}
	HardpointsAsCollection bool
}

// LoadConfig initialises Config using environment variables with optional overrides.
func LoadConfig(overrides *Config) Config {
	allowed := parseAllowedItemTypes(os.Getenv("ALLOWED_ITEM_TYPES"))
	hardpointsAsCollection := parseBoolean(os.Getenv("HARDPOINTS_AS_COLLECTION"), true)

	if overrides != nil {
		if overrides.AllowedItemTypes != nil {
			allowed = overrides.AllowedItemTypes
		}
		hardpointsAsCollection = overrides.HardpointsAsCollection
	}

	return Config{
		AllowedItemTypes:       allowed,
		HardpointsAsCollection: hardpointsAsCollection,
	}
}

func parseAllowedItemTypes(raw string) map[string]struct{} {
	result := make(map[string]struct{})
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return result
	}
	candidates := []string{}
	if strings.HasPrefix(raw, "[") {
		var jsonCandidates []string
		if err := json.Unmarshal([]byte(raw), &jsonCandidates); err == nil {
			candidates = jsonCandidates
		}
	}
	if len(candidates) == 0 {
		for _, token := range strings.FieldsFunc(raw, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\n' || r == '\t'
		}) {
			candidates = append(candidates, token)
		}
	}
	for _, candidate := range candidates {
		upper := strings.ToUpper(strings.TrimSpace(candidate))
		if upper != "" {
			result[upper] = struct{}{}
		}
	}
	return result
}

func parseBoolean(raw string, defaultValue bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}
