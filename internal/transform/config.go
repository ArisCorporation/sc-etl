package transform

import (
	"encoding/json"
	"os"
	"strings"
)

// Config holds runtime options for the transformation pipeline.
type Config struct {
	AllowedItemTypes       map[string]struct{}
	AllowedHardpointTypes  map[string]struct{}
	HardpointsAsCollection bool
}

var defaultAllowedHardpointTypes = []string{
	"WEAPON", "WEAPONGUN", "WEAPONDEFENSIVE", "WEAPONATTACHMENT", "WEAPONMINING",
	"TURRET", "TURRETBASE", "MANNEDTURRET", "REMOTETURRET", "UTILITYTURRET",
	"MISSILE", "MISSILELAUNCHER",
	"BOMB", "BOMBLAUNCHER",
	"SHIELD",
	"COOLER",
	"POWERPLANT",
	"QUANTUMDRIVE", "QED", "QUANTUMINTERDICTIONGENERATOR",
	"THRUSTER", "MAINTHRUSTER", "MANEUVERTHRUSTER", "RETROTHRUSTER", "VTOLTHRUSTER",
	"FUELTANK", "QUANTUMFUELTANK", "FUELINTAKE",
	"FLIGHTCONTROLLER",
	"LIFESUPPORTGENERATOR", "LIFESUPPORTVENT",
	"RADAR",
	"DEFENSE",
	"CARGO", "CARGOGRID",
	"EMP",
	"MININGCONTROLLER", "MININGMODIFIER",
	"SALVAGECONTROLLER", "SALVAGEFIELDEMITTER", "SALVAGEFIELDSUPPORTER", "SALVAGEFILLERSTATION", "SALVAGEHEAD", "SALVAGEINTERNALSTORAGE", "SALVAGEMODIFIER",
	"UTILITY", "TRACTORBEAM", "TOWINGBEAM", "TOOLARM",
}

// LoadConfig initialises Config using environment variables with optional overrides.
func LoadConfig(overrides *Config) Config {
	allowedItems := parseAllowedTokens(os.Getenv("ALLOWED_ITEM_TYPES"))
	allowedHardpoints := parseAllowedTokens(os.Getenv("ALLOWED_HARDPOINT_TYPES"))
	hardpointsAsCollection := parseBoolean(os.Getenv("HARDPOINTS_AS_COLLECTION"), true)

	if overrides != nil {
		if overrides.AllowedItemTypes != nil {
			allowedItems = overrides.AllowedItemTypes
		}
		if overrides.AllowedHardpointTypes != nil {
			allowedHardpoints = overrides.AllowedHardpointTypes
		}
		hardpointsAsCollection = overrides.HardpointsAsCollection
	}

	if len(allowedHardpoints) == 0 {
		allowedHardpoints = makeTokenSet(defaultAllowedHardpointTypes)
	}

	return Config{
		AllowedItemTypes:       allowedItems,
		AllowedHardpointTypes:  allowedHardpoints,
		HardpointsAsCollection: hardpointsAsCollection,
	}
}

func parseAllowedTokens(raw string) map[string]struct{} {
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

func makeTokenSet(list []string) map[string]struct{} {
	result := make(map[string]struct{}, len(list))
	for _, entry := range list {
		token := strings.ToUpper(strings.TrimSpace(entry))
		if token == "" {
			continue
		}
		result[token] = struct{}{}
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
