package transform

import "strings"

var shipClassificationPrefixes = map[string]struct{}{
	"ship":    {},
	"vehicle": {},
}

var shipItemTypeAllow = map[string]struct{}{
	"AIMODULE":                      {},
	"AMMOBOX":                       {},
	"ARMOR":                         {},
	"ATTACHEDPART":                  {},
	"BOMB":                          {},
	"BOMBLAUNCHER":                  {},
	"CAPACITORASSIGNMENTCONTROLLER": {},
	"CARGO":                         {},
	"CARGOGRID":                     {},
	"COMMSCONTROLLER":               {},
	"CONTAINER":                     {},
	"CONTROLPANEL":                  {},
	"COOLER":                        {},
	"COOLERCONTROLLER":              {},
	"DOCKINGANIMATOR":               {},
	"DOCKINGCOLLAR":                 {},
	"EMP":                           {},
	"ENERGYCONTROLLER":              {},
	"EXTERNALFUELTANK":              {},
	"FLIGHTCONTROLLER":              {},
	"FUELCONTROLLER":                {},
	"FUELINTAKE":                    {},
	"FUELTANK":                      {},
	"GRAVITYGENERATOR":              {},
	"JUMPDRIVE":                     {},
	"LANDINGSYSTEM":                 {},
	"LIFESUPPORTGENERATOR":          {},
	"LIFESUPPORTVENT":               {},
	"MAINTHRUSTER":                  {},
	"MANNEUVERTHRUSTER":             {},
	"MININGCONTROLLER":              {},
	"MININGMODIFIER":                {},
	"MISSILE":                       {},
	"MISSILECONTROLLER":             {},
	"MISSILELAUNCHER":               {},
	"MODULE":                        {},
	"PAINTS":                        {},
	"POWERPLANT":                    {},
	"QUANTUMDRIVE":                  {},
	"QUANTUMFUELTANK":               {},
	"QUANTUMINTERDICTIONGENERATOR":  {},
	"RADAR":                         {},
	"RELAY":                         {},
	"REMOTECONNECTION":              {},
	"SALVAGECONTROLLER":             {},
	"SALVAGEFIELDEMITTER":           {},
	"SALVAGEFIELDSUPPORTER":         {},
	"SALVAGEFILLERSTATION":          {},
	"SALVAGEHEAD":                   {},
	"SALVAGEINTERNALSTORAGE":        {},
	"SALVAGEMODIFIER":               {},
	"SCANNER":                       {},
	"SELFDESTRUCT":                  {},
	"SENSOR":                        {},
	"SHIELD":                        {},
	"SHIELDCONTROLLER":              {},
	"SPACEMINE":                     {},
	"STATUSSCREEN":                  {},
	"TARGETSELECTOR":                {},
	"TOOLARM":                       {},
	"TOWINGBEAM":                    {},
	"TRACTORBEAM":                   {},
	"TRANSPONDER":                   {},
	"TURRET":                        {},
	"TURRETBASE":                    {},
	"UTILITYTURRET":                 {},
	"WEAPONATTACHMENT":              {},
	"WEAPONCONTROLLER":              {},
	"WEAPONDEFENSIVE":               {},
	"WEAPONGUN":                     {},
	"WEAPONMINING":                  {},
	"WHEELEDCONTROLLER":             {},
}

func shouldIncludeItem(row map[string]any, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return isShipRelevantItem(row)
	}
	token := resolveItemTypeToken(row)
	if token == "" {
		return false
	}
	_, ok := allowed[token]
	return ok
}

func isShipRelevantItem(row map[string]any) bool {
	classification := itemClassification(row)
	if classification != "" {
		prefix := classification
		if idx := strings.Index(prefix, "."); idx >= 0 {
			prefix = prefix[:idx]
		}
		prefix = strings.ToLower(strings.TrimSpace(prefix))
		if _, ok := shipClassificationPrefixes[prefix]; ok {
			return true
		}
	}

	baseToken := resolveItemTypeToken(row)
	if baseToken == "" {
		return false
	}
	if _, ok := shipItemTypeAllow[baseToken]; ok {
		return true
	}
	if strings.HasPrefix(baseToken, "WEAPON") && baseToken != "WEAPONPERSONAL" {
		return true
	}
	if strings.HasSuffix(baseToken, "TURRET") {
		return true
	}
	if strings.HasSuffix(baseToken, "THRUSTER") {
		return true
	}
	if strings.HasSuffix(baseToken, "CONTROLLER") && baseToken != "DOORCONTROLLER" && baseToken != "LIGHTCONTROLLER" {
		return true
	}
	return false
}

func resolveItemTypeToken(row map[string]any) string {
	base := strings.TrimSpace(itemBaseType(row))
	if base == "" {
		return ""
	}
	return normalizeTypeToken(base)
}

func normalizeTypeToken(value string) string {
	value = strings.ToUpper(value)
	builder := strings.Builder{}
	for _, r := range value {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func itemBaseType(row map[string]any) string {
	if value := getString(row, "type", "Type"); value != "" {
		return value
	}
	if std := resolveNestedMap(row, "std_item", "stdItem", "StdItem"); std != nil {
		if value := getString(std, "Type", "type"); value != "" {
			if idx := strings.Index(value, "."); idx >= 0 {
				return value[:idx]
			}
			return value
		}
	}
	return ""
}

func itemClassification(row map[string]any) string {
	if value := firstStringValue(row["classification"]); value != "" {
		return value
	}
	if value := firstStringValue(row["Classification"]); value != "" {
		return value
	}
	if std := resolveNestedMap(row, "std_item", "stdItem", "StdItem"); std != nil {
		if value := firstStringValue(std["Classification"]); value != "" {
			return value
		}
	}
	return ""
}

func firstStringValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		for _, entry := range v {
			if s := firstStringValue(entry); s != "" {
				return s
			}
		}
	case []string:
		for _, entry := range v {
			if s := strings.TrimSpace(entry); s != "" {
				return s
			}
		}
	default:
		if s := optionalString(v); strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func resolveNestedMap(entry map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		if value, ok := entry[key]; ok {
			switch candidate := value.(type) {
			case map[string]any:
				return candidate
			}
		}
	}
	return nil
}

func nestedManufacturerCode(row map[string]any) string {
	if std := resolveNestedMap(row, "std_item", "stdItem", "StdItem"); std != nil {
		if code := getString(std, "manufacturer_code", "ManufacturerCode", "manufacturer", "Manufacturer"); code != "" {
			return code
		}
		if manufacturer := resolveNestedMap(std, "Manufacturer"); manufacturer != nil {
			if code := getString(manufacturer, "Code", "code"); code != "" {
				return code
			}
		}
	}
	return ""
}

func itemSubtypeFromRow(row map[string]any) string {
	if value := getString(row, "subtype", "subType", "Subtype"); value != "" {
		return value
	}
	if std := resolveNestedMap(row, "std_item", "stdItem", "StdItem"); std != nil {
		if value := getString(std, "SubType", "Subtype", "subType"); value != "" {
			return value
		}
		if typePath := getString(std, "Type", "type"); strings.Contains(typePath, ".") {
			parts := strings.Split(typePath, ".")
			return strings.Join(parts[1:], ".")
		}
	}
	if typePath := getString(row, "type", "Type"); strings.Contains(typePath, ".") {
		parts := strings.Split(typePath, ".")
		return strings.Join(parts[1:], ".")
	}
	return ""
}
