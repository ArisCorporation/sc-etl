package transform

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
)

func optionalString(value any) string {
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return ""
		}
		return v
	case json.Number:
		return v.String()
	case int, int64, float64:
		return strings.TrimSpace(formatNumber(v))
	default:
		return ""
	}
}

func optionalNumber(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	case string:
		if strings.TrimSpace(v) == "" {
			return math.NaN()
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return math.NaN()
		}
		return f
	default:
		return math.NaN()
	}
}

func coalesce[T comparable](values ...T) T {
	var zero T
	for _, value := range values {
		if value != zero {
			return value
		}
	}
	return zero
}

func formatNumber(value any) string {
	switch v := value.(type) {
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func normalizeString(value string) string {
	return strings.TrimSpace(value)
}

func sortStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func getString(entry map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := entry[key]; ok {
			if s := optionalString(value); s != "" {
				return s
			}
		}
	}
	return ""
}

func getNumber(entry map[string]any, keys ...string) float64 {
	for _, key := range keys {
		if value, ok := entry[key]; ok {
			if number := optionalNumber(value); !math.IsNaN(number) {
				return number
			}
		}
	}
	return math.NaN()
}

func getArray(entry map[string]any, key string) []any {
	if value, ok := entry[key]; ok {
		switch v := value.(type) {
		case []any:
			return v
		case []map[string]any:
			result := make([]any, len(v))
			for i, item := range v {
				result[i] = item
			}
			return result
		case []string:
			result := make([]any, len(v))
			for i, item := range v {
				result[i] = item
			}
			return result
		}
	}
	return nil
}

func getMap(entry map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		if value, ok := entry[key]; ok {
			if nested, ok := value.(map[string]any); ok {
				return nested
			}
		}
	}
	return nil
}

func getRawValue(entry map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		if value, ok := entry[key]; ok {
			if value != nil {
				return value, true
			}
		}
	}
	return nil, false
}

func toMapArray(input []any) []map[string]any {
	result := []map[string]any{}
	for _, entry := range input {
		if entry == nil {
			continue
		}
		switch v := entry.(type) {
		case map[string]any:
			result = append(result, v)
		}
	}
	return result
}

func dedupeStrings(values []string) []string {
	seen := map[string]struct{}{}
	result := []string{}
	for _, value := range values {
		normalized := strings.TrimSpace(value)
		if normalized == "" {
			continue
		}
		key := strings.ToLower(normalized)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, normalized)
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func uppercase(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func humanizeIdentifier(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	replacer := strings.NewReplacer("_", " ", "-", " ")
	cleaned := replacer.Replace(input)
	parts := strings.Fields(cleaned)
	for i, part := range parts {
		parts[i] = strings.Title(strings.ToLower(part))
	}
	return strings.Join(parts, " ")
}

func deriveShipClass(row map[string]any) string {
	candidates := []string{
		getString(row, "class"),
		getString(row, "Class"),
		getString(row, "Role"),
		getString(row, "Career"),
	}
	for _, candidate := range candidates {
		if candidate != "" {
			return candidate
		}
	}
	className := getString(row, "ClassName")
	if className != "" {
		if humanized := humanizeIdentifier(className); humanized != "" {
			return humanized
		}
		return className
	}
	return "Unknown"
}
