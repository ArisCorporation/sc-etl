package load

import (
	"math"
	"strconv"
	"strings"
)

func normalizeString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case nil:
		return ""
	default:
		return strings.TrimSpace(toString(v))
	}
}

func toString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func extractInt(value any) (*int, bool) {
	switch v := value.(type) {
	case int:
		return &v, true
	case int64:
		i := int(v)
		return &i, true
	case float64:
		i := int(math.Round(v))
		return &i, true
	case string:
		t := strings.TrimSpace(v)
		if t == "" {
			return nil, false
		}
		parsed, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return nil, false
		}
		i := int(math.Round(parsed))
		return &i, true
	default:
		return nil, false
	}
}

func extractBool(value any) (*bool, bool) {
	switch v := value.(type) {
	case bool:
		return &v, true
	case string:
		t := strings.ToLower(strings.TrimSpace(v))
		switch t {
		case "1", "true", "yes", "on":
			res := true
			return &res, true
		case "0", "false", "no", "off":
			res := false
			return &res, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func extractString(value any) (*string, bool) {
	str := normalizeString(value)
	if str == "" {
		return nil, false
	}
	return &str, true
}

func extractID(value any) string {
	switch v := value.(type) {
	case map[string]any:
		return normalizeString(v["id"])
	case string:
		return normalizeString(v)
	case nil:
		return ""
	default:
		return normalizeString(v)
	}
}

func extractIDPtr(value any) *string {
	id := extractID(value)
	if id == "" {
		return nil
	}
	return &id
}

func nullableString(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func nullableStringPtr(value *string) any {
	if value == nil {
		return nil
	}
	return nullableString(*value)
}

func stringFromPtr(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
