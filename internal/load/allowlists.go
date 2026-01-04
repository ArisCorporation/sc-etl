package load

import "strings"

func normalizeAllowlist(values []string) ([]string, map[string]struct{}) {
	list := []string{}
	set := map[string]struct{}{}
	seen := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		if _, ok := seen[upper]; ok {
			continue
		}
		seen[upper] = struct{}{}
		list = append(list, trimmed)
		set[upper] = struct{}{}
	}
	return list, set
}

func buildEqualityFilter(field string, values []string) map[string]any {
	switch len(values) {
	case 0:
		return nil
	case 1:
		return map[string]any{
			field: map[string]any{
				"_eq": values[0],
			},
		}
	default:
		return map[string]any{
			field: map[string]any{
				"_in": values,
			},
		}
	}
}
