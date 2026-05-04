package transform

import (
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/model"
)

// filterAllowedHardpoints keeps only hardpoints whose category matches the allowed set.
func filterAllowedHardpoints(hardpoints []model.NormalizedHardpoint, allowed map[string]struct{}) []model.NormalizedHardpoint {
	if len(allowed) == 0 || len(hardpoints) == 0 {
		return hardpoints
	}
	result := make([]model.NormalizedHardpoint, 0, len(hardpoints))
	for _, hp := range hardpoints {
		if isAllowedCategory(hp.Category, allowed) {
			result = append(result, hp)
		}
	}
	return result
}

func isAllowedCategory(category string, allowed map[string]struct{}) bool {
	cat := strings.ToUpper(strings.TrimSpace(category))
	if cat == "" {
		return false
	}
	if _, ok := allowed[cat]; ok {
		return true
	}
	for token := range allowed {
		if strings.HasPrefix(cat, token) || strings.HasSuffix(cat, token) {
			return true
		}
	}
	return false
}
