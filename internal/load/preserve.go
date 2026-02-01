package load

import (
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/model"
)

// preferString returns newVal if it is non-empty (after trim), otherwise returns existing.
func preferString(newVal string, existing string) string {
	newVal = strings.TrimSpace(newVal)
	if newVal != "" {
		return newVal
	}
	return existing
}

// preferPtrString dereferences a pointer and applies preferString semantics.
func preferPtrString(newVal *string, existing string) string {
	if newVal == nil {
		return existing
	}
	return preferString(*newVal, existing)
}

// preferMap returns newMap unless it is nil or empty, in which case it clones existing.
func preferMap(newMap map[string]any, existing map[string]any) map[string]any {
	if len(newMap) == 0 {
		if existing == nil {
			return map[string]any{}
		}
		cloned := make(map[string]any, len(existing))
		for k, v := range existing {
			cloned[k] = v
		}
		return cloned
	}
	return newMap
}

// preferRefs returns newRefs unless it is empty.
func preferRefs(newRefs []any, existing []any) []any {
	if len(newRefs) == 0 {
		return existing
	}
	return newRefs
}

// preferExternalRefs returns newRefs unless it is empty.
func preferExternalRefs(newRefs []model.NormalizedExternalReference, existing []model.NormalizedExternalReference) []model.NormalizedExternalReference {
	if len(newRefs) == 0 {
		return existing
	}
	return newRefs
}

// preferStrings keeps existing if newSlice is empty.
func preferStrings(newSlice []string, existing []string) []string {
	if len(newSlice) == 0 {
		return existing
	}
	return newSlice
}
