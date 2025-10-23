package load

import (
	"sort"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/model"
)

const primaryExternalSource = "SC_DATA"

func normalizeExternalRefsInput(value any) []model.NormalizedExternalReference {
	result := []model.NormalizedExternalReference{}
	entries, ok := value.([]any)
	if !ok {
		return result
	}
	for _, entry := range entries {
		record, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		source := normalizeString(record["source"])
		id := normalizeString(record["id"])
		if source == "" || id == "" {
			continue
		}
		reference := model.NormalizedExternalReference{Source: source, ID: id}
		if note, ok := extractString(record["note"]); ok {
			reference.Note = note
		}
		result = append(result, reference)
	}
	return sortRefs(result)
}

func cloneRefs(refs []model.NormalizedExternalReference) []model.NormalizedExternalReference {
	cloned := make([]model.NormalizedExternalReference, len(refs))
	for i, ref := range refs {
		cloned[i] = model.NormalizedExternalReference{Source: ref.Source, ID: ref.ID}
		if ref.Note != nil {
			note := *ref.Note
			cloned[i].Note = &note
		}
	}
	return cloned
}

func sortRefs(refs []model.NormalizedExternalReference) []model.NormalizedExternalReference {
	sorted := cloneRefs(refs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Source == sorted[j].Source {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].Source < sorted[j].Source
	})
	return sorted
}

func buildRefKeys(refs []model.NormalizedExternalReference) []string {
	keys := make([]string, len(refs))
	for i, ref := range refs {
		keys[i] = strings.ToUpper(ref.Source + ":" + ref.ID)
	}
	return keys
}

func uniqueRefs(refs []model.NormalizedExternalReference) []model.NormalizedExternalReference {
	if len(refs) == 0 {
		return []model.NormalizedExternalReference{}
	}
	seen := map[string]model.NormalizedExternalReference{}
	for _, ref := range refs {
		source := strings.TrimSpace(ref.Source)
		id := strings.TrimSpace(ref.ID)
		if source == "" || id == "" {
			continue
		}
		entry := model.NormalizedExternalReference{Source: source, ID: id}
		if ref.Note != nil {
			note := strings.TrimSpace(*ref.Note)
			if note != "" {
				entry.Note = &note
			}
		}
		key := strings.ToUpper(source + ":" + id)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = entry
	}
	result := make([]model.NormalizedExternalReference, 0, len(seen))
	for _, entry := range seen {
		result = append(result, entry)
	}
	return sortRefs(result)
}

func ensurePrimaryExternalRef(refs []model.NormalizedExternalReference, primary string) []model.NormalizedExternalReference {
	primary = strings.ToUpper(strings.TrimSpace(primary))
	clean := uniqueRefs(refs)
	if primary == "" {
		return clean
	}
	matched := false
	for idx := range clean {
		if strings.EqualFold(clean[idx].Source, primaryExternalSource) && strings.EqualFold(clean[idx].ID, primary) {
			clean[idx].Source = primaryExternalSource
			clean[idx].ID = primary
			matched = true
			break
		}
	}
	if !matched {
		clean = append(clean, model.NormalizedExternalReference{
			Source: primaryExternalSource,
			ID:     primary,
		})
	}
	return sortRefs(clean)
}

func extractPrimaryExternalID(refs []model.NormalizedExternalReference) string {
	for _, ref := range refs {
		if strings.EqualFold(ref.Source, primaryExternalSource) {
			return strings.ToUpper(strings.TrimSpace(ref.ID))
		}
	}
	return ""
}

func primaryExternalKey(external string) string {
	external = strings.ToUpper(strings.TrimSpace(external))
	if external == "" {
		return ""
	}
	return strings.ToUpper(primaryExternalSource + ":" + external)
}
