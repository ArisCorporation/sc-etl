package load

import (
	"fmt"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

type companySnapshot struct {
	Code         string
	Name         *string
	Category     *string
	ExternalRefs []model.NormalizedExternalReference
	Status       string
}

type companyState struct {
	ID       string
	Snapshot companySnapshot
}

func (b *builder) syncCompanies(companies []model.NormalizedCompanyV2) (map[string]string, error) {
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.Companies, []string{"id", "code", "name", "category", "category.id", "external_refs", "status"}, nil)
	if err != nil {
		return nil, err
	}

	byCode := map[string]companyState{}
	for _, row := range rows {
		state, ok := makeCompanyState(row)
		if !ok {
			continue
		}
		byCode[state.Snapshot.Code] = state
	}

	defaultCategory := pickDefaultCompanyCategoryState(byCode)
	if b.defaultCompanyCategory != nil {
		if defaultCategory == nil || *defaultCategory != *b.defaultCompanyCategory {
			utils.Logger().Info("Using configured default company category", "category", *b.defaultCompanyCategory)
		}
		defaultCategory = b.defaultCompanyCategory
	} else if defaultCategory == nil {
		utils.Logger().Warn("No default company category detected; normalized companies will not get a fallback category.")
	}

	idMap := map[string]string{}
	for _, company := range companies {
		state := snapshotFromCompany(company, defaultCategory)
		if state.Code == "" {
			utils.Logger().Warn("Skipping normalized company with invalid code")
			continue
		}
		existing, ok := byCode[state.Code]
		payload := map[string]any{
			"code":          state.Code,
			"name":          nullableStringPtr(state.Name),
			"category":      nullableStringPtr(state.Category),
			"external_refs": state.ExternalRefs,
			"status":        state.Status,
		}
		if !ok {
			utils.Logger().Warn("Skipping normalized company without existing Directus entry", "code", state.Code)
			continue
		}
		diffPayload := diff.Compute(snapshotToMap(existing.Snapshot), snapshotToMap(state), []string{"name", "category", "external_refs", "status"})
		if diffPayload != nil {
			if _, err := b.client.UpdateOne(b.ctx, b.collections.Companies, existing.ID, payload); err != nil {
				return nil, fmt.Errorf("update company %s: %w", state.Code, err)
			}
			existing.Snapshot = state
			byCode[state.Code] = existing
		}
		idMap[state.Code] = existing.ID
	}

	return idMap, nil
}

func makeCompanyState(row map[string]any) (companyState, bool) {
	code := strings.ToUpper(normalizeString(row["code"]))
	if code == "" {
		return companyState{}, false
	}
	name, _ := extractString(row["name"])
	category := extractCategory(row["category"])
	refs := normalizeExternalRefs(row["external_refs"])
	status := normalizeString(row["status"])
	if status == "" {
		status = "draft"
	}
	return companyState{
		ID: toString(row["id"]),
		Snapshot: companySnapshot{
			Code:         code,
			Name:         name,
			Category:     category,
			ExternalRefs: refs,
			Status:       status,
		},
	}, true
}

func snapshotFromCompany(company model.NormalizedCompanyV2, defaultCategory *string) companySnapshot {
	code := strings.ToUpper(strings.TrimSpace(company.Code))
	var name *string
	if company.Name != nil {
		n := strings.TrimSpace(*company.Name)
		if n != "" {
			name = &n
		}
	}
	category := defaultCategory
	refs := normalizeExternalRefSlice(company.ExternalRefs)
	return companySnapshot{
		Code:         code,
		Name:         name,
		Category:     category,
		ExternalRefs: refs,
		Status:       "published",
	}
}

func snapshotToMap(snapshot companySnapshot) map[string]any {
	return map[string]any{
		"name":          nullableStringPtr(snapshot.Name),
		"category":      nullableStringPtr(snapshot.Category),
		"external_refs": snapshot.ExternalRefs,
		"status":        snapshot.Status,
	}
}

func pickDefaultCompanyCategoryState(states map[string]companyState) *string {
	counts := map[string]int{}
	for _, state := range states {
		if state.Snapshot.Category == nil {
			continue
		}
		key := *state.Snapshot.Category
		counts[key]++
	}
	bestKey := ""
	bestCount := -1
	for key, count := range counts {
		if count > bestCount {
			bestKey = key
			bestCount = count
		}
	}
	if bestKey == "" {
		return nil
	}
	return &bestKey
}

func extractCategory(value any) *string {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case map[string]any:
		id := normalizeString(v["id"])
		if id == "" {
			return nil
		}
		return &id
	case string:
		id := normalizeString(v)
		if id == "" {
			return nil
		}
		return &id
	default:
		return nil
	}
}

func normalizeExternalRefs(value any) []model.NormalizedExternalReference {
	refs := []model.NormalizedExternalReference{}
	list, ok := value.([]any)
	if !ok {
		return refs
	}
	for _, entry := range list {
		record, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		source := normalizeString(record["source"])
		id := normalizeString(record["id"])
		if source == "" || id == "" {
			continue
		}
		var note *string
		if n, ok := extractString(record["note"]); ok {
			note = n
		}
		refs = append(refs, model.NormalizedExternalReference{Source: source, ID: id, Note: note})
	}
	return refs
}

func normalizeExternalRefSlice(refs []model.NormalizedExternalReference) []model.NormalizedExternalReference {
	result := make([]model.NormalizedExternalReference, 0, len(refs))
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
		result = append(result, entry)
	}
	return result
}
