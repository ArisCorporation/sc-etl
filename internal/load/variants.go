package load

import (
	"fmt"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

type variantSnapshot struct {
	HullID       string
	Name         string
	VariantCode  string
	ExternalRefs []model.NormalizedExternalReference
	Stats        map[string]any
	Thumbnail    string
	ReleasePatch string
}

type variantState struct {
	ID        string
	Snapshot  variantSnapshot
	Composite string
	RefKeys   []string
	Matched   bool
}

func (b *builder) syncShipVariants(variants []model.NormalizedShipVariantV2, stats map[string]map[string]any, hullIDs map[string]string, versionName string, promote bool) (map[string]string, error) {
	fields := []string{"id", "hull", "hull.id", "name", "variant_code", "external_refs", "stats", "thumbnail", "release_patch"}
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.ShipVariants, fields, nil)
	if err != nil {
		return nil, err
	}
	byComposite := map[string]*variantState{}
	byRef := map[string]*variantState{}
	byID := map[string]*variantState{}
	for _, row := range rows {
		state := makeVariantState(toString(row["id"]), makeVariantSnapshotFromRow(row))
		attachVariantState(state, byComposite, byRef)
		byID[state.ID] = state
	}

	variantIDs := map[string]string{}

	for _, variant := range variants {
		external := strings.ToUpper(strings.TrimSpace(variant.ExternalID))
		if external == "" {
			continue
		}
		hullID := hullIDs[strings.ToUpper(strings.TrimSpace(variant.ShipExternal))]
		if hullID == "" {
			return nil, fmt.Errorf("missing hull mapping for variant %s", variant.ExternalID)
		}
		statsPayload := stats[variant.ExternalID]
		if statsPayload == nil {
			statsPayload = map[string]any{}
		}
		variantCode := preferString(strings.TrimSpace(preferPtrString(variant.VariantCode, "")), "BASE")
		composite := variantCompositeKey(hullID, variantCode)

		externalRefs := ensurePrimaryExternalRef(variant.ExternalRefs, external)
		state := lookupVariantState(composite, externalRefs, byComposite, byRef)

		name := preferString(variant.Name, "")
		if state != nil {
			name = preferString(name, state.Snapshot.Name)
			variantCode = preferString(variantCode, state.Snapshot.VariantCode)
			externalRefs = preferExternalRefs(externalRefs, state.Snapshot.ExternalRefs)
			statsPayload = preferMap(statsPayload, state.Snapshot.Stats)
		}
		if strings.TrimSpace(name) == "" {
			name = variant.ExternalID
		}
		composite = variantCompositeKey(hullID, variantCode)

		thumbnail := preferPtrString(variant.Thumbnail, "")
		if state != nil {
			thumbnail = preferString(thumbnail, state.Snapshot.Thumbnail)
		}

		releasePatch := preferPtrString(variant.ReleasePatch, "")
		if state != nil {
			releasePatch = preferString(releasePatch, state.Snapshot.ReleasePatch)
		}

		snapshot := variantSnapshot{
			HullID:       hullID,
			Name:         name,
			VariantCode:  variantCode,
			ExternalRefs: externalRefs,
			Stats:        statsPayload,
			Thumbnail:    thumbnail,
			ReleasePatch: releasePatch,
		}

		payload := map[string]any{
			"hull":          snapshot.HullID,
			"name":          snapshot.Name,
			"variant_code":  nullableString(snapshot.VariantCode),
			"external_refs": snapshot.ExternalRefs,
			"stats":         snapshot.Stats,
			"thumbnail":     nullableString(snapshot.Thumbnail),
			"release_patch": nullableString(snapshot.ReleasePatch),
			"status":        "published",
		}

		if state == nil {
			state = lookupVariantState(composite, snapshot.ExternalRefs, byComposite, byRef)
		}

		if state != nil {
			diffPayload := diff.Compute(variantSnapshotMap(state.Snapshot), variantSnapshotMap(snapshot), []string{"hull", "name", "variant_code", "external_refs", "stats", "thumbnail", "release_patch"})
			if diffPayload != nil {
				detachVariantState(state, byComposite, byRef)
				if _, err := b.client.UpdateOneWithVersion(b.ctx, b.collections.ShipVariants, state.ID, payload, versionName, promote); err != nil {
					if versionErr := handleVersionError(err); versionErr != nil {
						return nil, fmt.Errorf("update ship variant %s: %w", external, versionErr)
					}
				}
				state.Snapshot = snapshot
				state.Composite = variantCompositeKey(snapshot.HullID, snapshot.VariantCode)
				state.RefKeys = buildRefKeys(snapshot.ExternalRefs)
				attachVariantState(state, byComposite, byRef)
			} else {
				if err := b.ensureVersionSnapshot(b.collections.ShipVariants, state.ID, payload, versionName, promote); err != nil {
					if versionErr := handleVersionError(err); versionErr != nil {
						return nil, fmt.Errorf("version ship variant %s: %w", external, versionErr)
					}
				}
			}
			state.Matched = true
			variantIDs[external] = state.ID
			continue
		}

		created, err := b.client.CreateOneWithVersion(b.ctx, b.collections.ShipVariants, payload, versionName, promote)
		if err != nil {
			if versionErr := handleVersionError(err); versionErr != nil {
				return nil, fmt.Errorf("create ship variant %s: %w", external, versionErr)
			}
		}
		id := toString(created["id"])
		variantIDs[external] = id
		newState := makeVariantState(id, snapshot)
		newState.Matched = true
		attachVariantState(newState, byComposite, byRef)
		byID[newState.ID] = newState
	}

	var toDelete []string
	for id, state := range byID {
		if !state.Matched {
			toDelete = append(toDelete, id)
		}
	}

	if len(toDelete) > 0 {
		for _, batch := range chunkStrings(toDelete, 100) {
			if err := b.client.DeleteMany(b.ctx, b.collections.ShipVariants, batch); err != nil {
				return nil, fmt.Errorf("delete ship variants: %w", err)
			}
		}
	}

	return variantIDs, nil
}

func makeVariantSnapshotFromRow(row map[string]any) variantSnapshot {
	hullID := extractID(row["hull"])
	name := normalizeString(row["name"])
	variantCode := normalizeString(row["variant_code"])
	refs := normalizeExternalRefsInput(row["external_refs"])
	stats := map[string]any{}
	if payload, ok := row["stats"].(map[string]any); ok {
		stats = payload
	}
	thumbnail := normalizeString(row["thumbnail"])
	release := normalizeString(row["release_patch"])
	return variantSnapshot{
		HullID:       hullID,
		Name:         name,
		VariantCode:  variantCode,
		ExternalRefs: refs,
		Stats:        stats,
		Thumbnail:    thumbnail,
		ReleasePatch: release,
	}
}

func variantSnapshotMap(snapshot variantSnapshot) map[string]any {
	return map[string]any{
		"hull":          snapshot.HullID,
		"name":          snapshot.Name,
		"variant_code":  nullableString(snapshot.VariantCode),
		"external_refs": snapshot.ExternalRefs,
		"stats":         snapshot.Stats,
		"thumbnail":     nullableString(snapshot.Thumbnail),
		"release_patch": nullableString(snapshot.ReleasePatch),
	}
}

func makeVariantState(id string, snapshot variantSnapshot) *variantState {
	return &variantState{
		ID:        id,
		Snapshot:  snapshot,
		Composite: variantCompositeKey(snapshot.HullID, snapshot.VariantCode),
		RefKeys:   buildRefKeys(snapshot.ExternalRefs),
		Matched:   false,
	}
}

func attachVariantState(state *variantState, byComposite map[string]*variantState, byRef map[string]*variantState) {
	if state == nil {
		return
	}
	if state.Composite != "" {
		byComposite[state.Composite] = state
	}
	for _, key := range state.RefKeys {
		byRef[key] = state
	}
}

func detachVariantState(state *variantState, byComposite map[string]*variantState, byRef map[string]*variantState) {
	if state == nil {
		return
	}
	if state.Composite != "" {
		if current, ok := byComposite[state.Composite]; ok && current == state {
			delete(byComposite, state.Composite)
		}
	}
	for _, key := range state.RefKeys {
		if current, ok := byRef[key]; ok && current == state {
			delete(byRef, key)
		}
	}
}

func lookupVariantState(composite string, refs []model.NormalizedExternalReference, byComposite map[string]*variantState, byRef map[string]*variantState) *variantState {
	if composite != "" {
		if state, ok := byComposite[composite]; ok {
			return state
		}
	}
	keys := buildRefKeys(refs)
	for _, key := range keys {
		if state, ok := byRef[key]; ok {
			return state
		}
	}
	return nil
}
