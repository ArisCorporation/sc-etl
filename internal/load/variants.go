package load

import (
	"fmt"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

type variantSnapshot struct {
	ShipID       string
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

func (b *builder) syncShipVariants(variants []model.NormalizedShipVariantV2, stats map[string]map[string]any, shipIDs map[string]string, versionName string, promote bool) (map[string]string, error) {
	fields := []string{"id", "ship", "ship.id", "name", "variant_code", "external_refs", "stats", "thumbnail", "release_patch"}
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
		shipID := shipIDs[strings.ToUpper(strings.TrimSpace(variant.ShipExternal))]
		if shipID == "" {
			return nil, fmt.Errorf("missing ship mapping for variant %s", variant.ExternalID)
		}
		statsPayload := stats[variant.ExternalID]
		if statsPayload == nil {
			statsPayload = map[string]any{}
		}
		variantCode := ""
		if variant.VariantCode != nil {
			variantCode = *variant.VariantCode
		}
		variantCode = strings.TrimSpace(variantCode)
		if variantCode == "" {
			variantCode = "BASE"
		}
		thumbnail := ""
		if variant.Thumbnail != nil {
			thumbnail = strings.TrimSpace(*variant.Thumbnail)
		}
		releasePatch := ""
		if variant.ReleasePatch != nil {
			releasePatch = strings.TrimSpace(*variant.ReleasePatch)
		}
		name := variant.Name
		if strings.TrimSpace(name) == "" {
			name = variant.ExternalID
		}
		snapshot := variantSnapshot{
			ShipID:       shipID,
			Name:         name,
			VariantCode:  variantCode,
			ExternalRefs: ensurePrimaryExternalRef(variant.ExternalRefs, external),
			Stats:        statsPayload,
			Thumbnail:    thumbnail,
			ReleasePatch: releasePatch,
		}

		payload := map[string]any{
			"ship":          snapshot.ShipID,
			"name":          snapshot.Name,
			"variant_code":  nullableString(snapshot.VariantCode),
			"external_refs": snapshot.ExternalRefs,
			"stats":         snapshot.Stats,
			"thumbnail":     nullableString(snapshot.Thumbnail),
			"release_patch": nullableString(snapshot.ReleasePatch),
			"status":        "published",
		}

		composite := variantCompositeKey(snapshot.ShipID, snapshot.VariantCode)
		state := lookupVariantState(composite, snapshot.ExternalRefs, byComposite, byRef)

		if state != nil {
			diffPayload := diff.Compute(variantSnapshotMap(state.Snapshot), variantSnapshotMap(snapshot), []string{"ship", "name", "variant_code", "external_refs", "stats", "thumbnail", "release_patch"})
			if diffPayload != nil {
				detachVariantState(state, byComposite, byRef)
				if _, err := b.client.UpdateOneWithVersion(b.ctx, b.collections.ShipVariants, state.ID, payload, versionName, promote); err != nil {
					if versionErr := handleVersionError(err); versionErr != nil {
						return nil, fmt.Errorf("update ship variant %s: %w", external, versionErr)
					}
				}
				state.Snapshot = snapshot
				state.Composite = variantCompositeKey(snapshot.ShipID, snapshot.VariantCode)
				state.RefKeys = buildRefKeys(snapshot.ExternalRefs)
				attachVariantState(state, byComposite, byRef)
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
	shipID := extractID(row["ship"])
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
		ShipID:       shipID,
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
		"ship":          snapshot.ShipID,
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
		Composite: variantCompositeKey(snapshot.ShipID, snapshot.VariantCode),
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
