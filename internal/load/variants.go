package load

import (
	"fmt"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

type variantSnapshot struct {
	HullID       string
	Name         string
	VariantCode  string
	ExternalRefs []model.NormalizedExternalReference
	Stats        map[string]any
	Thumbnail    string
	ThumbnailURL string
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
	fields := []string{"id", "hull", "hull.id", "name", "variant_code", "external_refs", "stats", "thumbnail", "thumbnail.id", "thumbnail.description", "release_patch"}
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.ShipVariants, fields, nil)
	if err != nil {
		return nil, err
	}
	b.ensureRSIMedia()
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

		thumbnail := ""
		thumbnailURL := ""
		if state != nil {
			thumbnail = state.Snapshot.Thumbnail
			thumbnailURL = state.Snapshot.ThumbnailURL
		}
		if thumbnail == "" {
			thumbnail = preferPtrString(variant.Thumbnail, "")
		}

		if rsiID := extractRSIID(externalRefs); rsiID != "" {
			if media, ok := b.rsiMedia[rsiID]; ok && media.Thumbnail != "" && (thumbnail == "" || thumbnailURL != media.Thumbnail) {
				if fileID, err := b.importFile(media.Thumbnail); err != nil {
					utils.Logger().Warn("Failed to import RSI thumbnail", "variant", variant.ExternalID, "url", media.Thumbnail, "error", err)
				} else if fileID != "" {
					thumbnail = fileID
					thumbnailURL = media.Thumbnail
				}
			}
			if media, ok := b.rsiMedia[rsiID]; ok {
				if len(media.Gallery) > 0 || media.Store != "" {
					hID := hullID
					if hID == "" && state != nil {
						hID = state.Snapshot.HullID
					}
					b.maybeAttachHullMedia(hID, media)
				}
			}
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
			ThumbnailURL: thumbnailURL,
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

	staleCount := 0
	for _, state := range byID {
		if !state.Matched {
			staleCount++
		}
	}

	if staleCount > 0 {
		utils.Logger().Info("Retaining unmatched ship variants", "count", staleCount)
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
	thumbnailField := row["thumbnail"]
	thumbnail := extractID(thumbnailField)
	thumbnailURL := normalizeString(extractMediaFileState(thumbnailField).Description)
	if strings.HasPrefix(thumbnailURL, "RSI matrix source: ") {
		thumbnailURL = strings.TrimPrefix(thumbnailURL, "RSI matrix source: ")
	}
	release := normalizeString(row["release_patch"])
	return variantSnapshot{
		HullID:       hullID,
		Name:         name,
		VariantCode:  variantCode,
		ExternalRefs: refs,
		Stats:        stats,
		Thumbnail:    thumbnail,
		ThumbnailURL: thumbnailURL,
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
