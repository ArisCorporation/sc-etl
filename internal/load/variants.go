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
	ID       string
	Snapshot variantSnapshot
}

func (b *builder) syncShipVariants(variants []model.NormalizedShipVariantV2, stats map[string]map[string]any, shipIDs map[string]string, versionName string, promote bool) (map[string]string, error) {
	fields := []string{"id", "ship", "ship.id", "name", "variant_code", "external_refs", "stats", "thumbnail", "release_patch"}
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.ShipVariants, fields, nil)
	if err != nil {
		return nil, err
	}
	existingByExternal := map[string]variantState{}
	for _, row := range rows {
		snapshot := makeVariantSnapshotFromRow(row)
		external := extractPrimaryExternalID(snapshot.ExternalRefs)
		if external == "" {
			continue
		}
		existingByExternal[external] = variantState{
			ID:       toString(row["id"]),
			Snapshot: snapshot,
		}
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

		if existing, ok := existingByExternal[external]; ok {
			diffPayload := diff.Compute(variantSnapshotMap(existing.Snapshot), variantSnapshotMap(snapshot), []string{"ship", "name", "variant_code", "external_refs", "stats", "thumbnail", "release_patch"})
			if diffPayload != nil {
				if _, err := b.client.UpdateOneWithVersion(b.ctx, b.collections.ShipVariants, existing.ID, payload, versionName, promote); err != nil {
					return nil, fmt.Errorf("update ship variant %s: %w", external, err)
				}
				existing.Snapshot = snapshot
				existingByExternal[external] = existing
			}
			variantIDs[external] = existing.ID
			continue
		}

		created, err := b.client.CreateOneWithVersion(b.ctx, b.collections.ShipVariants, payload, versionName, promote)
		if err != nil {
			return nil, fmt.Errorf("create ship variant %s: %w", external, err)
		}
		id := toString(created["id"])
		variantIDs[external] = id
		existingByExternal[external] = variantState{ID: id, Snapshot: snapshot}
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
