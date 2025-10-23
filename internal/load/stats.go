package load

import (
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/model"
)

func splitVariantStats(bundle model.NormalizedBundleV2) (map[string]map[string]any, []model.NormalizedHardpointV2) {
	statsByVariant := make(map[string]map[string]any, len(bundle.ShipVariants))
	extracted := []model.NormalizedHardpointV2{}
	for _, variant := range bundle.ShipVariants {
		stats, hardpoints := sanitizeVariantStats(variant)
		statsByVariant[variant.ExternalID] = stats
		extracted = append(extracted, hardpoints...)
	}
	explicit := bundle.Hardpoints
	if len(explicit) == 0 {
		explicit = extracted
	}
	return statsByVariant, explicit
}

func sanitizeVariantStats(variant model.NormalizedShipVariantV2) (map[string]any, []model.NormalizedHardpointV2) {
	stats := map[string]any{}
	if variant.Stats.Raw != nil {
		for key, value := range variant.Stats.Raw {
			stats[key] = value
		}
	}
	rawHardpoints, _ := stats["hardpoints"].([]any)
	hardpoints := []model.NormalizedHardpointV2{}
	for _, entry := range rawHardpoints {
		record, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		code := normalizeString(record["code"])
		category := normalizeString(record["category"])
		if code == "" || category == "" {
			continue
		}
		positionPtr, _ := extractString(record["position"])
		sizePtr, _ := extractInt(record["size"])
		gimballedPtr, _ := extractBool(record["gimballed"])
		poweredPtr, _ := extractBool(record["powered"])
		seatsPtr, _ := extractInt(record["seats"])
		externalID := normalizeString(record["external_id"])
		if externalID == "" {
			externalID = strings.ToUpper(variant.ExternalID + ":" + code)
		}
		hardpoints = append(hardpoints, model.NormalizedHardpointV2{
			ExternalID:          externalID,
			ShipVariantExternal: variant.ExternalID,
			Code:                code,
			Category:            category,
			Position:            positionPtr,
			Size:                sizePtr,
			Gimballed:           gimballedPtr,
			Powered:             poweredPtr,
			Seats:               seatsPtr,
		})
	}
	delete(stats, "hardpoints")
	return stats, hardpoints
}
