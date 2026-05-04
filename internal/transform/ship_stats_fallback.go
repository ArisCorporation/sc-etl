package transform

import (
	"math"
	"sort"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/model"
)

func buildShipStatsFallback(rawDir string, grouping *ShipGrouping, aggregatedShips []map[string]any) ([]model.NormalizedShipStat, error) {
	if grouping == nil {
		return nil, nil
	}
	records, err := readShipRecordsWithLoadouts(rawDir, aggregatedShips)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	statsByVariant := map[string]map[string]any{}
	for _, record := range records {
		assignment, ok := resolveRecordAssignment(record, grouping)
		if !ok {
			continue
		}
		variantID := canonicalVariantID(assignment.HullKey, assignment.VariantCode)
		if variantID == "" {
			continue
		}
		if _, exists := statsByVariant[variantID]; exists {
			continue
		}
		payload := buildRawShipStatPayload(record.Ship)
		if len(payload) == 0 {
			continue
		}
		statsByVariant[variantID] = payload
	}
	if len(statsByVariant) == 0 {
		return nil, nil
	}
	result := make([]model.NormalizedShipStat, 0, len(statsByVariant))
	for variantID, payload := range statsByVariant {
		result = append(result, model.NormalizedShipStat{
			ShipVariantExternalID: variantID,
			Stats:                 payload,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ShipVariantExternalID < result[j].ShipVariantExternalID
	})
	return result, nil
}

func buildRawShipStatPayload(ship map[string]any) map[string]any {
	if ship == nil {
		return nil
	}
	payload := map[string]any{}
	if flight := getMap(ship, "FlightCharacteristics", "flightCharacteristics"); len(flight) > 0 {
		payload["flight"] = flight
	}
	if propulsion := getMap(ship, "Propulsion", "propulsion"); len(propulsion) > 0 {
		payload["propulsion"] = propulsion
	}
	if quantum := getMap(ship, "QuantumTravel", "quantumTravel"); len(quantum) > 0 {
		payload["quantum"] = quantum
	}
	if insurance := getMap(ship, "Insurance", "insurance"); len(insurance) > 0 {
		payload["insurance"] = insurance
	}
	if cargo := optionalNumber(ship["Cargo"]); !math.IsNaN(cargo) {
		payload["cargo"] = cargo
	}
	if grids := getArray(ship, "CargoGrids"); len(grids) > 0 {
		payload["cargo_grids"] = grids
	}
	if health := optionalNumber(ship["Health"]); !math.IsNaN(health) {
		payload["health"] = health
	}
	if crew := optionalNumber(ship["Crew"]); !math.IsNaN(crew) {
		payload["crew"] = crew
	}
	if mass := optionalNumber(ship["Mass"]); !math.IsNaN(mass) {
		payload["mass"] = mass
	}
	if damage := getMap(ship, "DamageBeforeDestruction"); len(damage) > 0 {
		payload["damage_before_destruction"] = damage
	}
	if damage := getMap(ship, "DamageBeforeDetach"); len(damage) > 0 {
		payload["damage_before_detach"] = damage
	}
	if shield := strings.TrimSpace(optionalString(ship["ShieldFaceType"])); shield != "" {
		payload["shield_face_type"] = shield
	}
	if role := strings.TrimSpace(optionalString(ship["Role"])); role != "" {
		payload["role"] = role
	}
	if career := strings.TrimSpace(optionalString(ship["Career"])); career != "" {
		payload["career"] = career
	}
	if size := strings.TrimSpace(optionalString(ship["Size"])); size != "" {
		payload["size"] = size
	}
	dimensions := map[string]any{}
	if width := optionalNumber(ship["Width"]); !math.IsNaN(width) {
		dimensions["width"] = width
	}
	if length := optionalNumber(ship["Length"]); !math.IsNaN(length) {
		dimensions["length"] = length
	}
	if height := optionalNumber(ship["Height"]); !math.IsNaN(height) {
		dimensions["height"] = height
	}
	if len(dimensions) > 0 {
		payload["dimensions"] = dimensions
	}
	return payload
}
