package load

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

type shipSnapshot struct {
	Name         string
	Manufacturer string
	ExternalRefs []model.NormalizedExternalReference
	Paints       []string
}

type shipState struct {
	ID       string
	Snapshot shipSnapshot
}

func (b *builder) syncShips(resolveCompanyID func(string) (string, error), ships []model.NormalizedShipV2, versionName string, promote bool) (map[string]string, error) {
	fields := []string{"id", "name", "manufacturer", "manufacturer.id", "external_refs", "paints"}
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.Ships, fields, nil)
	if err != nil {
		return nil, err
	}

	existingByExternal := map[string]shipState{}
	for _, row := range rows {
		state := shipState{
			ID:       toString(row["id"]),
			Snapshot: makeShipSnapshotFromRow(row),
		}
		external := extractPrimaryExternalID(state.Snapshot.ExternalRefs)
		if external == "" {
			continue
		}
		existingByExternal[external] = state
	}

	shipIDByExternal := map[string]string{}

	for _, ship := range ships {
		external := strings.ToUpper(strings.TrimSpace(ship.ExternalID))
		if external == "" {
			continue
		}
		manufacturerID := ""
		if strings.TrimSpace(ship.CompanyCode) != "" {
			id, err := resolveCompanyID(ship.CompanyCode)
			if err != nil {
				return nil, err
			}
			manufacturerID = id
		}
		snapshot := shipSnapshot{
			Name:         ship.Name,
			Manufacturer: strings.TrimSpace(manufacturerID),
			ExternalRefs: ensurePrimaryExternalRef(ship.ExternalRefs, external),
			Paints:       normalizePaints(ship.Paints),
		}

		payload := map[string]any{
			"name":          snapshot.Name,
			"manufacturer":  nullableString(snapshot.Manufacturer),
			"external_refs": snapshot.ExternalRefs,
			"paints":        snapshot.Paints,
			"status":        "published",
		}

		if existing, ok := existingByExternal[external]; ok {
			diffPayload := diff.Compute(shipSnapshotMap(existing.Snapshot), shipSnapshotMap(snapshot), []string{"name", "manufacturer", "external_refs", "paints"})
			if diffPayload != nil {
				if _, err := b.client.UpdateOneWithVersion(b.ctx, b.collections.Ships, existing.ID, payload, versionName, promote); err != nil {
					return nil, fmt.Errorf("update ship %s: %w", external, err)
				}
				existing.Snapshot = snapshot
				existingByExternal[external] = existing
			}
			shipIDByExternal[external] = existing.ID
			continue
		}

		created, err := b.client.CreateOneWithVersion(b.ctx, b.collections.Ships, payload, versionName, promote)
		if err != nil {
			return nil, fmt.Errorf("create ship %s: %w", external, err)
		}
		id := toString(created["id"])
		shipIDByExternal[external] = id
		existingByExternal[external] = shipState{ID: id, Snapshot: snapshot}
	}

	return shipIDByExternal, nil
}

func makeShipSnapshotFromRow(row map[string]any) shipSnapshot {
	name := normalizeString(row["name"])
	manufacturer := extractID(row["manufacturer"])
	refs := normalizeExternalRefsInput(row["external_refs"])
	paints := normalizePaintsInput(row["paints"])
	return shipSnapshot{
		Name:         name,
		Manufacturer: manufacturer,
		ExternalRefs: refs,
		Paints:       paints,
	}
}

func shipSnapshotMap(snapshot shipSnapshot) map[string]any {
	return map[string]any{
		"name":          snapshot.Name,
		"manufacturer":  nullableString(snapshot.Manufacturer),
		"external_refs": snapshot.ExternalRefs,
		"paints":        snapshot.Paints,
	}
}

func normalizePaints(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}
		set[v] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func normalizePaintsInput(value any) []string {
	list, ok := value.([]any)
	if !ok {
		return []string{}
	}
	result := []string{}
	for _, entry := range list {
		if str, ok := entry.(string); ok {
			str = strings.TrimSpace(str)
			if str != "" {
				result = append(result, str)
			}
		}
	}
	sort.Strings(result)
	return result
}
