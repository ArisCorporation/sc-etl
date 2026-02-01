package load

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/transform"
)

type shipSnapshot struct {
	Name         string
	Manufacturer string
	ExternalRefs []model.NormalizedExternalReference
	Paints       []string
}

type shipState struct {
	ID        string
	Snapshot  shipSnapshot
	Composite string
	RefKeys   []string
	Matched   bool
}

func (b *builder) syncShips(grouping *transform.ShipGrouping, resolveCompanyID func(string) (string, error), ships []model.NormalizedShipV2, versionName string, promote bool) (map[string]string, error) {
	fields := []string{"id", "name", "manufacturer", "manufacturer.id", "external_refs", "paints"}
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.Ships, fields, nil)
	if err != nil {
		return nil, err
	}

	byComposite := map[string]*shipState{}
	byRef := map[string]*shipState{}
	byID := map[string]*shipState{}
	for _, row := range rows {
		state := makeShipState(toString(row["id"]), makeShipSnapshotFromRow(row))
		attachShipState(state, byComposite, byRef)
		byID[state.ID] = state
	}

	shipIDByExternal := map[string]string{}

	for _, ship := range ships {
		external := strings.ToUpper(strings.TrimSpace(ship.ExternalID))
		if external == "" {
			continue
		}
		if grouping != nil && !shipDefinedInGrouping(grouping, ship) {
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

		composite := shipCompositeKeyValue(snapshot.Manufacturer, snapshot.Name)
		state := lookupShipState(composite, snapshot.ExternalRefs, byComposite, byRef)

		if state != nil {
			snapshot.Name = preferString(snapshot.Name, state.Snapshot.Name)
			snapshot.Manufacturer = preferString(snapshot.Manufacturer, state.Snapshot.Manufacturer)
			snapshot.ExternalRefs = preferExternalRefs(snapshot.ExternalRefs, state.Snapshot.ExternalRefs)
			snapshot.Paints = preferStrings(snapshot.Paints, state.Snapshot.Paints)
			payload["name"] = snapshot.Name
			payload["manufacturer"] = nullableString(snapshot.Manufacturer)
			payload["external_refs"] = snapshot.ExternalRefs
			payload["paints"] = snapshot.Paints
		}

		if state != nil {
			diffPayload := diff.Compute(shipSnapshotMap(state.Snapshot), shipSnapshotMap(snapshot), []string{"name", "manufacturer", "external_refs", "paints"})
			if diffPayload != nil {
				detachShipState(state, byComposite, byRef)
				if _, err := b.client.UpdateOneWithVersion(b.ctx, b.collections.Ships, state.ID, payload, versionName, promote); err != nil {
					if versionErr := handleVersionError(err); versionErr != nil {
						return nil, fmt.Errorf("update ship %s: %w", external, versionErr)
					}
				}
				state.Snapshot = snapshot
				state.Composite = shipCompositeKeyValue(snapshot.Manufacturer, snapshot.Name)
				state.RefKeys = buildRefKeys(snapshot.ExternalRefs)
				attachShipState(state, byComposite, byRef)
			} else {
				if err := b.ensureVersionSnapshot(b.collections.Ships, state.ID, payload, versionName, promote); err != nil {
					if versionErr := handleVersionError(err); versionErr != nil {
						return nil, fmt.Errorf("version ship %s: %w", external, versionErr)
					}
				}
			}
			state.Matched = true
			shipIDByExternal[external] = state.ID
			continue
		}

		created, err := b.client.CreateOneWithVersion(b.ctx, b.collections.Ships, payload, versionName, promote)
		if err != nil {
			if versionErr := handleVersionError(err); versionErr != nil {
				return nil, fmt.Errorf("create ship %s: %w", external, versionErr)
			}
		}
		id := toString(created["id"])
		shipIDByExternal[external] = id
		newState := makeShipState(id, snapshot)
		newState.Matched = true
		attachShipState(newState, byComposite, byRef)
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
			if err := b.client.DeleteMany(b.ctx, b.collections.Ships, batch); err != nil {
				return nil, fmt.Errorf("delete ships: %w", err)
			}
		}
	}

	return shipIDByExternal, nil
}

func shipDefinedInGrouping(grouping *transform.ShipGrouping, ship model.NormalizedShipV2) bool {
	if grouping == nil {
		return true
	}
	if _, ok := grouping.GetHull(ship.ExternalID); ok {
		return true
	}
	candidates := []string{ship.ExternalID}
	for _, ref := range ship.ExternalRefs {
		if id := strings.TrimSpace(ref.ID); id != "" {
			candidates = append(candidates, id)
		}
	}
	if _, ok := grouping.LookupShipID(candidates...); ok {
		return true
	}
	return false
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

func makeShipState(id string, snapshot shipSnapshot) *shipState {
	composite := shipCompositeKeyValue(snapshot.Manufacturer, snapshot.Name)
	return &shipState{
		ID:        id,
		Snapshot:  snapshot,
		Composite: composite,
		RefKeys:   buildRefKeys(snapshot.ExternalRefs),
		Matched:   false,
	}
}

func attachShipState(state *shipState, byComposite map[string]*shipState, byRef map[string]*shipState) {
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

func detachShipState(state *shipState, byComposite map[string]*shipState, byRef map[string]*shipState) {
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

func lookupShipState(composite string, refs []model.NormalizedExternalReference, byComposite map[string]*shipState, byRef map[string]*shipState) *shipState {
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

func shipCompositeKeyValue(manufacturerID, name string) string {
	manufacturerID = strings.TrimSpace(manufacturerID)
	if manufacturerID == "" {
		return ""
	}
	normalized := manufacturerID
	return shipCompositeKey(&normalized, name)
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
