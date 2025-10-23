package load

import (
	"fmt"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

type itemSnapshot struct {
	Name           string
	Type           string
	Subtype        string
	Size           *int
	Grade          string
	Class          string
	ManufacturerID string
	Stats          map[string]any
	ExternalRefs   []model.NormalizedExternalReference
}

type itemState struct {
	ID       string
	Snapshot itemSnapshot
}

func (b *builder) syncItems(resolveCompanyID func(string) (string, error), items []model.NormalizedItemV2, versionName string, promote bool) (map[string]string, error) {
	fields := []string{"id", "external_id", "name", "type", "subtype", "size", "grade", "class", "manufacturer", "manufacturer.id", "stats", "external_refs"}
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.Items, fields, nil)
	if err != nil {
		return nil, err
	}
	states := map[string]itemState{}
	for _, row := range rows {
		external := strings.ToUpper(normalizeString(row["external_id"]))
		if external == "" {
			continue
		}
		states[external] = itemState{ID: toString(row["id"]), Snapshot: makeItemSnapshotFromRow(row)}
	}

	itemIDs := map[string]string{}

	for _, item := range items {
		external := strings.ToUpper(strings.TrimSpace(item.ExternalID))
		if external == "" {
			continue
		}
		manufacturerID := ""
		if item.CompanyCode != nil {
			code := strings.TrimSpace(*item.CompanyCode)
			if code != "" {
				id, err := resolveCompanyID(code)
				if err != nil {
					return nil, err
				}
				manufacturerID = id
			}
		}
		stats := map[string]any{}
		for key, value := range item.Stats {
			stats[key] = value
		}
		snapshot := itemSnapshot{
			Name:           item.Name,
			Type:           strings.ToUpper(strings.TrimSpace(item.Type)),
			Subtype:        strings.TrimSpace(stringFromPtr(item.Subtype)),
			Size:           item.Size,
			Grade:          strings.TrimSpace(stringFromPtr(item.Grade)),
			Class:          strings.TrimSpace(stringFromPtr(item.Class)),
			ManufacturerID: strings.TrimSpace(manufacturerID),
			Stats:          stats,
			ExternalRefs:   sortRefs(cloneRefs(item.ExternalRefs)),
		}

		payload := map[string]any{
			"external_id":   external,
			"name":          snapshot.Name,
			"type":          snapshot.Type,
			"subtype":       nullableString(snapshot.Subtype),
			"size":          snapshot.Size,
			"grade":         nullableString(snapshot.Grade),
			"class":         nullableString(snapshot.Class),
			"manufacturer":  nullableString(snapshot.ManufacturerID),
			"stats":         snapshot.Stats,
			"external_refs": snapshot.ExternalRefs,
			"status":        "published",
		}

		if existing, ok := states[external]; ok {
			diffPayload := diff.Compute(itemSnapshotMap(existing.Snapshot), itemSnapshotMap(snapshot), []string{"name", "type", "subtype", "size", "grade", "class", "manufacturer", "stats", "external_refs"})
			if diffPayload != nil {
				if _, err := b.client.UpdateOneWithVersion(b.ctx, b.collections.Items, existing.ID, payload, versionName, promote); err != nil {
					return nil, fmt.Errorf("update item %s: %w", external, err)
				}
				existing.Snapshot = snapshot
				states[external] = existing
			}
			itemIDs[external] = existing.ID
			continue
		}

		created, err := b.client.CreateOneWithVersion(b.ctx, b.collections.Items, payload, versionName, promote)
		if err != nil {
			return nil, fmt.Errorf("create item %s: %w", external, err)
		}
		id := toString(created["id"])
		itemIDs[external] = id
		states[external] = itemState{ID: id, Snapshot: snapshot}
	}

	return itemIDs, nil
}

func makeItemSnapshotFromRow(row map[string]any) itemSnapshot {
	name := normalizeString(row["name"])
	typeName := strings.ToUpper(normalizeString(row["type"]))
	subtype := normalizeString(row["subtype"])
	size, _ := extractInt(row["size"])
	grade := normalizeString(row["grade"])
	class := normalizeString(row["class"])
	manufacturer := extractID(row["manufacturer"])
	stats := map[string]any{}
	if payload, ok := row["stats"].(map[string]any); ok {
		stats = payload
	}
	refs := normalizeExternalRefsInput(row["external_refs"])
	return itemSnapshot{
		Name:           name,
		Type:           typeName,
		Subtype:        subtype,
		Size:           size,
		Grade:          grade,
		Class:          class,
		ManufacturerID: manufacturer,
		Stats:          stats,
		ExternalRefs:   refs,
	}
}

func itemSnapshotMap(snapshot itemSnapshot) map[string]any {
	return map[string]any{
		"name":          snapshot.Name,
		"type":          snapshot.Type,
		"subtype":       nullableString(snapshot.Subtype),
		"size":          snapshot.Size,
		"grade":         nullableString(snapshot.Grade),
		"class":         nullableString(snapshot.Class),
		"manufacturer":  nullableString(snapshot.ManufacturerID),
		"stats":         snapshot.Stats,
		"external_refs": snapshot.ExternalRefs,
	}
}
