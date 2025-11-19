package load

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

type hardpointSnapshot struct {
	ShipVariant  string
	Code         string
	Category     string
	Position     string
	Size         *int
	Gimballed    *bool
	Powered      *bool
	Path         string
	Meta         map[string]any
	ExternalID   string
	Parent       string
	Item         string
	ItemQuantity *int
	IsLeaf       bool
}

type hardpointState struct {
	ID       string
	Snapshot hardpointSnapshot
}

func (b *builder) syncHardpoints(hardpoints []model.NormalizedHardpointV2, variantIDs map[string]string, installed map[string]installedItemAggregate, itemIDs map[string]string, versionName string, promote bool) error {
	if len(hardpoints) == 0 {
		return nil
	}
	fields := []string{"id", "external_id", "ship_variant", "ship_variant.id", "code", "category", "position", "size", "gimballed", "powered", "path", "meta", "parent", "parent.id", "item", "item.id", "item_quantity", "is_leaf"}
	filter := buildEqualityFilter("category", b.allowedHardpointCategoryList)
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.Hardpoints, fields, filter)
	if err != nil {
		return err
	}
	states := map[string]hardpointState{}
	parentLookup := map[string]string{}
	for _, row := range rows {
		external := strings.ToUpper(normalizeString(row["external_id"]))
		if external == "" {
			continue
		}
		state := hardpointState{ID: toString(row["id"]), Snapshot: makeHardpointSnapshotFromRow(row)}
		states[external] = state
		parentLookup[external] = state.ID
	}

	sort.Slice(hardpoints, func(i, j int) bool {
		left := strings.ToUpper(strings.TrimSpace(hardpoints[i].ExternalID))
		right := strings.ToUpper(strings.TrimSpace(hardpoints[j].ExternalID))
		leftDepth := hardpointDepth(left)
		rightDepth := hardpointDepth(right)
		if leftDepth == rightDepth {
			return left < right
		}
		return leftDepth < rightDepth
	})

	seen := map[string]struct{}{}

	for _, hardpoint := range hardpoints {
		external := strings.ToUpper(strings.TrimSpace(hardpoint.ExternalID))
		if external == "" {
			continue
		}
		if _, exists := seen[external]; exists {
			continue
		}
		seen[external] = struct{}{}
		categoryUpper := strings.ToUpper(strings.TrimSpace(hardpoint.Category))
		if categoryUpper == "" {
			continue
		}
		if len(b.allowedHardpointCategories) > 0 {
			if _, ok := b.allowedHardpointCategories[categoryUpper]; !ok {
				continue
			}
		}
		variantID := variantIDs[strings.ToUpper(strings.TrimSpace(hardpoint.ShipVariantExternal))]
		if variantID == "" {
			return fmt.Errorf("missing variant mapping for hardpoint %s", hardpoint.ShipVariantExternal)
		}
		parentExternal, pathValue, codeValue := deriveHardpointPath(external, hardpoint.Code)
		parentID := ""
		if parentExternal != "" {
			parentID = parentLookup[parentExternal]
		}
		installedEntry := installed[external]
		itemID := ""
		quantity := installedEntry.Quantity
		if installedEntry.ItemExternalID != "" {
			itemID = itemIDs[strings.ToUpper(installedEntry.ItemExternalID)]
			if itemID == "" {
				quantity = 0
			}
		}
		var quantityPtr *int
		if itemID != "" && quantity > 0 {
			quantityPtr = &quantity
		}
		hasItem := itemID != ""
		meta := map[string]any{}
		if hardpoint.Seats != nil {
			meta["seats"] = *hardpoint.Seats
		}
		var metaPayload map[string]any
		if len(meta) > 0 {
			metaPayload = meta
		}
		var gimballedPtr, poweredPtr *bool
		if hardpoint.Gimballed != nil {
			value := *hardpoint.Gimballed
			gimballedPtr = &value
		}
		if hardpoint.Powered != nil {
			value := *hardpoint.Powered
			poweredPtr = &value
		}
		position := ""
		if hardpoint.Position != nil {
			position = *hardpoint.Position
		}
		snapshot := hardpointSnapshot{
			ShipVariant:  variantID,
			Code:         codeValue,
			Category:     strings.TrimSpace(hardpoint.Category),
			Position:     strings.TrimSpace(position),
			Size:         hardpoint.Size,
			Gimballed:    gimballedPtr,
			Powered:      poweredPtr,
			Path:         pathValue,
			Meta:         metaPayload,
			ExternalID:   external,
			Parent:       parentID,
			Item:         itemID,
			ItemQuantity: quantityPtr,
			IsLeaf:       hasItem,
		}
		payload := map[string]any{
			"external_id":   snapshot.ExternalID,
			"ship_variant":  snapshot.ShipVariant,
			"code":          snapshot.Code,
			"category":      snapshot.Category,
			"position":      nullableString(snapshot.Position),
			"size":          snapshot.Size,
			"gimballed":     snapshot.Gimballed,
			"powered":       snapshot.Powered,
			"path":          nullableString(snapshot.Path),
			"meta":          snapshot.Meta,
			"parent":        nullableString(snapshot.Parent),
			"item":          nullableString(snapshot.Item),
			"item_quantity": snapshot.ItemQuantity,
			"is_leaf":       snapshot.IsLeaf,
			"status":        "published",
		}
		if existing, ok := states[external]; ok {
			diffPayload := diff.Compute(hardpointSnapshotMap(existing.Snapshot), hardpointSnapshotMap(snapshot), []string{"ship_variant", "code", "category", "position", "size", "gimballed", "powered", "path", "meta", "parent", "item", "item_quantity", "is_leaf"})
			if diffPayload != nil {
				if _, err := b.client.UpdateOneWithVersion(b.ctx, b.collections.Hardpoints, existing.ID, payload, versionName, promote); err != nil {
					if versionErr := handleVersionError(err); versionErr != nil {
						return fmt.Errorf("update hardpoint %s: %w", external, versionErr)
					}
				}
				existing.Snapshot = snapshot
				states[external] = existing
			}
			parentLookup[external] = existing.ID
			continue
		}
		created, err := b.client.CreateOneWithVersion(b.ctx, b.collections.Hardpoints, payload, versionName, promote)
		if err != nil {
			if versionErr := handleVersionError(err); versionErr != nil {
				return fmt.Errorf("create hardpoint %s: %w", external, versionErr)
			}
		}
		id := toString(created["id"])
		states[external] = hardpointState{ID: id, Snapshot: snapshot}
		parentLookup[external] = id
	}
	return nil
}

func makeHardpointSnapshotFromRow(row map[string]any) hardpointSnapshot {
	shipVariant := extractID(row["ship_variant"])
	code := normalizeString(row["code"])
	category := normalizeString(row["category"])
	position := normalizeString(row["position"])
	size, _ := extractInt(row["size"])
	gimballed, _ := extractBool(row["gimballed"])
	powered, _ := extractBool(row["powered"])
	path := normalizeString(row["path"])
	meta := map[string]any{}
	if payload, ok := row["meta"].(map[string]any); ok {
		meta = payload
	}
	parent := extractID(row["parent"])
	item := extractID(row["item"])
	quantity, _ := extractInt(row["item_quantity"])
	isLeafPtr, _ := extractBool(row["is_leaf"])
	isLeaf := false
	if isLeafPtr != nil {
		isLeaf = *isLeafPtr
	}
	return hardpointSnapshot{
		ShipVariant:  shipVariant,
		Code:         code,
		Category:     category,
		Position:     position,
		Size:         size,
		Gimballed:    gimballed,
		Powered:      powered,
		Path:         path,
		Meta:         meta,
		ExternalID:   strings.ToUpper(normalizeString(row["external_id"])),
		Parent:       parent,
		Item:         item,
		ItemQuantity: quantity,
		IsLeaf:       isLeaf,
	}
}

func hardpointSnapshotMap(snapshot hardpointSnapshot) map[string]any {
	return map[string]any{
		"ship_variant":  snapshot.ShipVariant,
		"code":          snapshot.Code,
		"category":      snapshot.Category,
		"position":      nullableString(snapshot.Position),
		"size":          snapshot.Size,
		"gimballed":     snapshot.Gimballed,
		"powered":       snapshot.Powered,
		"path":          nullableString(snapshot.Path),
		"meta":          snapshot.Meta,
		"parent":        nullableString(snapshot.Parent),
		"item":          nullableString(snapshot.Item),
		"item_quantity": snapshot.ItemQuantity,
		"is_leaf":       snapshot.IsLeaf,
	}
}

func deriveHardpointPath(externalID string, fallbackCode string) (parentExternal string, path string, code string) {
	parts := strings.SplitN(externalID, ":", 2)
	if len(parts) < 2 {
		return "", "", fallbackCode
	}
	variant := parts[0]
	rest := parts[1]
	segments := strings.Split(rest, "/")
	code = fallbackCode
	if len(segments) > 0 {
		code = segments[len(segments)-1]
	}
	if len(segments) > 1 {
		parentExternal = variant + ":" + strings.Join(segments[:len(segments)-1], "/")
	}
	return parentExternal, rest, code
}

func hardpointDepth(external string) int {
	external = strings.TrimSpace(external)
	if external == "" {
		return 0
	}
	parts := strings.SplitN(external, ":", 2)
	if len(parts) < 2 || parts[1] == "" {
		return 1
	}
	return len(strings.Split(parts[1], "/"))
}
