package load

import (
	"fmt"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
)

var hardpointCategoryAllowlist = []string{
	"Shield",
	"ShieldController",
	"QuantumDrive",
	"Cooler",
	"CoolerController",
	"PowerPlant",
	"FuelTank",
	"ExternalFuelTank",
	"FuelIntake",
	"QuantumFuelTank",
	"Radar",
	"AIModule",
	"WeaponGun",
	"RailGun",
	"WeaponDefensive",
	"WeaponController",
	"WeaponMount",
	"Missile",
	"MissileLauncher",
	"MissileController",
	"Turret",
	"TurretBase",
	"UtilityTurret",
	"Armor",
	"MainThruster",
	"ManneuverThruster",
	"Relay",
	"ToolArm",
	"SalvageHead",
	"SalvageController",
	"SalvageModifier",
	"SalvageFillerStation",
	"SalvageFieldEmitter",
	"SalvageFieldSupporter",
	"SalvageInternalStorage",
	"MiningController",
	"WeaponMining",
	"TractorBeam",
	"TowingBeam",
	"CapacitorAssignmentController",
	"CommsController",
	"EnergyController",
	"FuelController",
	"FlightController",
	"LandingSystem",
	"DockingCollar",
	"Cargo",
	"JumpDrive",
	"LifeSupportGenerator",
	"WheeledController",
	"Container",
	"AttachedPart",
	"SelfDestruct",
}

var hardpointCategorySet = func() map[string]struct{} {
	set := map[string]struct{}{}
	for _, value := range hardpointCategoryAllowlist {
		set[strings.ToUpper(value)] = struct{}{}
	}
	return set
}()

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
	filter := map[string]any{
		"_or": []map[string]any{},
	}
	for _, category := range hardpointCategoryAllowlist {
		filter["_or"] = append(filter["_or"].([]map[string]any), map[string]any{"category": map[string]any{"_eq": category}})
	}
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

	for _, hardpoint := range hardpoints {
		external := strings.ToUpper(strings.TrimSpace(hardpoint.ExternalID))
		if external == "" {
			continue
		}
		categoryUpper := strings.ToUpper(strings.TrimSpace(hardpoint.Category))
		if _, ok := hardpointCategorySet[categoryUpper]; !ok {
			continue
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
		if installedEntry.ItemExternalID != "" {
			itemID = itemIDs[strings.ToUpper(installedEntry.ItemExternalID)]
		}
		quantity := installedEntry.Quantity
		var quantityPtr *int
		if quantity > 0 {
			quantityPtr = &quantity
		}
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
			IsLeaf:       itemID != "",
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
					return fmt.Errorf("update hardpoint %s: %w", external, err)
				}
				existing.Snapshot = snapshot
				states[external] = existing
			}
			parentLookup[external] = existing.ID
			continue
		}
		created, err := b.client.CreateOneWithVersion(b.ctx, b.collections.Hardpoints, payload, versionName, promote)
		if err != nil {
			return fmt.Errorf("create hardpoint %s: %w", external, err)
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
