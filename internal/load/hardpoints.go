package load

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/diff"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

type hardpointSnapshot struct {
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
	ItemQuantity *int
	IsLeaf       bool
}

type hardpointState struct {
	ID       string
	Snapshot hardpointSnapshot
}

type hardpointDelete struct {
	ID         string
	ExternalID string
}

func (b *builder) syncHardpoints(hardpoints []model.NormalizedHardpointV2, installed map[string]installedItemAggregate, versionName string, promote bool) (map[string]string, error) {
	b.pendingHardpointDeletes = nil
	hardpoints, duplicateInputs := dedupeHardpointInputs(hardpoints)
	if duplicateInputs > 0 {
		utils.Logger().Warn("Deduplicated incoming hardpoints", "duplicates", duplicateInputs)
	}
	if len(hardpoints) == 0 {
		return map[string]string{}, nil
	}
	fields := []string{"id", "external_id", "code", "category", "position", "size", "gimballed", "powered", "path", "meta", "parent", "parent.id", "item_quantity", "is_leaf"}
	filter := buildEqualityFilter("category", b.allowedHardpointCategoryList)
	rows, err := fetchAllRows(b.ctx, b.client, b.collections.Hardpoints, fields, filter)
	if err != nil {
		return nil, err
	}
	states, pendingDeletes := collapseExistingHardpointStates(rows)
	if len(pendingDeletes) > 0 {
		utils.Logger().Warn("Found duplicate hardpoints in Directus", "duplicates", len(pendingDeletes))
	}
	parentLookup := map[string]string{}
	for external, state := range states {
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
	hardpointIDs := map[string]string{}

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
		parentExternal, pathValue, codeValue := deriveHardpointPath(external, hardpoint.Code)
		parentID := ""
		if parentExternal != "" {
			parentID = parentLookup[parentExternal]
		}
		installedEntry := installed[external]
		quantity := installedEntry.Quantity
		var quantityPtr *int
		if quantity > 0 {
			quantityPtr = &quantity
		}
		hasItem := installedEntry.ItemExternalID != ""
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
			ItemQuantity: quantityPtr,
			IsLeaf:       hasItem,
		}
		payload := map[string]any{
			"external_id":   snapshot.ExternalID,
			"code":          snapshot.Code,
			"category":      snapshot.Category,
			"position":      nullableString(snapshot.Position),
			"size":          snapshot.Size,
			"gimballed":     snapshot.Gimballed,
			"powered":       snapshot.Powered,
			"path":          nullableString(snapshot.Path),
			"meta":          snapshot.Meta,
			"parent":        nullableString(snapshot.Parent),
			"item_quantity": snapshot.ItemQuantity,
			"is_leaf":       snapshot.IsLeaf,
			"status":        "published",
		}
		if existing, ok := states[external]; ok {
			diffPayload := diff.Compute(hardpointSnapshotMap(existing.Snapshot), hardpointSnapshotMap(snapshot), []string{"code", "category", "position", "size", "gimballed", "powered", "path", "meta", "parent", "item_quantity", "is_leaf"})
			if diffPayload != nil {
				if _, err := b.client.UpdateOneWithVersion(b.ctx, b.collections.Hardpoints, existing.ID, payload, versionName, promote); err != nil {
					if versionErr := handleVersionError(err); versionErr != nil {
						return nil, fmt.Errorf("update hardpoint %s: %w", external, versionErr)
					}
				}
				existing.Snapshot = snapshot
				states[external] = existing
			} else {
				if err := b.ensureVersionSnapshot(b.collections.Hardpoints, existing.ID, payload, versionName, promote); err != nil {
					if versionErr := handleVersionError(err); versionErr != nil {
						return nil, fmt.Errorf("version hardpoint %s: %w", external, versionErr)
					}
				}
			}
			parentLookup[external] = existing.ID
			if existing.ID != "" {
				hardpointIDs[external] = existing.ID
			}
			continue
		}
		created, err := b.client.CreateOneWithVersion(b.ctx, b.collections.Hardpoints, payload, versionName, promote)
		if err != nil {
			if versionErr := handleVersionError(err); versionErr != nil {
				return nil, fmt.Errorf("create hardpoint %s: %w", external, versionErr)
			}
		}
		id := toString(created["id"])
		states[external] = hardpointState{ID: id, Snapshot: snapshot}
		parentLookup[external] = id
		if id != "" {
			hardpointIDs[external] = id
		}
	}
	for external, state := range states {
		if _, keep := hardpointIDs[external]; keep {
			continue
		}
		if state.ID == "" {
			continue
		}
		pendingDeletes = append(pendingDeletes, hardpointDelete{ID: state.ID, ExternalID: external})
	}
	b.pendingHardpointDeletes = normalizeHardpointDeletes(pendingDeletes)
	return hardpointIDs, nil
}

func makeHardpointSnapshotFromRow(row map[string]any) hardpointSnapshot {
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
	quantity, _ := extractInt(row["item_quantity"])
	isLeafPtr, _ := extractBool(row["is_leaf"])
	isLeaf := false
	if isLeafPtr != nil {
		isLeaf = *isLeafPtr
	}
	return hardpointSnapshot{
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
		ItemQuantity: quantity,
		IsLeaf:       isLeaf,
	}
}

func hardpointSnapshotMap(snapshot hardpointSnapshot) map[string]any {
	return map[string]any{
		"code":          snapshot.Code,
		"category":      snapshot.Category,
		"position":      nullableString(snapshot.Position),
		"size":          snapshot.Size,
		"gimballed":     snapshot.Gimballed,
		"powered":       snapshot.Powered,
		"path":          nullableString(snapshot.Path),
		"meta":          snapshot.Meta,
		"parent":        nullableString(snapshot.Parent),
		"item_quantity": snapshot.ItemQuantity,
		"is_leaf":       snapshot.IsLeaf,
	}
}

func dedupeHardpointInputs(hardpoints []model.NormalizedHardpointV2) ([]model.NormalizedHardpointV2, int) {
	if len(hardpoints) == 0 {
		return nil, 0
	}
	byExternal := make(map[string]model.NormalizedHardpointV2, len(hardpoints))
	duplicates := 0
	for _, hardpoint := range hardpoints {
		external := strings.ToUpper(strings.TrimSpace(hardpoint.ExternalID))
		if external == "" {
			continue
		}
		hardpoint.ExternalID = external
		hardpoint.ShipVariantExternal = strings.ToUpper(strings.TrimSpace(hardpoint.ShipVariantExternal))
		hardpoint.Code = strings.TrimSpace(hardpoint.Code)
		hardpoint.Category = strings.TrimSpace(hardpoint.Category)
		if existing, ok := byExternal[external]; ok {
			duplicates++
			byExternal[external] = mergeHardpointInput(existing, hardpoint)
			continue
		}
		byExternal[external] = hardpoint
	}
	result := make([]model.NormalizedHardpointV2, 0, len(byExternal))
	for _, hardpoint := range byExternal {
		result = append(result, hardpoint)
	}
	return result, duplicates
}

func mergeHardpointInput(primary model.NormalizedHardpointV2, candidate model.NormalizedHardpointV2) model.NormalizedHardpointV2 {
	if primary.ShipVariantExternal == "" {
		primary.ShipVariantExternal = candidate.ShipVariantExternal
	}
	if primary.Code == "" {
		primary.Code = candidate.Code
	}
	if primary.Category == "" || strings.EqualFold(primary.Category, "unknown") {
		if category := strings.TrimSpace(candidate.Category); category != "" && !strings.EqualFold(category, "unknown") {
			primary.Category = category
		}
	}
	if primary.Position == nil && candidate.Position != nil {
		primary.Position = candidate.Position
	}
	if primary.Size == nil && candidate.Size != nil {
		primary.Size = candidate.Size
	}
	if primary.Gimballed == nil && candidate.Gimballed != nil {
		primary.Gimballed = candidate.Gimballed
	}
	if primary.Powered == nil && candidate.Powered != nil {
		primary.Powered = candidate.Powered
	}
	if primary.Seats == nil && candidate.Seats != nil {
		primary.Seats = candidate.Seats
	}
	return primary
}

func collapseExistingHardpointStates(rows []map[string]any) (map[string]hardpointState, []hardpointDelete) {
	states := map[string]hardpointState{}
	pendingDeletes := []hardpointDelete{}
	for _, row := range rows {
		external := strings.ToUpper(normalizeString(row["external_id"]))
		if external == "" {
			continue
		}
		state := hardpointState{ID: toString(row["id"]), Snapshot: makeHardpointSnapshotFromRow(row)}
		if existing, exists := states[external]; exists {
			keeper, duplicate := selectHardpointKeeper(existing, state)
			states[external] = keeper
			if duplicate.ID != "" {
				pendingDeletes = append(pendingDeletes, hardpointDelete{ID: duplicate.ID, ExternalID: external})
			}
			continue
		}
		states[external] = state
	}
	return states, normalizeHardpointDeletes(pendingDeletes)
}

func selectHardpointKeeper(current hardpointState, candidate hardpointState) (hardpointState, hardpointState) {
	if preferHardpointID(candidate.ID, current.ID) {
		return candidate, current
	}
	return current, candidate
}

func preferHardpointID(candidate string, current string) bool {
	candidate = strings.TrimSpace(candidate)
	current = strings.TrimSpace(current)
	if current == "" {
		return candidate != ""
	}
	if candidate == "" {
		return false
	}
	candidateInt, candidateErr := strconv.ParseInt(candidate, 10, 64)
	currentInt, currentErr := strconv.ParseInt(current, 10, 64)
	if candidateErr == nil && currentErr == nil {
		return candidateInt < currentInt
	}
	return candidate < current
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

func normalizeHardpointDeletes(entries []hardpointDelete) []hardpointDelete {
	if len(entries) == 0 {
		return nil
	}
	seen := map[string]hardpointDelete{}
	for _, entry := range entries {
		id := strings.TrimSpace(entry.ID)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = hardpointDelete{
			ID:         id,
			ExternalID: strings.ToUpper(strings.TrimSpace(entry.ExternalID)),
		}
	}
	result := make([]hardpointDelete, 0, len(seen))
	for _, entry := range seen {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		leftDepth := hardpointDepth(result[i].ExternalID)
		rightDepth := hardpointDepth(result[j].ExternalID)
		if leftDepth == rightDepth {
			if result[i].ExternalID == result[j].ExternalID {
				return result[i].ID < result[j].ID
			}
			return result[i].ExternalID < result[j].ExternalID
		}
		return leftDepth > rightDepth
	})
	return result
}

func (b *builder) cleanupPendingHardpoints() error {
	if len(b.pendingHardpointDeletes) == 0 {
		return nil
	}
	depthBuckets := map[int][]string{}
	depths := []int{}
	for _, entry := range b.pendingHardpointDeletes {
		depth := hardpointDepth(entry.ExternalID)
		if _, exists := depthBuckets[depth]; !exists {
			depths = append(depths, depth)
		}
		depthBuckets[depth] = append(depthBuckets[depth], entry.ID)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(depths)))
	for _, depth := range depths {
		if err := bulkDelete(b.ctx, b.client, b.collections.Hardpoints, depthBuckets[depth]); err != nil {
			return fmt.Errorf("delete hardpoints: %w", err)
		}
	}
	b.pendingHardpointDeletes = nil
	return nil
}
