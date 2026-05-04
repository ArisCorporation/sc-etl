package load

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/transform"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

type builder struct {
	ctx                          context.Context
	client                       *directus.Client
	result                       *transform.Result
	collections                  collections
	build                        buildRecord
	promoteEnabled               bool
	defaultCompanyCategory       *string
	allowedItemTypeList          []string
	allowedItemTypes             map[string]struct{}
	allowedHardpointCategoryList []string
	allowedHardpointCategories   map[string]struct{}
	skipMediaUpload              bool

	statsCompanies  int
	statsShips      int
	statsVariants   int
	statsItems      int
	statsHardpoints int

	rsiMedia                map[string]rsiMedia
	fieldExistsCache        map[string]map[string]bool
	fileImportCache         map[string]string
	hullMediaCache          map[string]hullMediaState
	pendingHardpointDeletes []hardpointDelete
}

func newBuilder(ctx context.Context, client *directus.Client, result *transform.Result, opts Options) *builder {
	var defaultCategory *string
	if candidate := strings.TrimSpace(opts.DefaultCompanyCategory); candidate != "" {
		defaultCategory = &candidate
	}
	promoteEnabled := true
	if opts.PromoteVersions != nil {
		promoteEnabled = *opts.PromoteVersions
	}
	itemList, itemSet := normalizeAllowlist(opts.AllowedItemTypes)
	hpList, hpSet := normalizeAllowlist(opts.AllowedHardpointCategories)
	return &builder{
		ctx:                          ctx,
		client:                       client,
		result:                       result,
		collections:                  loadCollections(),
		promoteEnabled:               promoteEnabled,
		defaultCompanyCategory:       defaultCategory,
		allowedItemTypeList:          itemList,
		allowedItemTypes:             itemSet,
		allowedHardpointCategoryList: hpList,
		allowedHardpointCategories:   hpSet,
		skipMediaUpload:              envBool("SKIP_MEDIA_UPLOAD", false),
		fieldExistsCache:             map[string]map[string]bool{},
		fileImportCache:              map[string]string{},
		hullMediaCache:               map[string]hullMediaState{},
	}
}

func (b *builder) stats() LoadStatistics {
	return LoadStatistics{
		Companies:    b.statsCompanies,
		Ships:        b.statsShips,
		ShipVariants: b.statsVariants,
		Items:        b.statsItems,
		Hardpoints:   b.statsHardpoints,
	}
}

func loadCollections() collections {
	return collections{
		Companies:           getenv("SC_COMPANY_COLLECTION", "companies"),
		Ships:               getenv("SC_SHIP_COLLECTION", "ship_hulls"),
		ShipVariants:        getenv("SC_SHIP_VARIANT_COLLECTION", "ship_variants"),
		Items:               getenv("SC_ITEM_COLLECTION", "items"),
		Hardpoints:          getenv("SC_HARDPOINT_COLLECTION", "ship_hardpoints"),
		ShipConfigurations:  getenv("SC_SHIP_CONFIGURATION_COLLECTION", "ship_variant_configurations"),
		ShipConfigurationHP: getenv("SC_SHIP_CONFIGURATION_HP_COLLECTION", "ship_variant_configuration_hardpoints"),
	}
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

type collections struct {
	Companies           string
	Ships               string
	ShipVariants        string
	Items               string
	Hardpoints          string
	ShipConfigurations  string
	ShipConfigurationHP string
}

type buildMetadata struct {
	Hash       string
	ReleasedAt string
	Status     string
}

type buildRecord struct {
	ID         string
	Status     string
	Hash       string
	ReleasedAt string
}

type hullMediaState struct {
	Loaded  bool
	Store   mediaFileState
	Gallery []mediaFileState
}

type mediaFileState struct {
	ID          string
	Description string
}

func (b *builder) ensureBuild() error {
	metadata, err := readBuildMetadata(b.result.NormalizedDir)
	if err != nil {
		return err
	}
	record, err := ensureBuild(b.ctx, b.client, b.result.V2.Channel, b.result.V2.Version, metadata)
	if err != nil {
		return err
	}
	b.build = record
	return nil
}

func (b *builder) sync() error {
	shipGrouping := transform.LoadShipGrouping()
	versionName := fmt.Sprintf("V%s-%s", b.result.V2.Version, b.result.V2.Channel)
	promoteVersions := b.promoteEnabled && b.result.V2.Channel == model.ChannelLive

	// Split stats and hardpoints
	statsByVariant, splitHardpoints := splitVariantStats(b.result.V2)

	var legacyHardpoints []model.NormalizedHardpointV2
	var skippedHardpoints int
	if len(b.result.V2.Hardpoints) > 0 {
		legacyHardpoints, skippedHardpoints = b.filterHardpointsList(b.result.V2.Hardpoints)
	} else {
		legacyHardpoints, skippedHardpoints = b.filterHardpointsList(splitHardpoints)
	}
	if skippedHardpoints > 0 {
		utils.Logger().Info("Skipping hardpoints outside allowlist", "skipped", skippedHardpoints)
	}

	installedByHardpoint := buildInstalledItemMap(b.result.Legacy.InstalledItems)

	companyIDs, err := b.syncCompanies(b.result.V2.Companies)
	if err != nil {
		return err
	}
	b.statsCompanies = len(companyIDs)

	companyResolver := utils.NewCompanyResolver(b.client, b.collections.Companies)
	if err := companyResolver.Warmup(b.ctx); err != nil {
		return err
	}

	resolveCompanyID := b.companyResolverFunc(companyIDs, companyResolver)

	hullIDs, err := b.syncShips(shipGrouping, resolveCompanyID, b.result.V2.Ships, versionName, promoteVersions)
	if err != nil {
		return err
	}
	b.statsShips = len(hullIDs)

	variantIDs, err := b.syncShipVariants(b.result.V2.ShipVariants, statsByVariant, hullIDs, versionName, promoteVersions)
	if err != nil {
		return err
	}
	b.statsVariants = len(variantIDs)

	itemIDs, err := b.syncItems(resolveCompanyID, b.result.V2.Items, versionName, promoteVersions)
	if err != nil {
		return err
	}
	b.statsItems = len(itemIDs)

	hardpointIDMap, err := b.syncHardpoints(legacyHardpoints, installedByHardpoint, versionName, promoteVersions)
	if err != nil {
		return err
	}
	b.statsHardpoints = len(hardpointIDMap)

	configs, err := b.syncShipConfigurations(shipGrouping, b.result.V2.ShipVariants, variantIDs)
	if err != nil {
		return err
	}

	if err := b.syncShipConfigurationHardpoints(configs, b.result.Legacy.InstalledItems, hardpointIDMap, itemIDs); err != nil {
		return err
	}

	if err := b.cleanupPendingHardpoints(); err != nil {
		return err
	}

	if err := markBuildIngested(b.ctx, b.client, b.build.ID); err != nil {
		return err
	}

	return nil
}

func (b *builder) companyResolverFunc(seed map[string]string, resolver *utils.CompanyResolver) func(string) (string, error) {
	cache := map[string]*string{}
	return func(code string) (string, error) {
		normalized := strings.TrimSpace(strings.ToUpper(code))
		cacheKey := normalized
		if cacheKey == "" {
			cacheKey = "__EMPTY__"
		}
		if cached, ok := cache[cacheKey]; ok {
			if cached == nil {
				return "", nil
			}
			return *cached, nil
		}
		if normalized == "" {
			cache[cacheKey] = nil
			return "", nil
		}
		if seeded, ok := seed[normalized]; ok {
			cache[cacheKey] = &seeded
			return seeded, nil
		}
		id, err := resolver.LookupID(b.ctx, normalized)
		if err != nil {
			return "", err
		}
		if id == "" {
			cache[cacheKey] = nil
			return "", nil
		}
		cache[cacheKey] = &id
		return id, nil
	}
}

func handleVersionError(err error) error {
	if err == nil {
		return nil
	}
	var promoteErr *directus.VersionPromotionError
	if errors.As(err, &promoteErr) {
		message := promoteErr.Err.Error()
		if message == "" {
			message = promoteErr.Error()
		}
		utils.Logger().Error("Directus version promotion failed",
			"collection", promoteErr.Collection,
			"item", promoteErr.ItemID,
			"version", promoteErr.VersionKey,
			"error", message)
		return err
	}
	var requestErr *directus.RequestError
	if errors.As(err, &requestErr) {
		if strings.Contains(requestErr.Path, "/versions") {
			utils.Logger().Error("Directus version request failed",
				"path", requestErr.Path,
				"status", requestErr.Status,
				"body", truncateString(requestErr.Body, 512))
			return err
		}
	}
	return err
}

func truncateString(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}
	return value[:limit-3] + "..."
}

func (b *builder) collectionHasField(collection, field string) bool {
	collection = strings.TrimSpace(collection)
	field = strings.TrimSpace(field)
	if collection == "" || field == "" {
		return false
	}
	if cache, ok := b.fieldExistsCache[collection]; ok {
		if exists, ok := cache[field]; ok {
			return exists
		}
	}
	exists, err := b.client.FieldExists(b.ctx, collection, field)
	if err != nil {
		return false
	}
	if _, ok := b.fieldExistsCache[collection]; !ok {
		b.fieldExistsCache[collection] = map[string]bool{}
	}
	b.fieldExistsCache[collection][field] = exists
	return exists
}

func (b *builder) importFile(url string) (string, error) {
	if b.skipMediaUpload {
		return "", nil
	}
	if strings.TrimSpace(url) == "" {
		return "", nil
	}
	if id, ok := b.fileImportCache[url]; ok {
		return id, nil
	}
	folder := strings.TrimSpace(os.Getenv("DIRECTUS_FILES_FOLDER"))
	data := map[string]any{}
	if folder != "" {
		data["folder"] = folder
	}
	if description := rsiMediaDescription(url); description != "" {
		data["description"] = description
	}
	id, err := b.client.ImportFile(b.ctx, url, data)
	if err != nil {
		return "", err
	}
	b.fileImportCache[url] = id
	return id, nil
}

func (b *builder) loadHullMediaState(hullID string) hullMediaState {
	if state, ok := b.hullMediaCache[hullID]; ok && state.Loaded {
		return state
	}
	collection := b.collections.Ships
	state := hullMediaState{Loaded: true}
	if hullID == "" {
		b.hullMediaCache[hullID] = state
		return state
	}
	fields := []string{"id"}
	wantStore := b.collectionHasField(collection, "store_image")
	wantGallery := b.collectionHasField(collection, "gallery")
	if wantStore {
		fields = append(fields, "store_image", "store_image.id", "store_image.description")
	}
	if wantGallery {
		fields = append(fields, "gallery", "gallery.id", "gallery.description")
	}
	if len(fields) == 1 {
		b.hullMediaCache[hullID] = state
		return state
	}
	rows, err := fetchAllRows(b.ctx, b.client, collection, fields, map[string]any{"id": map[string]any{"_eq": hullID}})
	if err != nil || len(rows) == 0 {
		b.hullMediaCache[hullID] = state
		return state
	}
	row := rows[0]
	if wantStore {
		state.Store = extractMediaFileState(row["store_image"])
	}
	if wantGallery {
		state.Gallery = extractMediaFileStates(row["gallery"])
	}
	b.hullMediaCache[hullID] = state
	return state
}

func (b *builder) maybeAttachHullMedia(hullID string, media rsiMedia) {
	if hullID == "" {
		return
	}
	collection := b.collections.Ships
	state := b.loadHullMediaState(hullID)
	payload := map[string]any{}
	updated := false

	if strings.TrimSpace(media.Store) != "" && b.collectionHasField(collection, "store_image") && !mediaFileMatchesURL(state.Store, media.Store) {
		if fileID, err := b.importFile(media.Store); err != nil {
			utils.Logger().Warn("Failed to import RSI store image", "hull", hullID, "url", media.Store, "error", err)
		} else if fileID != "" {
			payload["store_image"] = fileID
			state.Store = mediaFileState{
				ID:          fileID,
				Description: rsiMediaDescription(media.Store),
			}
			updated = true
		}
	}

	if len(media.Gallery) > 0 && b.collectionHasField(collection, "gallery") && !mediaGalleryMatchesURLs(state.Gallery, media.Gallery) {
		fileIDs := []string{}
		nextGallery := make([]mediaFileState, 0, len(media.Gallery))
		for _, url := range media.Gallery {
			if fileID, err := b.importFile(url); err != nil {
				utils.Logger().Warn("Failed to import RSI gallery image", "hull", hullID, "url", url, "error", err)
				continue
			} else if fileID != "" {
				fileIDs = append(fileIDs, fileID)
				nextGallery = append(nextGallery, mediaFileState{
					ID:          fileID,
					Description: rsiMediaDescription(url),
				})
			}
		}
		if len(fileIDs) > 0 {
			payload["gallery"] = fileIDs
			state.Gallery = nextGallery
			updated = true
		}
	}

	if updated {
		if _, err := b.client.UpdateOne(b.ctx, collection, hullID, payload); err != nil {
			utils.Logger().Warn("Failed to update hull media", "hull", hullID, "error", err)
		} else {
			state.Loaded = true
			b.hullMediaCache[hullID] = state
		}
	}
}

func extractMediaFileState(value any) mediaFileState {
	switch v := value.(type) {
	case map[string]any:
		return mediaFileState{
			ID:          extractID(v),
			Description: normalizeString(v["description"]),
		}
	case string:
		return mediaFileState{ID: normalizeString(v)}
	default:
		return mediaFileState{}
	}
}

func extractMediaFileStates(value any) []mediaFileState {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]mediaFileState, 0, len(items))
	for _, item := range items {
		state := extractMediaFileState(item)
		if state.ID == "" && state.Description == "" {
			continue
		}
		result = append(result, state)
	}
	return result
}

func mediaFileMatchesURL(state mediaFileState, url string) bool {
	if strings.TrimSpace(state.Description) == "" {
		return false
	}
	return state.Description == rsiMediaDescription(url)
}

func mediaGalleryMatchesURLs(existing []mediaFileState, desired []string) bool {
	if len(existing) != len(desired) {
		return false
	}
	for i, url := range desired {
		if !mediaFileMatchesURL(existing[i], url) {
			return false
		}
	}
	return true
}

func (b *builder) filterHardpointsList(hardpoints []model.NormalizedHardpointV2) ([]model.NormalizedHardpointV2, int) {
	if len(b.allowedHardpointCategories) == 0 {
		copied := make([]model.NormalizedHardpointV2, len(hardpoints))
		copy(copied, hardpoints)
		return copied, 0
	}
	filtered := make([]model.NormalizedHardpointV2, 0, len(hardpoints))
	skipped := 0
	for _, hp := range hardpoints {
		categoryUpper := strings.ToUpper(strings.TrimSpace(hp.Category))
		if categoryUpper == "" {
			skipped++
			continue
		}
		if _, ok := b.allowedHardpointCategories[categoryUpper]; !ok {
			skipped++
			continue
		}
		filtered = append(filtered, hp)
	}
	return filtered, skipped
}

func buildInstalledItemMap(installed []model.NormalizedInstalledItem) map[string]installedItemAggregate {
	result := map[string]installedItemAggregate{}
	for _, inst := range installed {
		if inst.HardpointExternalID == nil {
			continue
		}
		key := strings.ToUpper(*inst.HardpointExternalID)
		entry := result[key]
		if entry.ItemExternalID == "" {
			entry.ItemExternalID = strings.ToUpper(inst.ItemExternalID)
		}
		entry.Quantity += inst.Quantity
		result[key] = entry
	}
	return result
}

func (b *builder) ensureVersionSnapshot(collection, id string, payload map[string]any, versionName string, promote bool) error {
	return b.client.CreateItemVersion(b.ctx, collection, id, payload, directus.VersionOptions{
		Name:    versionName,
		Key:     versionName,
		Promote: promote,
	})
}

func (b *builder) ensureRSIMedia() {
	if b.rsiMedia != nil {
		return
	}
	media, err := loadRSIMedia(b.ctx)
	if err != nil {
		utils.Logger().Warn("Failed to load RSI media", "error", err)
		b.rsiMedia = map[string]rsiMedia{}
		return
	}
	b.rsiMedia = media
}

type installedItemAggregate struct {
	ItemExternalID string
	Quantity       int
}

func readBuildMetadata(normalizedDir string) (buildMetadata, error) {
	path := filepath.Join(normalizedDir, "build.json")
	data := map[string]any{}
	if err := utils.ReadJSONOrDefault(path, map[string]any{}, &data); err != nil {
		return buildMetadata{}, err
	}
	metadata := buildMetadata{}
	if value, ok := data["build_hash"].(string); ok {
		metadata.Hash = value
	}
	if value, ok := data["released_at"].(string); ok {
		metadata.ReleasedAt = value
	}
	if value, ok := data["status"].(string); ok {
		metadata.Status = value
	}
	return metadata, nil
}
