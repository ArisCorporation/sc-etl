package load

import (
	"context"
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
	ctx         context.Context
	client      *directus.Client
	result      *transform.Result
	collections collections
	build       buildRecord

	statsCompanies  int
	statsShips      int
	statsVariants   int
	statsItems      int
	statsHardpoints int
}

func newBuilder(ctx context.Context, client *directus.Client, result *transform.Result) *builder {
	return &builder{
		ctx:         ctx,
		client:      client,
		result:      result,
		collections: loadCollections(),
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
		Ships:               getenv("SC_SHIP_COLLECTION", "ships"),
		ShipVariants:        getenv("SC_SHIP_VARIANT_COLLECTION", "ship_variants"),
		Items:               getenv("SC_ITEM_COLLECTION", "items"),
		Hardpoints:          getenv("SC_HARDPOINT_COLLECTION", "hardpoints"),
		ShipConfigurations:  getenv("SC_SHIP_CONFIGURATION_COLLECTION", "ship_configurations"),
		ShipConfigurationHP: getenv("SC_SHIP_CONFIGURATION_HP_COLLECTION", "ship_configuration_hardpoints"),
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
	promoteVersions := b.result.V2.Channel == model.ChannelLive

	// Split stats and hardpoints
	statsByVariant, hardpoints := splitVariantStats(b.result.V2)

	legacyHardpoints := hardpoints
	if len(b.result.V2.Hardpoints) > 0 {
		legacyHardpoints = b.result.V2.Hardpoints
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

	shipIDs, err := b.syncShips(resolveCompanyID, b.result.V2.Ships, versionName, promoteVersions)
	if err != nil {
		return err
	}
	b.statsShips = len(shipIDs)

	variantIDs, err := b.syncShipVariants(b.result.V2.ShipVariants, statsByVariant, shipIDs, versionName, promoteVersions)
	if err != nil {
		return err
	}
	b.statsVariants = len(variantIDs)

	itemIDs, err := b.syncItems(resolveCompanyID, b.result.V2.Items, versionName, promoteVersions)
	if err != nil {
		return err
	}
	b.statsItems = len(itemIDs)

	if err := b.syncHardpoints(legacyHardpoints, variantIDs, installedByHardpoint, itemIDs, versionName, promoteVersions); err != nil {
		return err
	}
	b.statsHardpoints = len(legacyHardpoints)

	configs, err := b.syncShipConfigurations(shipGrouping, b.result.V2.ShipVariants, variantIDs)
	if err != nil {
		return err
	}

	hardpointIDMap, err := fetchHardpointIDMap(b.ctx, b.client, b.collections.Hardpoints)
	if err != nil {
		return err
	}

	if err := b.syncShipConfigurationHardpoints(configs, b.result.Legacy.InstalledItems, hardpointIDMap, itemIDs); err != nil {
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
