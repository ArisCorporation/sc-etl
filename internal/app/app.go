package app

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/ArisCorporation/sc-goetl/internal/config"
	"github.com/ArisCorporation/sc-goetl/internal/directus"
	"github.com/ArisCorporation/sc-goetl/internal/extract"
	"github.com/ArisCorporation/sc-goetl/internal/load"
	"github.com/ArisCorporation/sc-goetl/internal/model"
	"github.com/ArisCorporation/sc-goetl/internal/transform"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
	"github.com/ArisCorporation/sc-goetl/internal/validate"
)

// Run executes the ETL pipeline end-to-end.
func Run(ctx context.Context, cfg *config.Config) error {
	start := time.Now()
	utils.Info("ETL started", "channel", cfg.Channel, "version", cfg.Version, "data_root", cfg.DataRoot)

	extractResult, err := extract.Run(ctx, cfg)
	if err != nil {
		return fmt.Errorf("extract phase failed: %w", err)
	}

	utils.Info("Extraction completed",
		"raw_dir", extractResult.RawDir,
		"normalized_dir", extractResult.NormalizedDir,
		"files", len(extractResult.Discovered),
	)

	transformResult, err := transform.Run(ctx, cfg.DataRoot, model.Channel(cfg.Channel), cfg.Version)
	if err != nil {
		return fmt.Errorf("transform phase failed: %w", err)
	}

	schemaDir := filepath.Join(".", "schemas")
	if err := validate.Bundle(ctx, transformResult.Legacy, schemaDir); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	var loadResult *load.Result
	if !cfg.LoadEnabled {
		utils.Info("Load skipped", "reason", "disabled via configuration")
	} else if cfg.DirectusURL == "" || cfg.DirectusToken == "" {
		utils.Info("Load skipped", "reason", "missing Directus credentials")
	} else {
		client, err := directus.NewClient(cfg.DirectusURL, cfg.DirectusToken)
		if err != nil {
			return fmt.Errorf("initialize Directus client: %w", err)
		}
		loadResult, err = load.Run(ctx, transformResult, load.Options{
			Client:                     client,
			DefaultCompanyCategory:     cfg.DefaultCompanyCategory,
			AllowedItemTypes:           selectAllowedList(transformResult.Config.AllowedItemTypes),
			AllowedHardpointCategories: selectAllowedList(transformResult.Config.AllowedHardpointTypes),
			PromoteVersions:            &cfg.PromoteVersions,
		})
		if err != nil {
			return fmt.Errorf("load phase failed: %w", err)
		}
	}

	if loadResult != nil {
		utils.Info("Load finished", "build_id", loadResult.BuildID, "stats", loadResult.Stats)
	}
	utils.Info("ETL completed", "elapsed_ms", time.Since(start).Milliseconds())
	return nil
}

func selectAllowedList(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	list := make([]string, 0, len(set))
	for value := range set {
		list = append(list, value)
	}
	sort.Strings(list)
	return list
}
