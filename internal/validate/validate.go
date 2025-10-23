package validate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/ArisCorporation/sc-goetl/internal/model"
)

var (
	schemaCache sync.Map
)

// Bundle validates the normalized structures against JSON Schemas.
func Bundle(ctx context.Context, bundle model.NormalizedDataBundle, schemaDir string) error {
	tasks := []struct {
		File string
		Data any
	}{
		{File: "manufacturer.json", Data: bundle.Manufacturers},
		{File: "ship.json", Data: bundle.Ships},
		{File: "ship_variant.json", Data: bundle.ShipVariants},
		{File: "item.json", Data: bundle.Items},
		{File: "hardpoint.json", Data: bundle.Hardpoints},
		{File: "item_stats.json", Data: bundle.ItemStats},
		{File: "ship_stats.json", Data: bundle.ShipStats},
		{File: "installed_item.json", Data: bundle.InstalledItems},
		{File: "locale.json", Data: bundle.Locales},
	}

	for _, task := range tasks {
		path := filepath.Join(schemaDir, task.File)
		if err := validateArray(ctx, path, task.Data); err != nil {
			return fmt.Errorf("validate %s: %w", task.File, err)
		}
	}
	return nil
}

func validateArray(ctx context.Context, schemaPath string, data any) error {
	if _, err := os.Stat(schemaPath); err != nil {
		return err
	}
	schema, err := loadSchema(schemaPath)
	if err != nil {
		return err
	}
	generic, err := toInterface(data)
	if err != nil {
		return err
	}
	arr, ok := generic.([]any)
	if !ok {
		return fmt.Errorf("expected array input for %s", schemaPath)
	}
	for idx, item := range arr {
		if err := schema.Validate(item); err != nil {
			return fmt.Errorf("%s[%d]: %w", schemaPath, idx, err)
		}
	}
	return nil
}

func loadSchema(path string) (*jsonschema.Schema, error) {
	if value, ok := schemaCache.Load(path); ok {
		return value.(*jsonschema.Schema), nil
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)

	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc any
	if err := json.Unmarshal(bytes, &doc); err != nil {
		return nil, err
	}
	if err := compiler.AddResource(path, doc); err != nil {
		return nil, err
	}
	schema, err := compiler.Compile(path)
	if err != nil {
		return nil, err
	}
	schemaCache.Store(path, schema)
	return schema, nil
}

func toInterface(data any) (any, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var v any
	if err := json.Unmarshal(bytes, &v); err != nil {
		return nil, err
	}
	return v, nil
}
