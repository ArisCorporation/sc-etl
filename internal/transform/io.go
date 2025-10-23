package transform

import (
	"path/filepath"

	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

func readJSONArray(path string) ([]map[string]any, error) {
	values, err := utils.ReadJSONArrayGeneric(path)
	if err != nil {
		return nil, err
	}
	return toMapArray(values), nil
}

func jsonPath(root string, parts ...string) string {
	segments := append([]string{root}, parts...)
	return filepath.Join(segments...)
}
