package load

import (
	"context"
	"fmt"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
	"github.com/ArisCorporation/sc-goetl/internal/transform"
)

// Result captures statistics about persisted content.
type Result struct {
	BuildID string
	Stats   LoadStatistics
}

// LoadStatistics mirrors the metrics emitted from the legacy pipeline.
type LoadStatistics struct {
	Companies    int
	Ships        int
	ShipVariants int
	Items        int
	Hardpoints   int
}

// Options configure the loader.
type Options struct {
	Client *directus.Client
}

// Run writes the normalized bundle to Directus.
func Run(ctx context.Context, result *transform.Result, opts Options) (*Result, error) {
	if result == nil {
		return nil, fmt.Errorf("transform result missing")
	}
	client := opts.Client
	if client == nil {
		var err error
		client, err = directus.NewFromEnv()
		if err != nil {
			return nil, err
		}
	}

	builder := newBuilder(ctx, client, result)

	if err := builder.ensureBuild(); err != nil {
		return nil, err
	}
	if err := builder.sync(); err != nil {
		return nil, err
	}

	return &Result{
		BuildID: builder.build.ID,
		Stats:   builder.stats(),
	}, nil
}
