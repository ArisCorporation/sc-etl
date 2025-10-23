package load

import (
	"context"

	"github.com/ArisCorporation/sc-goetl/internal/directus"
)

const pageLimit = 200

func fetchAllRows(ctx context.Context, client *directus.Client, collection string, fields []string, filter map[string]any) ([]map[string]any, error) {
	rows := []map[string]any{}
	offset := 0
	for {
		query := map[string]any{
			"fields": fields,
			"limit":  pageLimit,
			"offset": offset,
		}
		if filter != nil {
			query["filter"] = filter
		}
		batch, err := client.ReadByQuery(ctx, collection, query)
		if err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
		if len(batch) < pageLimit {
			break
		}
		offset += pageLimit
	}
	return rows, nil
}
