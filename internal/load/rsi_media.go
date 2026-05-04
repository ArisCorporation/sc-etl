package load

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// rsiMedia bundles useful image URLs extracted from the RSI ship matrix.
type rsiMedia struct {
	Thumbnail string
	Store     string
	Gallery   []string
}

type rsiMatrixMedia struct {
	MembershipSlot string            `json:"membership.slot"`
	Purpose        string            `json:"purpose"`
	SourceURL      string            `json:"source_url"`
	SourceStream   rsiSourceStream   `json:"source_stream"`
	DerivedData    rsiDerivedData    `json:"derived_data"`
	Images         map[string]string `json:"images"`
}

type rsiSourceStream struct {
	Progressive string `json:"progressive"`
}

type rsiDerivedData struct {
	Sizes map[string]rsiDerivedSize `json:"sizes"`
}

type rsiDerivedSize struct {
	Width  any    `json:"width"`
	Height any    `json:"height"`
	Mode   string `json:"mode"`
}

type rsiMatrixEntry struct {
	ID    int              `json:"id"`
	Name  string           `json:"name"`
	URL   string           `json:"url"`
	Media []rsiMatrixMedia `json:"media"`
}

type rsiMatrixPayload struct {
	Data []rsiMatrixEntry `json:"data"`
}

func loadRSIMedia(ctx context.Context) (map[string]rsiMedia, error) {
	rows, err := fetchRSIMatrix(ctx)
	if err != nil {
		return nil, err
	}
	result := map[string]rsiMedia{}
	for _, row := range rows {
		if row.ID <= 0 || len(row.Media) == 0 {
			continue
		}
		media := pickMedia(row.Media)
		if media == nil {
			continue
		}
		result[fmt.Sprintf("%d", row.ID)] = *media
	}
	return result, nil
}

func pickMedia(entries []rsiMatrixMedia) *rsiMedia {
	if len(entries) == 0 {
		return nil
	}

	thumb := pickPreferredMediaURL(entries)
	store := thumb
	gallery := []string{}
	for _, entry := range entries {
		if url := bestRSIMediaURL(entry); url != "" {
			gallery = appendUnique(gallery, url)
		}
	}

	if thumb == "" && store == "" && len(gallery) == 0 {
		return nil
	}
	return &rsiMedia{
		Thumbnail: thumb,
		Store:     store,
		Gallery:   gallery,
	}
}

func pickPreferredMediaURL(entries []rsiMatrixMedia) string {
	for _, entry := range entries {
		if strings.EqualFold(strings.TrimSpace(entry.MembershipSlot), "thumbnail") {
			if url := bestRSIMediaURL(entry); url != "" {
				return url
			}
		}
	}
	for _, entry := range entries {
		if url := bestRSIMediaURL(entry); url != "" {
			return url
		}
	}
	return ""
}

func bestRSIMediaURL(entry rsiMatrixMedia) string {
	if largest := largestDerivedImageURL(entry); largest != "" {
		return largest
	}
	for _, candidate := range []string{
		entry.SourceStream.Progressive,
		entry.SourceURL,
	} {
		if normalized := normalizeRSIURL(candidate); normalized != "" {
			return normalized
		}
	}
	return ""
}

func largestDerivedImageURL(entry rsiMatrixMedia) string {
	bestURL := ""
	bestArea := -1
	bestWidth := -1
	bestHeight := -1

	for key, size := range entry.DerivedData.Sizes {
		url := normalizeRSIURL(entry.Images[key])
		if url == "" {
			continue
		}

		widthPtr, _ := extractInt(size.Width)
		heightPtr, _ := extractInt(size.Height)
		width := 0
		height := 0
		if widthPtr != nil {
			width = *widthPtr
		}
		if heightPtr != nil {
			height = *heightPtr
		}
		if width <= 0 && height <= 0 {
			continue
		}

		scoreHeight := height
		if scoreHeight <= 0 {
			scoreHeight = 1
		}
		area := width * scoreHeight

		if area > bestArea || (area == bestArea && (width > bestWidth || (width == bestWidth && height > bestHeight))) {
			bestURL = url
			bestArea = area
			bestWidth = width
			bestHeight = height
		}
	}

	return bestURL
}

func normalizeRSIURL(u string) string {
	u = strings.TrimSpace(u)
	if u == "" {
		return ""
	}
	if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		return u
	}
	if strings.HasPrefix(u, "//") {
		return "https:" + u
	}
	if strings.HasPrefix(u, "/") {
		return "https://robertsspaceindustries.com" + u
	}
	return u
}

func rsiMediaDescription(u string) string {
	normalized := normalizeRSIURL(u)
	if normalized == "" {
		return ""
	}
	return "RSI matrix source: " + normalized
}

func appendUnique(list []string, value string) []string {
	for _, existing := range list {
		if existing == value {
			return list
		}
	}
	return append(list, value)
}

func fetchRSIMatrix(ctx context.Context) ([]rsiMatrixEntry, error) {
	if rows, err := fetchRSIMatrixRemote(ctx); err == nil && len(rows) > 0 {
		return rows, nil
	}
	bytes, err := os.ReadFile("matrix.json")
	if err != nil {
		return nil, err
	}
	var payload rsiMatrixPayload
	if err := json.Unmarshal(bytes, &payload); err != nil {
		return nil, err
	}
	return payload.Data, nil
}

func fetchRSIMatrixRemote(ctx context.Context) ([]rsiMatrixEntry, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://robertsspaceindustries.com/ship-matrix/index", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}
	var payload rsiMatrixPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Data, nil
}
