package directus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// Client wraps the Directus REST API.
type Client struct {
	BaseURL    *url.URL
	Token      string
	HTTPClient *http.Client
}

// NewClient constructs a Client from explicit parameters.
func NewClient(baseURL, token string) (*Client, error) {
	if baseURL == "" {
		return nil, errors.New("directus base URL missing")
	}
	if token == "" {
		return nil, errors.New("directus token missing")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse directus url: %w", err)
	}
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
	}
	return &Client{
		BaseURL:    parsed,
		Token:      token,
		HTTPClient: httpClient,
	}, nil
}

// NewFromEnv initialises a Client using DIRECTUS_URL and DIRECTUS_TOKEN.
func NewFromEnv() (*Client, error) {
	url := strings.TrimSpace(os.Getenv("DIRECTUS_URL"))
	token := strings.TrimSpace(os.Getenv("DIRECTUS_TOKEN"))
	return NewClient(url, token)
}

func (c *Client) cloneURL(segments ...string) *url.URL {
	clone := *c.BaseURL
	joined := path.Join(append([]string{clone.Path}, segments...)...)
	clone.Path = joined
	return &clone
}

func (c *Client) buildRequest(ctx context.Context, method string, segments []string, query url.Values, body any) (*http.Request, error) {
	endpoint := c.cloneURL(segments...)
	if query != nil && len(query) > 0 {
		endpoint.RawQuery = query.Encode()
	}
	var reader io.Reader
	if body != nil {
		buf := &bytes.Buffer{}
		encoder := json.NewEncoder(buf)
		if err := encoder.Encode(body); err != nil {
			return nil, err
		}
		reader = buf
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func (c *Client) do(req *http.Request, dest any) error {
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("directus %s %s failed: %s (body: %s)", req.Method, req.URL.Path, resp.Status, string(payload))
	}

	if dest == nil {
		return nil
	}

	return json.NewDecoder(resp.Body).Decode(dest)
}

// ReadByQuery mirrors the SDK readItems() helper.
func (c *Client) ReadByQuery(ctx context.Context, collection string, query map[string]any) ([]map[string]any, error) {
	values := encodeQueryParams(query)
	req, err := c.buildRequest(ctx, http.MethodGet, []string{"items", collection}, values, nil)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data []map[string]any `json:"data"`
	}
	if err := c.do(req, &result); err != nil {
		return nil, err
	}
	return result.Data, nil
}

// CreateMany inserts multiple records.
func (c *Client) CreateMany(ctx context.Context, collection string, items []map[string]any) ([]map[string]any, error) {
	if len(items) == 0 {
		return nil, nil
	}
	req, err := c.buildRequest(ctx, http.MethodPost, []string{"items", collection}, nil, items)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data []map[string]any `json:"data"`
	}
	if err := c.do(req, &result); err != nil {
		return nil, err
	}
	return result.Data, nil
}

// CreateOne inserts a single record.
func (c *Client) CreateOne(ctx context.Context, collection string, item map[string]any) (map[string]any, error) {
	req, err := c.buildRequest(ctx, http.MethodPost, []string{"items", collection}, nil, item)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data map[string]any `json:"data"`
	}
	if err := c.do(req, &result); err != nil {
		return nil, err
	}
	return result.Data, nil
}

// UpdateMany performs a batch update.
func (c *Client) UpdateMany(ctx context.Context, collection string, items []map[string]any) ([]map[string]any, error) {
	if len(items) == 0 {
		return nil, nil
	}
	req, err := c.buildRequest(ctx, http.MethodPatch, []string{"items", collection}, nil, items)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data []map[string]any `json:"data"`
	}
	if err := c.do(req, &result); err != nil {
		return nil, err
	}
	return result.Data, nil
}

// UpdateOne patches a single record.
func (c *Client) UpdateOne(ctx context.Context, collection string, key string, item map[string]any) (map[string]any, error) {
	req, err := c.buildRequest(ctx, http.MethodPatch, []string{"items", collection, key}, nil, item)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data map[string]any `json:"data"`
	}
	if err := c.do(req, &result); err != nil {
		return nil, err
	}
	return result.Data, nil
}

// DeleteMany removes multiple records by primary key.
func (c *Client) DeleteMany(ctx context.Context, collection string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	req, err := c.buildRequest(ctx, http.MethodDelete, []string{"items", collection}, nil, map[string]any{
		"keys": keys,
	})
	if err != nil {
		return err
	}
	return c.do(req, nil)
}

type VersionOptions struct {
	Name    string
	Status  string
	Key     string
	Promote bool
}

// CreateItemVersion mirrors the TypeScript version management flow.
func (c *Client) CreateItemVersion(ctx context.Context, collection string, key string, item map[string]any, opts VersionOptions) error {
	versionKey := opts.Key
	if versionKey == "" {
		versionKey = opts.Name
	}
	if versionKey == "" {
		return errors.New("version key missing")
	}

	payload := map[string]any{
		"collection": collection,
		"item":       key,
		"key":        versionKey,
		"name":       opts.Name,
	}
	if opts.Status != "" {
		payload["status"] = opts.Status
	}

	req, err := c.buildRequest(ctx, http.MethodPost, []string{"versions"}, nil, payload)
	if err != nil {
		return err
	}

	var response struct {
		Data map[string]any `json:"data"`
	}
	err = c.do(req, &response)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			versionID, lookupErr := c.findExistingVersion(ctx, collection, key, versionKey)
			if lookupErr != nil {
				return lookupErr
			}
			if versionID == "" {
				return nil
			}
			updatePayload := map[string]any{
				"name": opts.Name,
				"key":  versionKey,
			}
			if opts.Status != "" {
				updatePayload["status"] = opts.Status
			}
			updateReq, err := c.buildRequest(ctx, http.MethodPatch, []string{"versions", versionID}, nil, updatePayload)
			if err != nil {
				return err
			}
			if err := c.do(updateReq, nil); err != nil {
				return err
			}
			return c.saveVersion(ctx, versionID, item, opts.Promote)
		}
		return err
	}

	versionID, _ := response.Data["id"].(string)
	if versionID == "" {
		return errors.New("directus version response missing id")
	}
	return c.saveVersion(ctx, versionID, item, opts.Promote)
}

func (c *Client) findExistingVersion(ctx context.Context, collection, item, versionKey string) (string, error) {
	params := url.Values{
		"filter[collection][_eq]": []string{collection},
		"filter[item][_eq]":       []string{item},
		"filter[key][_eq]":        []string{versionKey},
		"limit":                   []string{"1"},
	}
	req, err := c.buildRequest(ctx, http.MethodGet, []string{"versions"}, params, nil)
	if err != nil {
		return "", err
	}
	var result struct {
		Data []map[string]any `json:"data"`
	}
	if err := c.do(req, &result); err != nil {
		return "", err
	}
	if len(result.Data) == 0 {
		return "", nil
	}
	id, _ := result.Data[0]["id"].(string)
	return id, nil
}

func (c *Client) saveVersion(ctx context.Context, versionID string, item map[string]any, promote bool) error {
	saveReq, err := c.buildRequest(ctx, http.MethodPost, []string{"versions", versionID, "save"}, nil, map[string]any{
		"data": item,
	})
	if err != nil {
		return err
	}

	var saveResp struct {
		Data map[string]any `json:"data"`
		Meta map[string]any `json:"meta"`
	}
	if err := c.do(saveReq, &saveResp); err != nil {
		return err
	}

	if !promote {
		return nil
	}

	mainHash := extractMainHash(saveResp.Data, saveResp.Meta)
	if mainHash == "" {
		hash, err := c.fetchVersionHash(ctx, versionID)
		if err != nil {
			return err
		}
		mainHash = hash
	}
	if mainHash == "" {
		return nil
	}
	promoteReq, err := c.buildRequest(ctx, http.MethodPost, []string{"versions", versionID, "promote"}, nil, map[string]any{
		"mainHash": mainHash,
	})
	if err != nil {
		return err
	}
	return c.do(promoteReq, nil)
}

// CreateOneWithVersion creates a record and persists a version snapshot.
func (c *Client) CreateOneWithVersion(ctx context.Context, collection string, item map[string]any, version string, promote bool) (map[string]any, error) {
	created, err := c.CreateOne(ctx, collection, item)
	if err != nil {
		return nil, err
	}
	id := normalizeID(created["id"])
	if id == "" {
		return created, errors.New("directus create response missing id")
	}
	if err := c.CreateItemVersion(ctx, collection, id, item, VersionOptions{
		Name:    version,
		Key:     version,
		Promote: promote,
	}); err != nil {
		return nil, err
	}
	return created, nil
}

// UpdateOneWithVersion updates a record and persists a version snapshot.
func (c *Client) UpdateOneWithVersion(ctx context.Context, collection string, key string, item map[string]any, version string, promote bool) (map[string]any, error) {
	updated, err := c.UpdateOne(ctx, collection, key, item)
	if err != nil {
		return nil, err
	}
	id := normalizeID(updated["id"])
	if id == "" {
		id = normalizeID(key)
	}
	if err := c.CreateItemVersion(ctx, collection, id, item, VersionOptions{
		Name:    version,
		Key:     version,
		Promote: promote,
	}); err != nil {
		return nil, err
	}
	return updated, nil
}

func normalizeID(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	case json.Number:
		return strings.TrimSpace(v.String())
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%.0f", v)
	case nil:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func encodeQueryParams(query map[string]any) url.Values {
	values := url.Values{}
	if query == nil {
		return values
	}
	for key, raw := range query {
		switch strings.ToLower(key) {
		case "filter":
			if filter, ok := raw.(map[string]any); ok {
				encodeFilter(values, "filter", filter)
			}
		case "fields":
			switch v := raw.(type) {
			case []string:
				values.Set("fields", strings.Join(v, ","))
			case []any:
				parts := make([]string, 0, len(v))
				for _, entry := range v {
					parts = append(parts, fmt.Sprint(entry))
				}
				values.Set("fields", strings.Join(parts, ","))
			case string:
				values.Set("fields", v)
			}
		case "limit":
			if num, ok := toInt(raw); ok {
				values.Set("limit", strconv.Itoa(num))
			}
		case "offset":
			if num, ok := toInt(raw); ok {
				values.Set("offset", strconv.Itoa(num))
			}
		case "sort":
			values.Set("sort", fmt.Sprint(raw))
		default:
			values.Set(key, fmt.Sprint(raw))
		}
	}
	return values
}

func encodeFilter(values url.Values, prefix string, data any) {
	switch v := data.(type) {
	case map[string]any:
		for key, child := range v {
			next := prefix + "[" + key + "]"
			encodeFilter(values, next, child)
		}
	case []any:
		for idx, child := range v {
			next := fmt.Sprintf("%s[%d]", prefix, idx)
			encodeFilter(values, next, child)
		}
	default:
		values.Add(prefix, fmt.Sprint(v))
	}
}

func toInt(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, false
		}
		return int(n), true
	case string:
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func extractMainHash(data map[string]any, meta map[string]any) string {
	candidates := []string{}
	if data != nil {
		if hash, ok := data["mainHash"].(string); ok && hash != "" {
			candidates = append(candidates, hash)
		}
		if hash, ok := data["main_hash"].(string); ok && hash != "" {
			candidates = append(candidates, hash)
		}
	}
	if meta != nil {
		if hash, ok := meta["mainHash"].(string); ok && hash != "" {
			candidates = append(candidates, hash)
		}
		if hash, ok := meta["main_hash"].(string); ok && hash != "" {
			candidates = append(candidates, hash)
		}
	}
	if len(candidates) > 0 {
		return candidates[0]
	}
	return ""
}

func (c *Client) fetchVersionHash(ctx context.Context, versionID string) (string, error) {
	req, err := c.buildRequest(ctx, http.MethodGet, []string{"versions", versionID, "compare"}, nil, nil)
	if err != nil {
		return "", err
	}
	var result struct {
		Data any `json:"data"`
	}
	if err := c.do(req, &result); err != nil {
		return "", err
	}
	switch data := result.Data.(type) {
	case []any:
		for _, raw := range data {
			entry, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if hash := extractMainHash(entry, nil); hash != "" {
				return hash, nil
			}
		}
	case map[string]any:
		if hash := extractMainHash(data, nil); hash != "" {
			return hash, nil
		}
	}
	return "", nil
}
