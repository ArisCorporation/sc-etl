package transform

import (
	"regexp"
	"sort"
	"strings"
)

var (
	rsiLookupSplitRegex = regexp.MustCompile(`[^A-Za-z0-9]+`)
	rsiCompactCodeRegex = regexp.MustCompile(`^([A-Z]*\d+[A-Z0-9]*)([A-Z]{1,2})$`)
	rsiRomanNumerals    = map[string]string{
		"I":    "1",
		"II":   "2",
		"III":  "3",
		"IV":   "4",
		"V":    "5",
		"VI":   "6",
		"VII":  "7",
		"VIII": "8",
		"IX":   "9",
		"X":    "10",
	}
)

type rsiMatrixMatcher struct {
	entry        rsiMatrixEntry
	manufacturer string
	exactKeys    map[string]struct{}
	tokens       map[string]struct{}
}

func buildRSIMatrixMatchers(rows []rsiMatrixEntry) []rsiMatrixMatcher {
	matchers := make([]rsiMatrixMatcher, 0, len(rows))
	for _, entry := range rows {
		if entry.ID <= 0 {
			continue
		}
		exactKeys := map[string]struct{}{}
		for _, key := range exactRSIEntryKeys(entry) {
			exactKeys[key] = struct{}{}
		}
		tokens := lookupTokenSet(entry.Name)
		if slug := matrixURLLeaf(entry.URL); slug != "" {
			mergeTokenSets(tokens, lookupTokenSet(slug))
		}
		matchers = append(matchers, rsiMatrixMatcher{
			entry:        entry,
			manufacturer: normalizeManufacturerCode(entry.Manufacturer.Code),
			exactKeys:    exactKeys,
			tokens:       tokens,
		})
	}
	return matchers
}

func exactRSIEntryKeys(entry rsiMatrixEntry) []string {
	keys := []string{}
	add := func(value string) {
		if key := normalizeLookupPhrase(value); key != "" {
			keys = append(keys, "phrase:"+key)
		}
		if key := normalizeLookupBag(value); key != "" {
			keys = append(keys, "bag:"+key)
		}
	}
	add(entry.Name)
	if entry.Manufacturer.Code != "" {
		add(entry.Manufacturer.Code + " " + entry.Name)
	}
	if slug := matrixURLLeaf(entry.URL); slug != "" {
		add(slug)
		if entry.Manufacturer.Code != "" {
			add(entry.Manufacturer.Code + " " + slug)
		}
	}
	return dedupeOrderedStrings(keys)
}

func matchRSIMatrixEntry(variant *variantBuilder, hulls map[string]*hullBuilder, matchers []rsiMatrixMatcher) (rsiMatrixEntry, bool) {
	if variant == nil || len(matchers) == 0 {
		return rsiMatrixEntry{}, false
	}
	manufacturer := variantManufacturerCode(variant, hulls)
	for _, candidate := range exactRSIVariantKeys(variant, hulls) {
		for _, matcher := range matchers {
			if !manufacturerCodesCompatible(manufacturer, matcher.manufacturer) {
				continue
			}
			if _, ok := matcher.exactKeys[candidate]; ok {
				return matcher.entry, true
			}
		}
	}

	required := variantRequiredRSITokens(variant, hulls)
	if len(required) == 0 {
		return rsiMatrixEntry{}, false
	}

	bestIdx := -1
	bestExtras := 0
	bestTokenCount := 0
	for idx, matcher := range matchers {
		if !manufacturerCodesCompatible(manufacturer, matcher.manufacturer) {
			continue
		}
		if !tokenSubset(required, matcher.tokens) {
			continue
		}
		extras := extraTokenCount(required, matcher.tokens)
		tokenCount := len(matcher.tokens)
		if bestIdx == -1 || extras < bestExtras || (extras == bestExtras && tokenCount < bestTokenCount) || (extras == bestExtras && tokenCount == bestTokenCount && matcher.entry.ID < matchers[bestIdx].entry.ID) {
			bestIdx = idx
			bestExtras = extras
			bestTokenCount = tokenCount
		}
	}
	if bestIdx >= 0 {
		return matchers[bestIdx].entry, true
	}
	return rsiMatrixEntry{}, false
}

func exactRSIVariantKeys(variant *variantBuilder, hulls map[string]*hullBuilder) []string {
	if variant == nil {
		return nil
	}
	keys := []string{}
	add := func(value string) {
		if key := normalizeLookupPhrase(value); key != "" {
			keys = append(keys, "phrase:"+key)
		}
		if key := normalizeLookupBag(value); key != "" {
			keys = append(keys, "bag:"+key)
		}
	}

	hullName := variantHullName(variant, hulls)
	manufacturer := variantManufacturerCode(variant, hulls)
	name := strings.TrimSpace(variant.Name)
	code := strings.TrimSpace(variant.VariantCode)
	codeValue := strings.ReplaceAll(code, "_", " ")

	if name != "" {
		add(name)
		if manufacturer != "" {
			add(manufacturer + " " + name)
		}
	}
	if codeValue != "" && !strings.EqualFold(codeValue, "BASE") {
		add(hullName + " " + codeValue)
		add(codeValue + " " + hullName)
		if manufacturer != "" {
			add(manufacturer + " " + hullName + " " + codeValue)
			add(manufacturer + " " + codeValue + " " + hullName)
		}
	}
	if variant.ExternalID != "" {
		add(variant.ExternalID)
	}
	add(hullName)
	if manufacturer != "" {
		add(manufacturer + " " + hullName)
	}
	return dedupeOrderedStrings(keys)
}

func variantRequiredRSITokens(variant *variantBuilder, hulls map[string]*hullBuilder) map[string]struct{} {
	set := map[string]struct{}{}
	hullName := variantHullName(variant, hulls)
	mergeTokenSets(set, lookupTokenSet(hullName))
	code := strings.TrimSpace(variant.VariantCode)
	if code != "" && !strings.EqualFold(code, "BASE") {
		mergeTokenSets(set, lookupTokenSet(code))
	}
	return set
}

func variantHullName(variant *variantBuilder, hulls map[string]*hullBuilder) string {
	if variant == nil {
		return ""
	}
	if hulls != nil {
		if hb := hulls[variant.HullKey]; hb != nil && strings.TrimSpace(hb.Name) != "" {
			return hb.Name
		}
	}
	return variant.HullKey
}

func variantManufacturerCode(variant *variantBuilder, hulls map[string]*hullBuilder) string {
	if variant == nil || hulls == nil {
		return ""
	}
	if hb := hulls[variant.HullKey]; hb != nil {
		return normalizeManufacturerCode(hb.CompanyCode)
	}
	return ""
}

func normalizeLookupPhrase(value string) string {
	parts := lookupParts(value)
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "")
}

func normalizeLookupBag(value string) string {
	parts := lookupParts(value)
	if len(parts) == 0 {
		return ""
	}
	sort.Strings(parts)
	return strings.Join(dedupeOrderedStrings(parts), "_")
}

func lookupTokenSet(values ...string) map[string]struct{} {
	set := map[string]struct{}{}
	for _, value := range values {
		parts := lookupParts(value)
		for _, part := range parts {
			set[part] = struct{}{}
			for _, expanded := range expandCompactLookupToken(part) {
				set[expanded] = struct{}{}
			}
		}
		for idx := 0; idx+1 < len(parts); idx++ {
			if combined := combineLookupParts(parts[idx], parts[idx+1]); combined != "" {
				set[combined] = struct{}{}
			}
		}
	}
	return set
}

func lookupParts(value string) []string {
	rawParts := rsiLookupSplitRegex.Split(strings.ToUpper(strings.TrimSpace(value)), -1)
	parts := make([]string, 0, len(rawParts))
	for idx := 0; idx < len(rawParts); idx++ {
		token := sanitizeLookupToken(rawParts[idx])
		if token == "" {
			continue
		}
		if token == "MK" && idx+1 < len(rawParts) {
			if normalized := normalizeMkNumber(rawParts[idx+1]); normalized != "" {
				parts = append(parts, "MK"+normalized)
				idx++
				continue
			}
		}
		parts = append(parts, normalizeStandaloneMkToken(token))
	}
	return parts
}

func sanitizeLookupToken(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	builder := strings.Builder{}
	for _, r := range value {
		switch {
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func normalizeStandaloneMkToken(token string) string {
	if !strings.HasPrefix(token, "MK") || len(token) <= 2 {
		return token
	}
	if normalized := normalizeMkNumber(token[2:]); normalized != "" {
		return "MK" + normalized
	}
	return token
}

func normalizeMkNumber(value string) string {
	value = sanitizeLookupToken(value)
	if value == "" {
		return ""
	}
	if mapped, ok := rsiRomanNumerals[value]; ok {
		return mapped
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return value
}

func expandCompactLookupToken(token string) []string {
	token = normalizeStandaloneMkToken(sanitizeLookupToken(token))
	if token == "" || strings.HasPrefix(token, "MK") {
		return nil
	}
	match := rsiCompactCodeRegex.FindStringSubmatch(token)
	if len(match) != 3 {
		return nil
	}
	prefix := sanitizeLookupToken(match[1])
	suffix := sanitizeLookupToken(match[2])
	if prefix == "" || suffix == "" {
		return nil
	}
	return []string{prefix, suffix}
}

func combineLookupParts(lhs, rhs string) string {
	lhs = sanitizeLookupToken(lhs)
	rhs = sanitizeLookupToken(rhs)
	if lhs == "" || rhs == "" {
		return ""
	}
	if strings.HasPrefix(lhs, "MK") || strings.HasPrefix(rhs, "MK") {
		return ""
	}
	if !(containsDigit(lhs) || containsDigit(rhs)) {
		return ""
	}
	if len(lhs) > 4 || len(rhs) > 2 {
		return ""
	}
	return lhs + rhs
}

func containsDigit(value string) bool {
	for _, r := range value {
		if r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}

func matrixURLLeaf(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parts := strings.Split(strings.Trim(value, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func mergeTokenSets(target, source map[string]struct{}) {
	for token := range source {
		target[token] = struct{}{}
	}
}

func tokenSubset(required, candidate map[string]struct{}) bool {
	for token := range required {
		if _, ok := candidate[token]; !ok {
			return false
		}
	}
	return true
}

func extraTokenCount(required, candidate map[string]struct{}) int {
	total := 0
	for token := range candidate {
		if _, ok := required[token]; !ok {
			total++
		}
	}
	return total
}

func normalizeManufacturerCode(value string) string {
	value = sanitizeLookupToken(value)
	if value == "" {
		return ""
	}
	return strings.TrimSuffix(value, "S")
}

func manufacturerCodesCompatible(lhs, rhs string) bool {
	if lhs == "" || rhs == "" {
		return true
	}
	if lhs == rhs {
		return true
	}
	return strings.HasPrefix(lhs, rhs) || strings.HasPrefix(rhs, lhs)
}

func dedupeOrderedStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	result := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
