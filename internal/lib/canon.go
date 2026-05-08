package lib

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	variantTokens = []string{
		"1",
		"1T",
		"2",
		"25",
		"3",
		"A",
		"A1",
		"A2",
		"AA",
		"ALPHA",
		"ANDROMEDA",
		"ANTARES",
		"AQUILA",
		"ARCHIMEDES",
		"ARGOS",
		"ATLS",
		"BETA",
		"BIS2950",
		"BIS2951",
		"BLACK",
		"BLADE",
		"BLUE",
		"C",
		"C1",
		"C2",
		"CARBON",
		"CARGO",
		"CITIZENCON2018",
		"CIVILIAN",
		"CL",
		"COMET",
		"COMPETITION",
		"CROCODILE",
		"DELTA",
		"DS",
		"DUNESTALKER",
		"DUNLEVY",
		"DUR",
		"ECLIPSE",
		"EMERALD",
		"ES",
		"EX",
		"EXEC",
		"EXECUTIVE",
		"EXPEDITION",
		"F7C",
		"F7CM",
		"F7CR",
		"F7CS",
		"F8",
		"F8C",
		"FIREBIRD",
		"FORCE",
		"FORTUNE",
		"FREELANCER",
		"FURY",
		"GAMMA",
		"GEMINI",
		"GEO",
		"GLADIUS",
		"GLAIVE",
		"GRAD01",
		"GRAD02",
		"GRAD03",
		"GUARDIAN",
		"HAMMERHEAD",
		"HARBINGER",
		"HEARTSEEKER",
		"HOPLITE",
		"IKTI",
		"INDUST",
		"INDUSTRIAL",
		"INFERNO",
		"ION",
		"JAVELIN",
		"KUE",
		"LN",
		"LX",
		"M",
		"M2",
		"MAKO",
		"MAX",
		"MEDIC",
		"MEDIVAC",
		"MERLIN",
		"MILITARY",
		"MILT",
		"MIRU",
		"MK1",
		"MK2",
		"MOD",
		"MR",
		"MT",
		"MX",
		"NOX",
		"OMEGA",
		"P",
		"PEREGRINE",
		"PHOENIX",
		"PINK",
		"PIR",
		"PIRATE",
		"PISCES",
		"PLAT",
		"PROSPECTOR",
		"PULSE",
		"QI",
		"RAMBLER",
		"RAVEN",
		"RAZOR",
		"RC",
		"RECLAIMER",
		"RED",
		"REDEEMER",
		"RELIANT",
		"RENEGADE",
		"RETALIATOR",
		"RN",
		"ROVER",
		"RUNNER",
		"SABRE",
		"SCOUT",
		"SCYTHE",
		"SEN",
		"SENTINEL",
		"SHOWDOWN",
		"SHRIKE",
		"SNOWBLIND",
		"STALKER",
		"STARFARER",
		"STEALTH",
		"STEALTHINDUSTRIAL",
		"STEEL",
		"SYULEN",
		"TAC",
		"TALUS",
		"TANA",
		"TAURUS",
		"TITAN",
		"TOURING",
		"TR",
		"TRANSPORT",
		"TRIAGE",
		"UTILITY",
		"VALIANT",
		"VANGUARD",
		"VELOCITY",
		"WARLOCK",
		"WILDFIRE",
		"WOLF",
		"YELLOW",
	}

	variantTokenSet = func() map[string]struct{} {
		set := make(map[string]struct{}, len(variantTokens))
		for _, token := range variantTokens {
			set[token] = struct{}{}
		}
		return set
	}()

	liveryRegex     = regexp.MustCompile(`\b(LIVERY|PAINT)\b`)
	tokenSplitRegex = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	nonAlphaNumeric = regexp.MustCompile(`[^A-Z0-9]+`)
	multiUnderscore = regexp.MustCompile(`_+`)
)

var (
	editionKeywords     []string
	editionKeywordSet   map[string]struct{}
	editionVariantCodes map[string]struct{}
	editionRegex        *regexp.Regexp
)

func init() {
	editionKeywords = loadStringSlice("edition-keywords.json")
	editionKeywordSet = make(map[string]struct{}, len(editionKeywords))
	for _, keyword := range editionKeywords {
		editionKeywordSet[strings.ToUpper(keyword)] = struct{}{}
	}
	editionVariantCodes = make(map[string]struct{})
	for _, code := range loadStringSlice("edition-variant-codes.json") {
		editionVariantCodes[sanitizeToken(code)] = struct{}{}
	}
	if len(editionKeywords) > 0 {
		patternParts := make([]string, 0, len(editionKeywords))
		for _, keyword := range editionKeywords {
			patternParts = append(patternParts, regexp.QuoteMeta(keyword))
		}
		pattern := `\b(` + strings.Join(patternParts, "|") + `)\b`
		editionRegex = regexp.MustCompile(pattern)
	}
}

func loadStringSlice(filename string) []string {
	path := filepath.Join("schemas", filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var result []string
	if err := json.Unmarshal(data, &result); err != nil {
		return nil
	}
	return result
}

// CanonicalVariantCode mirrors the TypeScript alias.
type CanonicalVariantCode = string

// PartitionVariantSuffix splits tokens into base and suffix tokens.
func PartitionVariantSuffix(tokens []string) (base []string, suffix []string) {
	if len(tokens) == 0 {
		return []string{}, []string{}
	}
	base = append([]string{}, tokens...)
	for len(base) > 1 {
		candidate := base[len(base)-1]
		if candidate == "" {
			break
		}
		if _, ok := variantTokenSet[candidate]; !ok {
			break
		}
		suffix = append([]string{candidate}, suffix...)
		base = base[:len(base)-1]
	}
	if len(base) == 0 && len(suffix) > 0 {
		base = append(base, suffix...)
		suffix = suffix[:0]
	}
	return base, suffix
}

func sanitizeToken(value string) string {
	value = strings.ToUpper(value)
	value = nonAlphaNumeric.ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	value = multiUnderscore.ReplaceAllString(value, "_")
	return value
}

func tokenize(input string) []string {
	if input == "" {
		return nil
	}
	parts := tokenSplitRegex.Split(input, -1)
	var tokens []string
	for _, part := range parts {
		if part == "" {
			continue
		}
		tokens = append(tokens, part)
	}
	return tokens
}

func titleCase(input string) string {
	if input == "" {
		return ""
	}
	parts := strings.Fields(input)
	for i, part := range parts {
		if part == "" {
			continue
		}
		// Preserve all-uppercase words (acronyms/abbreviations like "MK", "II")
		if len(part) > 1 && part == strings.ToUpper(part) {
			continue
		}
		lower := strings.ToLower(part)
		parts[i] = strings.ToUpper(lower[:1]) + lower[1:]
	}
	return strings.Join(parts, " ")
}

// BuildHullKey constructs a canonical hull key.
func BuildHullKey(manufacturer, family string) string {
	manufacturerToken := "UNKNOWN"
	if manufacturer != "" {
		manufacturerToken = sanitizeToken(manufacturer)
	}
	familyToken := "HULL"
	if family != "" {
		familyToken = sanitizeToken(family)
	}
	return manufacturerToken + "_" + familyToken
}

// ExtractVariantCode derives a variant code from the provided string.
func ExtractVariantCode(source string) CanonicalVariantCode {
	if strings.TrimSpace(source) == "" {
		return "BASE"
	}
	var bestKnown string
	var fallback string
	for _, token := range tokenize(source) {
		normalized := sanitizeToken(token)
		if normalized == "" {
			continue
		}
		if normalized == "BASE" {
			return "BASE"
		}
		if fallback == "" {
			fallback = normalized
		}
		if _, ok := variantTokenSet[normalized]; ok {
			if bestKnown == "" || len(normalized) > len(bestKnown) || (len(normalized) == len(bestKnown) && normalized < bestKnown) {
				bestKnown = normalized
			}
		}
	}
	if bestKnown != "" {
		return bestKnown
	}
	if fallback != "" {
		return fallback
	}
	return "BASE"
}

// EditionDetection mirrors the TypeScript return shape.
type EditionDetection struct {
	EditionCode string
	Livery      *string
}

// DetectEditionOrLivery identifies edition tokens and potential livery names.
func DetectEditionOrLivery(name string, forced CanonicalVariantCode) EditionDetection {
	if strings.TrimSpace(name) == "" {
		return EditionDetection{}
	}
	var detected []string
	for _, token := range tokenize(name) {
		upper := strings.ToUpper(token)
		if _, ok := editionKeywordSet[upper]; ok {
			detected = append(detected, upper)
		}
	}
	var editionCode string
	if len(detected) > 0 {
		priority := []string{"IAE", "INVICTUS"}
		sort.Slice(detected, func(i, j int) bool {
			score := func(value string) int {
				for idx, target := range priority {
					if value == target {
						return idx
					}
				}
				return len(priority)
			}
			aScore := score(detected[i])
			bScore := score(detected[j])
			if aScore != bScore {
				return aScore < bScore
			}
			return detected[i] < detected[j]
		})
		editionCode = sanitizeToken(strings.Join(detected, "_"))
	}
	if forced != "" {
		normalized := sanitizeToken(forced)
		if normalized != "" && normalized != "BASE" {
			editionCode = normalized
		}
	}
	var livery *string
	loc := liveryRegex.FindStringIndex(name)
	if loc != nil {
		tail := strings.TrimSpace(name[loc[1]:])
		if tail != "" {
			parts := tokenize(tail)
			if len(parts) > 3 {
				parts = parts[:3]
			}
			summary := strings.Join(parts, " ")
			if summary != "" {
				value := titleCase(summary)
				livery = &value
			}
		}
	}
	return EditionDetection{
		EditionCode: editionCode,
		Livery:      livery,
	}
}

// IsEditionVariantCode reports true if the code represents an edition.
func IsEditionVariantCode(code CanonicalVariantCode) bool {
	if code == "" {
		return false
	}
	_, ok := editionVariantCodes[sanitizeToken(code)]
	return ok
}

// IsEditionOnly mirrors the TS helper.
func IsEditionOnly(name string, variantCode CanonicalVariantCode) bool {
	if strings.TrimSpace(name) == "" {
		return false
	}
	normalizedCode := sanitizeToken(variantCode)
	if normalizedCode == "" {
		normalizedCode = sanitizeToken(ExtractVariantCode(name))
	}
	if IsEditionVariantCode(normalizedCode) {
		return true
	}
	if normalizedCode != "BASE" {
		return false
	}
	if editionRegex == nil {
		return false
	}
	return editionRegex.MatchString(name)
}

// ToCanonicalVariantExtID constructs the variant external id.
func ToCanonicalVariantExtID(hullKey string, variantCode CanonicalVariantCode) string {
	return sanitizeToken(hullKey) + "_" + sanitizeToken(variantCode)
}

// CleanFamilyName removes manufacturer/variant tokens.
func CleanFamilyName(input string, variantCode CanonicalVariantCode, manufacturer string) string {
	variantToken := sanitizeToken(variantCode)
	manufacturerTokens := map[string]struct{}{}
	if manufacturer != "" {
		normalized := sanitizeToken(manufacturer)
		if normalized != "" {
			manufacturerTokens[normalized] = struct{}{}
			if strings.HasSuffix(normalized, "S") {
				manufacturerTokens[normalized[:len(normalized)-1]] = struct{}{}
			}
		}
		for _, token := range tokenize(manufacturer) {
			manufacturerTokens[sanitizeToken(token)] = struct{}{}
		}
	}

	filtered := []string{}
	for _, token := range tokenize(input) {
		upper := sanitizeToken(token)
		if upper == "" {
			continue
		}
		if upper == variantToken {
			continue
		}
		if upper != "BASE" {
			if _, ok := variantTokenSet[upper]; ok {
				continue
			}
		}
		if _, ok := editionKeywordSet[upper]; ok {
			continue
		}
		excluded := false
		if _, ok := manufacturerTokens[upper]; ok {
			continue
		}
		for candidate := range manufacturerTokens {
			if candidate != "" && (strings.HasPrefix(upper, candidate) || strings.HasPrefix(candidate, upper)) {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		filtered = append(filtered, sanitizeToken(token))
	}
	if len(filtered) == 0 {
		return sanitizeToken(input)
	}
	return strings.Join(filtered, "_")
}

// CanonicalVariantName formats the variant display string.
func CanonicalVariantName(baseName string, variantCode CanonicalVariantCode) string {
	if sanitizeToken(variantCode) == "BASE" || variantCode == "" {
		return titleCase(baseName)
	}
	return titleCase(baseName) + " " + titleCase(string(variantCode))
}
