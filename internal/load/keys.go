package load

import "strings"

func shipCompositeKey(manufacturerID *string, name string) string {
	if manufacturerID == nil {
		return ""
	}
	if strings.TrimSpace(*manufacturerID) == "" || strings.TrimSpace(name) == "" {
		return ""
	}
	return strings.ToUpper(*manufacturerID) + ":" + strings.ToLower(strings.TrimSpace(name))
}

func variantCompositeKey(shipID string, variantCode string) string {
	shipID = strings.TrimSpace(strings.ToUpper(shipID))
	if shipID == "" {
		return ""
	}
	code := strings.TrimSpace(strings.ToUpper(variantCode))
	if code == "" {
		code = "BASE"
	}
	return shipID + ":" + code
}
