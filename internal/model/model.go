package model

// Channel represents the Star Citizen build stream being ingested.
type Channel string

const (
	ChannelLive Channel = "LIVE"
	ChannelPTU  Channel = "PTU"
	ChannelEPTU Channel = "EPTU"
)

type NormalizedManufacturer struct {
	ExternalID  string  `json:"external_id"`
	Code        string  `json:"code"`
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
	DataSource  *string `json:"data_source,omitempty"`
}

type NormalizedShip struct {
	ExternalID       string  `json:"external_id"`
	Name             string  `json:"name"`
	Class            string  `json:"class"`
	Size             *string `json:"size,omitempty"`
	ManufacturerCode string  `json:"manufacturer_code"`
	Description      *string `json:"description,omitempty"`
}

type NormalizedShipVariant struct {
	ExternalID     string  `json:"external_id"`
	ShipExternalID string  `json:"ship_external_id"`
	VariantCode    *string `json:"variant_code,omitempty"`
	Name           *string `json:"name,omitempty"`
	Thumbnail      *string `json:"thumbnail,omitempty"`
	Description    *string `json:"description,omitempty"`
}

type NormalizedHardpoint struct {
	ExternalID            string  `json:"external_id"`
	ShipVariantExternalID string  `json:"ship_variant_external_id"`
	Code                  string  `json:"code"`
	Category              string  `json:"category"`
	Position              *string `json:"position,omitempty"`
	Size                  *int    `json:"size,omitempty"`
	Gimballed             *bool   `json:"gimballed,omitempty"`
	Powered               *bool   `json:"powered,omitempty"`
	Seats                 *int    `json:"seats,omitempty"`
}

type NormalizedItem struct {
	ExternalID       string  `json:"external_id"`
	Type             string  `json:"type"`
	Subtype          *string `json:"subtype,omitempty"`
	Name             string  `json:"name"`
	ManufacturerCode *string `json:"manufacturer_code,omitempty"`
	Size             *int    `json:"size,omitempty"`
	Grade            *string `json:"grade,omitempty"`
	Class            *string `json:"class,omitempty"`
	Description      *string `json:"description,omitempty"`
}

type NormalizedItemStat struct {
	ItemExternalID string         `json:"item_external_id"`
	Stats          map[string]any `json:"stats"`
	PriceAUEC      *float64       `json:"price_auec,omitempty"`
	Availability   *string        `json:"availability,omitempty"`
}

type NormalizedShipStat struct {
	ShipVariantExternalID string         `json:"ship_variant_external_id"`
	Stats                 map[string]any `json:"stats"`
}

type NormalizedInstalledItem struct {
	ShipVariantExternalID string  `json:"ship_variant_external_id"`
	ItemExternalID        string  `json:"item_external_id"`
	Quantity              int     `json:"quantity"`
	HardpointExternalID   *string `json:"hardpoint_external_id,omitempty"`
	Profile               *string `json:"profile,omitempty"`
	Livery                *string `json:"livery,omitempty"`
}

type NormalizedLocaleEntry struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Lang      string `json:"lang"`
	Value     string `json:"value"`
}

type NormalizedDataBundle struct {
	Manufacturers  []NormalizedManufacturer  `json:"manufacturers"`
	Ships          []NormalizedShip          `json:"ships"`
	ShipVariants   []NormalizedShipVariant   `json:"ship_variants"`
	Items          []NormalizedItem          `json:"items"`
	Hardpoints     []NormalizedHardpoint     `json:"hardpoints"`
	ItemStats      []NormalizedItemStat      `json:"item_stats"`
	ShipStats      []NormalizedShipStat      `json:"ship_stats"`
	InstalledItems []NormalizedInstalledItem `json:"installed_items"`
	Locales        []NormalizedLocaleEntry   `json:"locales"`
}

type NormalizedExternalReference struct {
	Source string  `json:"source"`
	ID     string  `json:"id"`
	Note   *string `json:"note,omitempty"`
}

type NormalizedCompanyV2 struct {
	Code         string                        `json:"code"`
	Name         *string                       `json:"name,omitempty"`
	ExternalRefs []NormalizedExternalReference `json:"external_refs"`
}

type NormalizedShipV2 struct {
	ExternalID   string                        `json:"external_id"`
	Name         string                        `json:"name"`
	CompanyCode  string                        `json:"company_code"`
	ExternalRefs []NormalizedExternalReference `json:"external_refs"`
	Paints       []string                      `json:"paints,omitempty"`
}

type ShipPerformanceEnvelope struct {
	SCMSpeed         *float64           `json:"scm_speed,omitempty"`
	AfterburnerSpeed *float64           `json:"afterburner_speed,omitempty"`
	Accelerations    map[string]float64 `json:"accelerations,omitempty"`
	PitchRate        *float64           `json:"pitch_rate,omitempty"`
	YawRate          *float64           `json:"yaw_rate,omitempty"`
	RollRate         *float64           `json:"roll_rate,omitempty"`
}

type ShipCrewProfile struct {
	Minimum     *int `json:"minimum,omitempty"`
	Maximum     *int `json:"maximum,omitempty"`
	Recommended *int `json:"recommended,omitempty"`
}

type ShipPropulsionProfile struct {
	HydrogenCapacity  *float64 `json:"hydrogen_capacity,omitempty"`
	QuantumCapacity   *float64 `json:"quantum_capacity,omitempty"`
	MainThrusters     *int     `json:"main_thrusters,omitempty"`
	ManeuverThrusters *int     `json:"maneuver_thrusters,omitempty"`
	FuelIntakes       *int     `json:"fuel_intakes,omitempty"`
	FuelTanks         *int     `json:"fuel_tanks,omitempty"`
	PowerOutput       *float64 `json:"power_output,omitempty"`
}

type ShipDefenceProfile struct {
	ShieldSlots      *int `json:"shield_slots,omitempty"`
	ShieldGenerators *int `json:"shield_generators,omitempty"`
	WeaponMounts     *int `json:"weapon_mounts,omitempty"`
}

type NormalizedHardpointV2 struct {
	ExternalID          string  `json:"external_id"`
	ShipVariantExternal string  `json:"ship_variant_external"`
	Code                string  `json:"code"`
	Category            string  `json:"category"`
	Position            *string `json:"position,omitempty"`
	Size                *int    `json:"size,omitempty"`
	Gimballed           *bool   `json:"gimballed,omitempty"`
	Powered             *bool   `json:"powered,omitempty"`
	Seats               *int    `json:"seats,omitempty"`
}

type ShipVariantStatsV2 struct {
	Length        *float64                 `json:"length,omitempty"`
	Width         *float64                 `json:"width,omitempty"`
	Height        *float64                 `json:"height,omitempty"`
	Mass          *float64                 `json:"mass,omitempty"`
	CargoCapacity *float64                 `json:"cargo_capacity,omitempty"`
	Crew          *ShipCrewProfile         `json:"crew,omitempty"`
	Performance   *ShipPerformanceEnvelope `json:"performance,omitempty"`
	Propulsion    *ShipPropulsionProfile   `json:"propulsion,omitempty"`
	Defence       *ShipDefenceProfile      `json:"defence,omitempty"`
	Insurance     map[string]any           `json:"insurance,omitempty"`
	Raw           map[string]any           `json:"raw,omitempty"`
	Hardpoints    []NormalizedHardpointV2  `json:"hardpoints,omitempty"`
	Additional    map[string]any           `json:"additional,omitempty"`
}

type NormalizedShipVariantV2 struct {
	ExternalID   string                        `json:"external_id"`
	ShipExternal string                        `json:"ship_external"`
	Name         string                        `json:"name"`
	VariantCode  *string                       `json:"variant_code,omitempty"`
	ExternalRefs []NormalizedExternalReference `json:"external_refs"`
	Thumbnail    *string                       `json:"thumbnail,omitempty"`
	ReleasePatch *string                       `json:"release_patch,omitempty"`
	Stats        ShipVariantStatsV2            `json:"stats"`
	DateCreated  *string                       `json:"date_created,omitempty"`
}

type NormalizedItemV2 struct {
	ExternalID   string                        `json:"external_id"`
	Name         string                        `json:"name"`
	CompanyCode  *string                       `json:"company_code,omitempty"`
	Type         string                        `json:"type"`
	Subtype      *string                       `json:"subtype,omitempty"`
	Size         *int                          `json:"size,omitempty"`
	Grade        *string                       `json:"grade,omitempty"`
	Class        *string                       `json:"class,omitempty"`
	Description  *string                       `json:"description,omitempty"`
	ExternalRefs []NormalizedExternalReference `json:"external_refs"`
	Stats        map[string]any                `json:"stats"`
	DateCreated  *string                       `json:"date_created,omitempty"`
}

type NormalizedBundleV2 struct {
	Channel      Channel                   `json:"channel"`
	Version      string                    `json:"version"`
	Companies    []NormalizedCompanyV2     `json:"companies"`
	Ships        []NormalizedShipV2        `json:"ships"`
	ShipVariants []NormalizedShipVariantV2 `json:"ship_variants"`
	Items        []NormalizedItemV2        `json:"items"`
	Hardpoints   []NormalizedHardpointV2   `json:"hardpoints,omitempty"`
}
