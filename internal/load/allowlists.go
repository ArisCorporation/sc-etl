package load

import "strings"

var defaultAllowedItemTypes = []string{
	"QUANTUMDRIVE",
	// "SHIELD",
	// "SHIELDCONTROLLER",
	// "COOLER",
	// "COOLERCONTROLLER",
	// "POWERPLANT",
	// "FUELTANK",
	// "EXTERNALFUELTANK",
	// "FUELINTAKE",
	// "QUANTUMFUELTANK",
	// "RADAR",
	// "SENSOR",
	// "SCANNER",
	// "AIMODULE",
	// "WEAPONGUN",
	// "WEAPONATTACHMENT",
	// "WEAPONDEFENSIVE",
	// "WEAPONCONTROLLER",
	// "MISSILE",
	// "MISSILELAUNCHER",
	// "MISSILECONTROLLER",
	// "TURRET",
	// "TURRETBASE",
	// "UTILITYTURRET",
	// "ARMOR",
	// "MAINTHRUSTER",
	// "MANNEUVERTHRUSTER",
	// "TRACTORBEAM",
	// "TOWINGBEAM",
	// "RAILGUN",
	// "RELAY",
	// "MININGMODIFIER",
	// "MININGCONTROLLER",
	// "WEAPONMINING",
	// "TOOLARM",
	// "SALVAGEHEAD",
	// "SALVAGECONTROLLER",
	// "SALVAGEMODIFIER",
	// "SALVAGEFILLERSTATION",
	// "SALVAGEINTERNALSTORAGE",
	// "SALVAGEFIELDSUPPORTER",
	// "SALVAGEFIELDEMITTER",
}

var defaultAllowedHardpointCategories = []string{
	// "Shield",
	// "ShieldController",
	"QuantumDrive",
	// "Cooler",
	// "CoolerController",
	// "PowerPlant",
	// "FuelTank",
	// "ExternalFuelTank",
	// "FuelIntake",
	// "QuantumFuelTank",
	// "Radar",
	// "AIModule",
	// "WeaponGun",
	// "RailGun",
	// "WeaponDefensive",
	// "WeaponController",
	// "WeaponMount",
	// "Missile",
	// "MissileLauncher",
	// "MissileController",
	// "Turret",
	// "TurretBase",
	// "UtilityTurret",
	// "Armor",
	// "MainThruster",
	// "ManneuverThruster",
	// "Relay",
	// "ToolArm",
	// "SalvageHead",
	// "SalvageController",
	// "SalvageModifier",
	// "SalvageFillerStation",
	// "SalvageFieldEmitter",
	// "SalvageFieldSupporter",
	// "SalvageInternalStorage",
	// "MiningController",
	// "WeaponMining",
	// "TractorBeam",
	// "TowingBeam",
	// "CapacitorAssignmentController",
	// "CommsController",
	// "EnergyController",
	// "FuelController",
	// "FlightController",
	// "LandingSystem",
	// "DockingCollar",
	// "Cargo",
	// "JumpDrive",
	// "LifeSupportGenerator",
	// "WheeledController",
	// "Container",
	// "AttachedPart",
	// "SelfDestruct",
}

func normalizeAllowlist(values []string) ([]string, map[string]struct{}) {
	list := []string{}
	set := map[string]struct{}{}
	seen := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		if _, ok := seen[upper]; ok {
			continue
		}
		seen[upper] = struct{}{}
		list = append(list, trimmed)
		set[upper] = struct{}{}
	}
	return list, set
}

func buildEqualityFilter(field string, values []string) map[string]any {
	switch len(values) {
	case 0:
		return nil
	case 1:
		return map[string]any{
			field: map[string]any{
				"_eq": values[0],
			},
		}
	default:
		return map[string]any{
			field: map[string]any{
				"_in": values,
			},
		}
	}
}
