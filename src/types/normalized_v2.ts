import type { Channel } from './normalized.js';

export interface NormalizedExternalReference {
  source: string;
  id: string;
  note?: string;
}

export interface NormalizedCompanyV2 {
  code: string;
  name?: string;
  external_refs: NormalizedExternalReference[];
}

export interface NormalizedShipV2 {
  external_id: string;
  name: string;
  company_code: string;
  external_refs: NormalizedExternalReference[];
  paints?: string[];
}

export interface ShipPerformanceEnvelope {
  scm_speed?: number;
  afterburner_speed?: number;
  accelerations?: Record<string, number>;
  pitch_rate?: number;
  yaw_rate?: number;
  roll_rate?: number;
}

export interface ShipCrewProfile {
  minimum?: number;
  maximum?: number;
  recommended?: number;
}

export interface ShipPropulsionProfile {
  hydrogen_capacity?: number;
  quantum_capacity?: number;
  main_thrusters?: number;
  maneuver_thrusters?: number;
  fuel_intakes?: number;
  fuel_tanks?: number;
  power_output?: number;
}

export interface ShipDefenceProfile {
  shield_slots?: number;
  shield_generators?: number;
  weapon_mounts?: number;
}

export interface ShipVariantStatsV2 {
  length?: number;
  width?: number;
  height?: number;
  mass?: number;
  cargo_capacity?: number;
  crew?: ShipCrewProfile;
  performance?: ShipPerformanceEnvelope;
  propulsion?: ShipPropulsionProfile;
  defence?: ShipDefenceProfile;
  insurance?: Record<string, unknown>;
  raw?: Record<string, unknown>;
  hardpoints?: NormalizedHardpointV2[];
  [key: string]: unknown;
}

export interface NormalizedShipVariantV2 {
  external_id: string;
  ship_external: string;
  name: string;
  variant_code?: string;
  external_refs: NormalizedExternalReference[];
  thumbnail?: string;
  release_patch?: string;
  stats: ShipVariantStatsV2;
  date_created?: string;
}

export interface NormalizedItemV2 {
  external_id: string;
  name: string;
  company_code?: string;
  type: string;
  subtype?: string;
  size?: number;
  grade?: string;
  class?: string;
  description?: string;
  external_refs: NormalizedExternalReference[];
  stats: Record<string, unknown>;
  date_created?: string;
}

export interface NormalizedHardpointV2 {
  external_id: string;
  ship_variant_external: string;
  code: string;
  category: string;
  position?: string;
  size?: number;
  gimballed?: boolean;
  powered?: boolean;
  seats?: number;
}

export interface NormalizedBundleV2 {
  channel: Channel;
  version: string;
  companies: NormalizedCompanyV2[];
  ships: NormalizedShipV2[];
  ship_variants: NormalizedShipVariantV2[];
  items: NormalizedItemV2[];
  hardpoints?: NormalizedHardpointV2[];
}
