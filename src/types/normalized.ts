export type Channel = 'LIVE' | 'PTU' | 'EPTU';

export interface NormalizedManufacturer {
  external_id: string;
  code: string;
  name: string;
  description?: string;
  data_source?: string;
}

export interface NormalizedShip {
  external_id: string;
  name: string;
  class: string;
  size?: string;
  manufacturer_external_id: string;
  description?: string;
}

export interface NormalizedShipVariant {
  external_id: string;
  ship_external_id: string;
  variant_code?: string;
  name?: string;
  thumbnail?: string;
  description?: string;
}

export interface NormalizedHardpoint {
  external_id: string;
  ship_variant_external_id: string;
  code: string;
  category: string;
  position?: string;
  size?: number;
  gimballed?: boolean;
  powered?: boolean;
  seats?: number;
}

export interface NormalizedItem {
  external_id: string;
  type: string;
  subtype?: string;
  name: string;
  manufacturer_external_id?: string;
  size?: number;
  grade?: string;
  class?: string;
  description?: string;
}

export interface NormalizedItemStat {
  item_external_id: string;
  stats: Record<string, unknown>;
  price_auec?: number;
  availability?: string;
}

export interface NormalizedShipStat {
  ship_variant_external_id: string;
  stats: Record<string, unknown>;
}

export interface NormalizedInstalledItem {
  ship_variant_external_id: string;
  item_external_id: string;
  quantity: number;
  hardpoint_external_id?: string;
  profile?: string;
  livery?: string | null;
}

export interface NormalizedLocaleEntry {
  namespace: string;
  key: string;
  lang: string;
  value: string;
}

export interface TransformContext {
  dataRoot: string;
  channel: Channel;
  version: string;
}

export interface NormalizedDataBundle {
  manufacturers: NormalizedManufacturer[];
  ships: NormalizedShip[];
  ship_variants: NormalizedShipVariant[];
  items: NormalizedItem[];
  hardpoints: NormalizedHardpoint[];
  item_stats: NormalizedItemStat[];
  ship_stats: NormalizedShipStat[];
  installed_items: NormalizedInstalledItem[];
  locales: NormalizedLocaleEntry[];
}
