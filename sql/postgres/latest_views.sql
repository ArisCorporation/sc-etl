-- Postgres view definitions for latest build projections

CREATE OR REPLACE VIEW vw_latest_item_stats AS
WITH ranked AS (
  SELECT
    is_data.id,
    is_data.item,
    i.external_id AS item_external_id,
    is_data.build,
    b.channel,
    b.released,
    b.ingested,
    is_data.stats,
    is_data.price_auec,
    is_data.availability,
    ROW_NUMBER() OVER (
      PARTITION BY i.external_id, b.channel
      ORDER BY COALESCE(b.released, b.ingested) DESC, b.ingested DESC, is_data.id DESC
    ) AS rn
  FROM item_stats AS is_data
  JOIN game_builds AS b ON b.id = is_data.build
  JOIN items AS i ON i.id = is_data.item
  WHERE b.status = 'ingested'
)
SELECT
  concat(r.channel, ':', r.item_external_id) AS pk,
  r.id,
  r.item,
  r.item_external_id,
  r.build,
  r.channel,
  r.released,
  r.ingested,
  r.stats,
  r.price_auec,
  r.availability
FROM ranked AS r
WHERE r.rn = 1;

CREATE OR REPLACE VIEW vw_latest_ship_stats AS
WITH ranked AS (
  SELECT
    ss.id,
    ss.ship_variant,
    sv.external_id AS ship_variant_external_id,
    ss.build,
    b.channel,
    b.released,
    b.ingested,
    ss.stats,
    ROW_NUMBER() OVER (
      PARTITION BY sv.external_id, b.channel
      ORDER BY COALESCE(b.released, b.ingested) DESC, b.ingested DESC, ss.id DESC
    ) AS rn
  FROM ship_stats AS ss
  JOIN game_builds AS b ON b.id = ss.build
  JOIN ship_variants AS sv ON sv.id = ss.ship_variant
  WHERE b.status = 'ingested'
)
SELECT
  concat(r.channel, ':', r.ship_variant_external_id) AS pk,
  r.id,
  r.ship_variant,
  r.ship_variant_external_id,
  r.build,
  r.channel,
  r.released,
  r.ingested,
  r.stats
FROM ranked AS r
WHERE r.rn = 1;

-- The installed items view assumes an `item` column exists on the `installed_items` collection.
-- If the current Directus schema omits this field, add it (UUID M2O -> items) before running the
-- statement below or skip creating this view for now.
CREATE OR REPLACE VIEW vw_installed_items_latest AS
WITH ranked AS (
  SELECT
    ii.id,
    ii.ship_variant,
    sv.external_id AS ship_variant_external_id,
    ii.item,
    i.external_id AS item_external_id,
    ii.hardpoint,
    hp.external_id AS hardpoint_external_id,
    ii.quantity,
    ii.build,
    b.channel,
    b.released,
    b.ingested,
    ROW_NUMBER() OVER (
      PARTITION BY sv.external_id, i.external_id, COALESCE(hp.external_id, ''), b.channel
      ORDER BY COALESCE(b.released, b.ingested) DESC, b.ingested DESC, ii.id DESC
    ) AS rn
  FROM installed_items AS ii
  JOIN game_builds AS b ON b.id = ii.build
  JOIN ship_variants AS sv ON sv.id = ii.ship_variant
  JOIN items AS i ON i.id = ii.item
  LEFT JOIN hardpoints AS hp ON hp.id = ii.hardpoint
  WHERE b.status = 'ingested'
)
SELECT
  concat(
    r.channel,
    ':',
    r.ship_variant_external_id,
    ':',
    r.item_external_id,
    ':',
    COALESCE(r.hardpoint_external_id, 'na')
  ) AS pk,
  r.id,
  r.ship_variant,
  r.ship_variant_external_id,
  r.item,
  r.item_external_id,
  r.hardpoint,
  r.hardpoint_external_id,
  r.quantity,
  r.build,
  r.channel,
  r.released,
  r.ingested
FROM ranked AS r
WHERE r.rn = 1;
