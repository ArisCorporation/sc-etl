-- Ensure manufacturer codes remain unique regardless of casing.
CREATE UNIQUE INDEX IF NOT EXISTS ux_manufacturers_code_ci
  ON manufacturers (LOWER(code));
