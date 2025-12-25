# Star Citizen ETL Pipeline (Go)

Robuste, versionierte ETL-Pipeline zur Übernahme der Star-Citizen-Spieldaten aus entpackten P4K-Exports in Directus – vollständig in Go umgesetzt.

## Voraussetzungen

- Go \>= 1.22
- Zugriff auf eine Directus-Instanz (Static Token mit Schreibrechten)
- Lokale Rohdaten unter `data/raw/<CHANNEL>/<VERSION>/`
- Optional: Wine, falls Windows-Tools (`unp4k.exe`, `unforge.exe`) unter Linux/macOS ausgeführt werden

## Installation & Build

1. Repository auschecken.
2. Optional: `go mod tidy`, um alle Module lokal aufzulösen.
3. Binary bauen oder direkt über `go run` starten:

```bash
go build -o bin/scgoetl ./cmd/scgoetl
# oder
go run ./cmd/scgoetl --channel=LIVE --version=4.3.1
```

## Konfiguration

Die CLI liest Flags (siehe `--help`) und Umgebungsvariablen. Eine minimale `.env` reicht aus, um nur Directus-Zugangsdaten (für den Load-Step) und z. B. den Wine-Pfad zu setzen:

```env
DIRECTUS_URL=https://directus.example.com
DIRECTUS_TOKEN=STATIC_TOKEN
WINE_BIN=wine
#DATA_ROOT=./custom-data         # optional override
#P4K_PATH=./data/p4k/Data.p4k    # optional override
```

Standardwerte (gelten ohne Overrides):

- `CHANNEL=LIVE`, `GAME_VERSION=0.0.0` (werden meist über CLI-Flags gesetzt)
- `DATA_ROOT=./data`
- `P4K_PATH=./data/p4k/Data.p4k`
- `UNP4K_ENABLED=1`, `UNP4K_BIN=./bins/unp4k/unp4k.exe`
- `UNFORGE_ENABLED=1`, `UNFORGE_BIN=./bins/unp4k/unforge.exe`
- `SC_DATA_DUMPER_ENABLED=1`, `SC_DATA_DUMPER_BIN=php`, `SC_DATA_DUMPER_ARGS="-d memory_limit=2G cli.php load:data --scUnpackedFormat {{input}} {{output}}"`
- `WINE_BIN=wine` (auf macOS/Linux; auf Windows leer)

Damit läuft die komplette Extract-Pipeline automatisch, solange `./data/p4k/Data.p4k` existiert. Direktes Laden nach Directus benötigt weiterhin gültige `DIRECTUS_URL`/`DIRECTUS_TOKEN` Werte (oder das Flag `--load-enabled=false`, falls nur Extract/Transform nötig ist).

Wichtige Flags/ENV-Variablen:

- `--channel` / `CHANNEL` (LIVE/PTU/EPTU)
- `--version` / `GAME_VERSION`
- `--data-root` / `DATA_ROOT`
- `--load-enabled` / `LOAD_ENABLED`
- `--default-company-category` / `DEFAULT_COMPANY_CATEGORY`
- `--p4k` / `P4K_PATH`
- `--unp4k-enabled`, `--unforge-enabled`, `--scd-enabled`

Neue Firmen (Manufacturers) werden vom Loader nicht mehr automatisch in Directus angelegt. Alle Hersteller-Codes müssen daher bereits im Directus-Modul existieren, damit sie per Code gematcht und aktualisiert werden können. Fehlt ein Eintrag, wird der Datensatz übersprungen und z. B. Items erhalten keinen Herstellerbezug.

## Ablauf

1. **Extract** – optionaler Aufruf von `unp4k`, `unforge`, `scdatadumper`. Die Rohdaten landen unter `data/raw/<CHANNEL>/<VERSION>/`.
2. **Transform** – Normalisierte JSONs und V2-Bundles werden in `data/normalized/<CHANNEL>/<VERSION>/` geschrieben und gegen `schemas/*.json` validiert.
3. **Load** – Der Loader vergleicht die Daten mit Directus (`companies`, `ship_hulls`, `ship_variants`, `items`, `ship_hardpoints`, `item_stats`, `ship_stats`, `locales`, `ship_variant_configurations`, `ship_variant_configuration_hardpoints`) und führt versionierte Upserts aus. Builds werden auf `ingested` gesetzt.

Alle Schritte loggen JSON über `internal/utils/log` (stderr).

## Datenablage

```
data/
 ├─ raw/<CHANNEL>/<VERSION>/        # Input aus dem Game-Export
 └─ normalized/<CHANNEL>/<VERSION>/ # Normalisierte JSONs für Audits & Re-Runs
```

## Hinweise zu externen Tools

### unp4k

- Binärdatei unter `./bins/unp4k/` ablegen (z. B. `unp4k.exe`).
- CLI-Platzhalter `{{p4k}}` und `{{output}}` werden ersetzt.
- Unter Linux/macOS via `wine` (oder Mono/.NET) lauffähig.

### unforge

- Wird nach unp4k ausgeführt, sofern `UNFORGE_ENABLED=1`.
- Standardargument `{{input}}` kann über `UNFORGE_ARGS` bzw. `--unforge-arg` angepasst werden.

### scdatadumper

- Optional (`SC_DATA_DUMPER_ENABLED=1`).
- Unterstützt `docker`, `podman`, `docker-compose`, `php` oder native Binaries.
- Platzhalter `{{input}}` und `{{output}}` werden ersetzt.

## SQL Views

Unter `./sql` befinden sich Beispielfunktionen/-Views (Postgres). Sie setzen ein zusätzliches Feld `item` auf `installed_items` voraus:

- `vw_latest_data_bundle`
- `vw_latest_item_stats`
- `vw_latest_ship_stats`
- `vw_installed_items_latest`

## Troubleshooting

- **Validierungsfehler**: JSON-Schemas liegen unter `schemas/`. Bei Schemafehlern erscheinen detaillierte Meldungen im Log.
- **Directus-Fehler**: Responses werden inklusive Status/Body protokolliert.
- **Build schlägt fehl**: Go-Version prüfen (`go version`). `go mod tidy` nachziehen, falls Abhängigkeiten fehlen.

## Weiteres Vorgehen

- Directus-Permissions für neue Collections/Views anpassen.
- Optional Materialized Views / Indexe in der Datenbank ergänzen.
- Automatisierte Tests mit Rohdatensamples etablieren.

Viel Erfolg beim Betrieb der Go-basierten ETL-Pipeline!
