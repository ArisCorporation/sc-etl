# Star Citizen ETL Pipeline

Robuste, versionierte ETL-Pipeline zur Übernahme der Star-Citizen-Spieldaten aus entpackten P4K-Exports in Directus.

## Voraussetzungen

- Node.js \>= 20.10 (Volta optional)
- pnpm 9\+
- Zugriff auf eine Directus-Instanz (Static Token mit Schreibrechten)
- Lokale Rohdaten unter `data/raw/<CHANNEL>/<VERSION>/`

## Installation

1. Repository auschecken und Abhängigkeiten installieren:
   ```bash
   pnpm install
   ```
2. `.env` anlegen (Beispiel unten) und sensible Werte nur lokal halten:
   ```env
   DIRECTUS_URL=https://directus.example.com
   DIRECTUS_TOKEN=STATIC_TOKEN
   DATA_ROOT=./data
   P4K_PATH=./Data.p4k
   UNP4K_ENABLED=1
   UNP4K_BIN=./bins/unp4k/unp4k.exe
   UNP4K_ARGS="{{p4k}} *.xml *.ini --output {{output}}"
   UNFORGE_ENABLED=1
   UNFORGE_BIN=./bins/unp4k/unforge.exe
   UNFORGE_ARGS={{input}}
   # Optional: weitere Verarbeitung (z. B. scdatadumper von octfx)
   SC_DATA_DUMPER_ENABLED=1
   SC_DATA_DUMPER_BIN=php
   # Optional: zusätzliche Argumente; Default ist "cli.php load:data --scUnpackedFormat {{input}} {{output}}"
   #SC_DATA_DUMPER_ARGS="cli.php load:data --scUnpackedFormat {{input}} {{output}}"
   #SC_DATA_DUMPER_OUTPUT=./data/raw/LIVE/4.3.1
   WINE_BIN=wine
   ```
3. Rohdaten (JSON) in das passende Channel/Version-Verzeichnis legen, z. B. `data/raw/LIVE/4.3.1/`.

## Ausführung

```bash
pnpm tsx src/index.ts --channel=LIVE --version=4.3.1
```

Optionale Flags:

- `--data-root=./data` überschreibt `DATA_ROOT`
- `--skip-diffs` deaktiviert das Schreiben in die `diffs`-Collection

Der Lauf schreibt normalisierte JSONs zurück nach `data/normalized/<CHANNEL>/<VERSION>/`, validiert sie gegen `schemas/*.json` und lädt sie via Directus REST SDK.

### Migrations

```bash
pnpm tsx scripts/migrate_manufacturers.ts --dry-run
pnpm tsx scripts/migrate_dedup_variants.ts --dry-run --channel=LIVE
```

Mit `--apply` lassen sich beide Skripte produktiv ausführen. Standard ist stets ein Dry-Run.

### Binaries & Wine-Erkennung

- Lege die entpackten Tools unter `./bins/` ab (Standard: `./bins/unp4k/unp4k.exe`, `./bins/unp4k/unforge.exe`, optional `./bins/scdatadumper/...`). Für die P4K-Datei gilt die Konvention `./data/p4k/Data.p4k` – dieser Pfad wird automatisch gefunden, solange du keinen anderen angibst.
- Auf Nicht-Windows-Systemen ruft der ETL automatisch `wine` auf, sobald ein `.exe` erkannt wird. Den verwendeten Befehl kannst du über `WINE_BIN`/`--wine-bin` anpassen.

### Automatische unp4k-Extraktion

- Aktivierung via `UNP4K_ENABLED=1` (Default: aktiv, solange `P4K_PATH` gesetzt ist). Mit `UNP4K_ENABLED=0` oder `--unp4k-enabled=false` lässt sich der Schritt deaktivieren.
- Liegt die P4K-Datei unter `Data.p4k`, wird sie automatisch gefunden. Alternativ Pfad via `P4K_PATH` (ENV) oder `--p4k=/pfad/zur/Data.p4k` setzen.
- `UNP4K_BIN` (oder `--unp4k-bin`) definiert das aufzurufende Binary, standardmäßig `unp4k`.
- Mit `UNP4K_ARGS` bzw. wiederholbaren `--unp4k-arg` Flags übergibst du zusätzliche Parameter. Die Platzhalter `{{p4k}}` und `{{output}}` werden vor dem Aufruf ersetzt.
- Falls das Raw-Verzeichnis bereits existiert, wird unp4k übersprungen. Mit `FORCE_UNP4K=1` oder `--force-unp4k` erzwingst du eine frische Extraktion (bestehende Dateien werden zuvor entfernt).
- Das Zielverzeichnis lautet immer `data/raw/<CHANNEL>/<VERSION>/` und wird vor dem Transform-Schritt befüllt.
- **Release & Installation:** Lade das aktuelle `unp4k-suite` ZIP (z. B. v3.3.x) von [dolkensp/unp4k](https://github.com/dolkensp/unp4k/releases) herunter, entpacke es, und stelle sicher, dass das Archiv ggf. über den Windows Explorer „entblockt“ wurde. Das CLI benötigt mindestens .NET Framework 4.6.2 (auf Windows) – bei der Fehlermeldung `Method not found: '!!0[] System.Array.Empty()'.` muss das Framework installiert werden.
- **Quickstart:** Unter Windows reicht es, `Data.p4k` auf `unp4k.exe` zu ziehen. Für Skripte bietet sich `unp4k.exe <Pfad\zur\Data.p4k> [filter]` an; Wildcards sind auf `*.ext` beschränkt.
- **GUI:** `unp4k.gui.exe` existiert als Alpha-Version (instabil, viele Crashes). Nutzung auf eigenes Risiko.
- **Linux-Kompatibilität:** unp4k ist primär für Windows gebaut. Unter Linux kannst du es über `wine` bzw. das .NET/Mono-Runtime starten (`UNP4K_BIN="wine"` und erste `--unp4k-arg` als Pfad zu `unp4k.exe`). Alternativ lässt sich ein vorgefertigtes Container-Image verwenden – wichtig ist, dass das CLI am Ende als Prozess erreichbar ist.
- **Dateiformat:** Die P4K-Dateien sind ZIP-Archive mit CryEngine-spezifischen Formaten (inkl. CryXML und DataForge). unp4k extrahiert viele Inhalte als XML.

### Optionaler unforge-Schritt

- Aktivierung via `UNFORGE_ENABLED=1` (Default: folgt dem Status von unp4k). Mit `UNFORGE_ENABLED=0`/`--unforge-enabled=false` lässt sich der Schritt überspringen.
- Mit `UNFORGE_ENABLED=1` (oder `--unforge-enabled`) wird nach unp4k automatisch `unforge` ausgeführt, um CryXML-Dateien zu de-serialisieren. Standardmäßig wird `./bins/unp4k/unforge.exe` mit dem Platzhalter `{{input}}` (das Zielverzeichnis von unp4k) aufgerufen.
- Argumente lassen sich über `UNFORGE_ARGS` (bzw. `--unforge-arg`) steuern. Verwende `{{input}}`, um das Verzeichnis zu referenzieren.

### Optionaler XML → JSON Schritt (scdatadumper o. ä.)

- Wenn nach dem unp4k-Run nur XML-Dateien vorliegen, kannst du eine weitere CLI (z. B. [octfx/scdatadumper](https://github.com/Oct0f1sh/scdatadumper)) einhängen. Setze dafür `SC_DATA_DUMPER_BIN`/`--scd-bin` sowie `SC_DATA_DUMPER_ARGS`/`--scd-arg` und nutze die Platzhalter `{{input}}` (Roh-Ordner) und `{{output}}`.
- Standardmäßig erwartet der ETL JSON-Dateien im Raw-Verzeichnis (`data/raw/<CHANNEL>/<VERSION>/`). Wenn dein Converter in ein separates Output-Verzeichnis schreibt, kannst du es über `SC_DATA_DUMPER_OUTPUT` bzw. `--scd-output` festlegen.
- Sollten zusätzliche Formate (etwa reine XML) übrig bleiben, erweitere den Transform-Schritt oder die Konvertierung, sodass die erwarteten JSON-Dateien (`manufacturers.json`, `ships.json`, …) erzeugt werden.
- Vorbereitung (Docker): Container einmalig mit `docker compose up -d --build --force-recreate` starten; der ETL führt anschließend `docker compose exec …` aus.
- Vorbereitung (lokale PHP-Installation): Im Ordner `bins/scdatadumper` `composer install --no-dev` ausführen, damit `vendor/` und Autoloader vorhanden sind.
- Quickstart (Docker):
  ```bash
  SC_DATA_DUMPER_BIN=docker
  SC_DATA_DUMPER_ARGS="compose exec scdatadumper php cli.php load:data --scUnpackedFormat {{input}} {{output}}"
  ```
  Optional kann (einmalig) der Cache über `docker compose exec scdatadumper php cli.php generate:cache {{input}}` erzeugt werden – der ETL ruft diesen Schritt bei Bedarf automatisch vor `load:data` auf.
- Quickstart (lokale PHP-Installation):
  ```bash
  SC_DATA_DUMPER_BIN=php
  SC_DATA_DUMPER_ARGS="cli.php load:data --scUnpackedFormat {{input}} {{output}}"
  ```
- Der PHP-Modus ersetzt `cli.php` automatisch durch den absoluten Pfad (`bins/scdatadumper/cli.php`) und übergibt absolute `{{input}}`/`{{output}}`-Pfadwerte. Cache-Erzeugung (`generate:cache`) läuft vor `load:data` automatisch, solange `SC_DATA_DUMPER_ENABLED=1` gesetzt ist.
- Zusätzliche Dumps (`load:items`, `load:vehicles`, …) kannst du über weitere Aufrufe realisieren, indem du mehrere `--scd-arg` Blöcke oder nachgelagerte Skripte definierst. Der ETL ruft aktuell genau einen Konverter-Lauf auf – stelle sicher, dass in `{{output}}` die finalen JSON-Dateien landen, mit denen `transform.ts` arbeitet.
- Aktivierung via `SC_DATA_DUMPER_ENABLED=1` (Default: deaktiviert, solange kein `SC_DATA_DUMPER_BIN` gesetzt ist). Wenn keine Argumente gesetzt sind, werden je nach Binary sinnvolle Defaults verwendet (`docker compose exec ... load:data`, bzw. `php cli.php load:data ...`).

## Pipeline-Überblick

1. **Extract** (`src/extract.ts`): Verifiziert die Rohdatenstruktur und meldet fehlende Pflichtdateien.
   - Optional: Liegt `P4K_PATH`/`--p4k` vor und das Zielverzeichnis fehlt (oder `--force-unp4k` ist aktiv), wird automatisch `unp4k` aufgerufen. Die CLI-Argumente lassen sich über `UNP4K_ARGS` steuern (Platzhalter `{{p4k}}`, `{{output}}`).
   - Optional: `unforge` läuft direkt nach unp4k (`UNFORGE_*` Variablen) und konvertiert CryXML in „normale“ XML-Dateien.
   - Optional: Mit `SC_DATA_DUMPER_BIN`/`--scd-bin` kann ein weiterer Converter (z. B. scdatadumper) ausgeführt werden, um XML-Ausgaben nach JSON zu überführen (`{{input}}`, `{{output}}`).
2. **Transform** (`src/transform.ts`): Mapped Game-Attribute auf normalisierte Strukturen mit `*_external_id`, aggregiert Locales und persistiert JSON-Ausgaben.
3. **Validate** (`src/validate.ts`): AJV-Validierung pro Collection anhand der Schemas in `schemas/`.
4. **Load** (`src/load.ts`):
   - Stellt sicher, dass der Build in Directus (`builds`) existiert (`ensureBuild`).
   - Upsertet Stammdaten (`companies`, `ships`, `ship_variants`, `items`, `hardpoints`).
   - Synchronisiert build-gebundene Tabellen (`item_stats`, `ship_stats`, `installed_items`) inkl. Löschung veralteter Kombinationen.
   - Aktualisiert Locales (`namespace`, `key`, `lang`).
   - Markiert den Build als `ingested`.
5. **Diffs** (`src/diffs.ts`): Vergleicht aktuelle Daten mit dem zuletzt ingested Build des selben Channels und schreibt Änderungen in `diffs`.

Batch-Größen werden bei Upserts auf 500 limitiert; Fremdschlüssel werden via `external_id`-Maps aufgelöst. Alle Annahmen über Rohdaten sind im Code mit `// ASSUMPTION:` gekennzeichnet.

> **Hinweis:** Das Directus-Schema in `directus-schema.json` enthält aktuell kein `item`-Feld auf `installed_items`. Der Loader überspringt deshalb Loadouts und loggt eine Warnung. Sobald das Feld (UUID M2O -> `items`) ergänzt wurde, kann der entsprechende Block in `src/load.ts` wieder aktiviert werden.

## SQL-Views für "Latest"-Abfragen

Fertige SQL-Skripte für Postgres und MySQL befinden sich in `sql/postgres/latest_views.sql` bzw. `sql/mysql/latest_views.sql`. Sie erzeugen:

- `vw_latest_item_stats`
- `vw_latest_ship_stats`
- `vw_installed_items_latest`

Jede View liefert eine künstliche `pk`, sortiert je Channel nach `builds.released` (Fallback `ingested`) und zeigt nur den aktuellsten Stand pro Entität. Für `vw_installed_items_latest` muss – wie oben erwähnt – ein `item`-Feld auf `installed_items` vorhanden sein.

## Nützliche Skripte

- `pnpm run build` — TypeScript-Build (emittiert nach `dist/`)
- `pnpm run clean` — entfernt `dist/`
- `pnpm run etl` — Alias für den Dev-Run (ts-node)

## Datenablage

```
data/
 ├─ raw/<CHANNEL>/<VERSION>/        # Input aus dem Game-Export
 └─ normalized/<CHANNEL>/<VERSION>/ # Normalisierte JSONs für Audits & Re-Runs
```

## Fehlerbehandlung & Logging

Einfaches konsolenbasiertes Logging (`src/utils/log.ts`) kennzeichnet jeden Schritt. Validierungs- oder Ladefehler brechen den Run ab (`process.exitCode = 1`).

## Weiteres Vorgehen

- Directus-Permissions für neue Collections/Views setzen.
- Optional Materialized Views / Indexe in der DB ergänzen.
- Automatisierte Tests auf Rohdatensamples aufsetzen.

## Prozess-Schritte im Überblick

1. **Binaries & Daten bereitstellen** – `Data.p4k` unter `./data/p4k/Data.p4k` (oder via `P4K_PATH`) ablegen, unp4k/unforge (und optional scdatadumper) nach `./bins/...` entpacken, Toggle-ENV (`UNP4K_ENABLED`, `UNFORGE_ENABLED`, `SC_DATA_DUMPER_ENABLED`) setzen.
2. **Environment konfigurieren** – `.env` mit `DIRECTUS_URL`, `DIRECTUS_TOKEN`, `DATA_ROOT` etc. anlegen; bei Bedarf `WINE_BIN` anpassen.
3. **ETL starten** – `pnpm run dev -- --channel=LIVE --version=4.3.1` (Parameter anpassen). Die konfigurierten Schritte (`unp4k` → `unforge` → optional `scdatadumper`) laufen automatisch, sofern aktiviert.
4. **Transform & Validate prüfen** – Normalisierte Dateien unter `data/normalized/...` sichten, AJV-Validierung läuft automatisch.
5. **Load beobachten** – Loader schreibt in die Directus-Collections (`companies`, `ships`, `ship_variants`, `items`, `hardpoints`, `item_stats`, `ship_stats`, `locales`, `builds`).
6. **Diffs generieren** – sofern `--skip-diffs` nicht gesetzt ist, landen Unterschiede im `diffs`-Table.
7. **Views einspielen** – SQL-Skripte für Postgres/MySQL auf der Datenbank ausführen (nach Ergänzung des `installed_items.item`-Feldes).
