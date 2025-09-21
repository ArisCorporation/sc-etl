#!/usr/bin/env bash
set -euo pipefail
export CHANNEL=LIVE
export GAME_VERSION="$1" # z. B. ./scripts/ingest-live.sh 4.3.1
bun run src/index.ts --channel=$CHANNEL --version=$GAME_VERSION