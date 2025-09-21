#!/usr/bin/env bash
set -euo pipefail
export CHANNEL=PTU
export GAME_VERSION="$1"
bun run src/index.ts --channel=$CHANNEL --version=$GAME_VERSION