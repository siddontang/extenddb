#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
# Portable offline checker. Set EXTENDDB_BINARY or pass --binary to select a build.
set -euo pipefail
TASK_REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
exec python3 "$TASK_REPO_ROOT/devtools/doc_consistency.py" "$@"
