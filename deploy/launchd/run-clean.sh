#!/usr/bin/env bash
# Mac-side fusion-ops-clean runner (issue #15): workspace sweep with audit
# into fusion_ops.cleanup_audit. Policy lives in ~/.fusion-ops/clean-config.json
# (dry_run=true until the operator flips it after reviewing would_delete rows).
set -euo pipefail

ENV_FILE="$HOME/.fusion-ops/env"
POLICY="$HOME/.fusion-ops/clean-config.json"
LOG_TAG="fusion-ops-clean"

# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

# Rotate our own log (10MB cap, keep one previous) before writing more.
LOG="$HOME/.fusion-ops/logs/clean.log"
if [ -f "$LOG" ] && [ "$(stat -f%z "$LOG")" -gt 10485760 ]; then
    mv "$LOG" "$LOG.1"
fi

exec "$HOME/.fusion-ops/bin/fusion-ops-clean" -config="$POLICY"
