#!/bin/bash
# Launcher for the launchd probe daemon (issue #5). Sources the private env
# file so credentials never live inside the plist.
set -a
[ -f "$HOME/.fusion-ops/env" ] && . "$HOME/.fusion-ops/env"
set +a
exec /Users/dahai/.fusion-ops/bin/fusion-ops-probe
