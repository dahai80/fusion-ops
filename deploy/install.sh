#!/usr/bin/env bash
# Server-side installer for fusion-ops on the VPS (issue #5).
# Runs as root on the VPS after deploy-vps.sh staged files into
# /tmp/fusion-ops-deploy. Idempotent: re-running upgrades binaries and
# units in place; the env file is never overwritten.
set -euo pipefail

STAGE=/tmp/fusion-ops-deploy
PREFIX=/opt/fusion-ops

install -d -m 755 "$PREFIX/bin" "$PREFIX/migrations" "$PREFIX/env" /var/log/fusion-ops
install -m 755 "$STAGE"/bin/* "$PREFIX/bin/"
install -m 644 "$STAGE"/migrations/*.sql "$PREFIX/migrations/"

if [ ! -f "$PREFIX/env/fusion-ops.env" ]; then
    install -m 640 -o root -g ops "$STAGE/env/fusion-ops.env" "$PREFIX/env/fusion-ops.env"
    echo "install: env file installed (first run)"
fi
chown root:ops "$PREFIX/env/fusion-ops.env"
chmod 640 "$PREFIX/env/fusion-ops.env"

install -m 644 "$STAGE"/deploy/fusion-ops.service \
               "$STAGE"/deploy/fusion-ops-retention.service \
               "$STAGE"/deploy/fusion-ops-retention.timer \
               "$STAGE"/deploy/fusion-ops-cluster.service \
               "$STAGE"/deploy/fusion-ops-cluster.timer \
               /etc/systemd/system/

systemctl daemon-reload
echo "install: staged; enable/start via deploy script after one-shot migrate"
echo "install: done"
