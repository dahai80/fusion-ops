#!/usr/bin/env bash
# Mac-side deploy of fusion-ops to the VPS (issue #5, topology decision (a)):
# jobs + API server run on the VPS next to the prod ops store; the blackbox
# probe stays on the Mac and writes through the SSH tunnel (launchd).
# Env file with the app-role password is generated once locally into
# deploy/env/ (git-ignored) and staged to /opt/fusion-ops/env (root:ops 640).
set -euo pipefail
cd "$(dirname "$0")/.."

VPS=fusion-vps
STAGE=/tmp/fusion-ops-deploy
ENV_LOCAL=deploy/env/fusion-ops.env

mkdir -p deploy/env bin-linux
if [ ! -f "$ENV_LOCAL" ]; then
    pw=$(openssl rand -hex 16)
    cat > "$ENV_LOCAL" <<EOF
FUSION_OPS_DATABASE_URL=postgres://fusion_ops_app:${pw}@127.0.0.1:5432/fusion_ops?sslmode=disable
FUSION_OPS_LISTEN=127.0.0.1:11400
FUSION_OPS_MIGRATIONS_DIR=/opt/fusion-ops/migrations
EOF
    chmod 600 "$ENV_LOCAL"
    echo "deploy: generated $ENV_LOCAL with fresh app-role password"
fi

pw=$(sed -n 's/^FUSION_OPS_DATABASE_URL=postgres:\/\/fusion_ops_app:\([^@]*\)@.*/\1/p' "$ENV_LOCAL")

echo "deploy: building linux/amd64 binaries"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin-linux/ ./cmd/fusion-ops ./cmd/fusion-ops-job ./cmd/fusion-ops-probe ./cmd/migrate

echo "deploy: staging to $VPS:$STAGE"
ssh "$VPS" "rm -rf $STAGE && mkdir -p $STAGE/bin $STAGE/migrations $STAGE/deploy $STAGE/env"
scp -q bin-linux/* "$VPS:$STAGE/bin/"
scp -q migrations/*.sql "$VPS:$STAGE/migrations/"
scp -q deploy/*.service deploy/*.timer deploy/install.sh "$VPS:$STAGE/deploy/"
scp -q "$ENV_LOCAL" "$VPS:$STAGE/env/fusion-ops.env"

echo "deploy: run installer"
ssh "$VPS" "sudo bash $STAGE/deploy/install.sh"

echo "deploy: set app-role password (via psql stdin, never in remote argv)"
ssh "$VPS" "sudo -u postgres psql -v ON_ERROR_STOP=1" <<SQL
ALTER ROLE fusion_ops_app PASSWORD '${pw}';
SQL

echo "deploy: one-shot migrate as postgres admin (tracks schema_migrations on prod)"
ssh "$VPS" "sudo -u postgres env FUSION_OPS_DATABASE_URL='host=/var/run/postgresql dbname=fusion_ops sslmode=disable' /opt/fusion-ops/bin/migrate -dir /opt/fusion-ops/migrations"

echo "deploy: enable + start service and timers"
ssh "$VPS" "sudo systemctl enable --now fusion-ops.service fusion-ops-retention.timer fusion-ops-cluster.timer && systemctl is-active fusion-ops.service"

echo "deploy: done"
