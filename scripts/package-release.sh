#!/usr/bin/env bash
# Build cross-platform release tarballs for the homebrew tap (fusion-ops
# packaging, mirrors the fusion-identity pattern: prebuilt per-platform
# tarballs attached to the GitHub release, formula copied verbatim into
# dahai80/homebrew-fusion-mlx with real sha256s).
#
# Usage: scripts/package-release.sh <version>   e.g. 0.0.9
set -euo pipefail
cd "$(dirname "$0")/.."

version="${1:?usage: package-release.sh <version>}"
dist="dist"
mkdir -p "$dist"

binaries=(fusion-ops fusion-ops-job fusion-ops-probe fusion-broker migrate)

for platform in darwin/arm64 darwin/amd64 linux/arm64 linux/amd64; do
    os="${platform%/*}"
    arch="${platform#*/}"
    out="fusion-ops-${version}-${os}-${arch}"
    echo "package: building $out"
    work="$dist/$out"
    rm -rf "$work" && mkdir -p "$work"
    for b in "${binaries[@]}"; do
        GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build -o "$work/$b" "./cmd/$b"
    done
    tar -czf "$dist/$out.tar.gz" -C "$work" .
    rm -rf "$work"
    shasum -a 256 "$dist/$out.tar.gz"
done
echo "package: tarballs in $dist/"
