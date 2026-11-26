#!/usr/bin/env bash
# Build cross-platform release archives for the GitHub release (fusion-ops
# packaging, mirrors the fusion-identity pattern: prebuilt per-platform
# archives attached to the release; the homebrew formula consumes the
# darwin/linux tarballs and is copied verbatim into
# dahai80/homebrew-fusion-mlx with real sha256s). Default platforms cover
# macOS, Linux (ubuntu) and Windows.
#
# Usage: scripts/package-release.sh <version>   e.g. 0.0.13
set -euo pipefail
cd "$(dirname "$0")/.."

version="${1:?usage: package-release.sh <version>}"
dist="dist"
mkdir -p "$dist"

binaries=(fusion-ops fusion-ops-job fusion-ops-probe fusion-broker migrate)

for platform in darwin/arm64 darwin/amd64 linux/arm64 linux/amd64 windows/arm64 windows/amd64; do
    os="${platform%/*}"
    arch="${platform#*/}"
    out="fusion-ops-${version}-${os}-${arch}"
    echo "package: building $out"
    work="$dist/$out"
    rm -rf "$work" && mkdir -p "$work"
    for b in "${binaries[@]}"; do
        exe="$b"
        [ "$os" = "windows" ] && exe="$b.exe"
        GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build -o "$work/$exe" "./cmd/$b"
    done
    if [ "$os" = "windows" ]; then
        (cd "$work" && zip -q "$OLDPWD/$dist/$out.zip" ./*)
    else
        tar -czf "$dist/$out.tar.gz" -C "$work" .
    fi
    rm -rf "$work"
    shasum -a 256 "$dist/$out".* | sed "s|$dist/||"
done
echo "package: archives in $dist/"
