#!/usr/bin/env bash
# Build the SDK packages that carry the Go binaries, from GoReleaser's dist/.
#
#   scripts/release/bundle.sh <goreleaser-dist-dir> [python|npm]
#
# Python: one platform wheel per release archive (plus the pure wheel and the
# sdist from `uv build`) into meshcall-py/dist. npm: one platform package per
# archive into meshcall-ts/dist-npm/<name>/, and meshcall-ts/package.json gains
# optionalDependencies on them at the exact version, so `npm install meshcall`
# pulls the right binaries and the runtime finds them without a download.
set -euo pipefail
cd "$(dirname "$0")/../.."
DIST=${1:?goreleaser dist dir}
TARGETS=${2:-python npm}
VERSION=$(cd meshcall-py && uv version --short)

wheel_tag() { # <os> <arch>
  case "$1/$2" in
    linux/amd64)   echo manylinux_2_17_x86_64.manylinux2014_x86_64 ;;
    linux/arm64)   echo manylinux_2_17_aarch64.manylinux2014_aarch64 ;;
    darwin/amd64)  echo macosx_11_0_x86_64 ;;
    darwin/arm64)  echo macosx_11_0_arm64 ;;
    windows/amd64) echo win_amd64 ;;
    windows/arm64) echo win_arm64 ;;
    *) echo "unknown platform $1/$2" >&2; return 1 ;;
  esac
}
node_os() { case "$1" in linux) echo linux ;; darwin) echo darwin ;; windows) echo win32 ;; esac; }
node_cpu() { case "$1" in amd64) echo x64 ;; arm64) echo arm64 ;; esac; }

SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT
optional="{"
for archive in "$DIST"/meshcall-router_"$VERSION"_*.tar.gz; do
  name=$(basename "$archive" .tar.gz)
  os=${name#meshcall-router_"$VERSION"_}; arch=${os#*_}; os=${os%_*}
  dir="$SCRATCH/$os-$arch"; mkdir -p "$dir"
  tar -xzf "$archive" -C "$dir"
  if [[ " $TARGETS " == *" python "* ]]; then
    tag=$(wheel_tag "$os" "$arch")
    (cd meshcall-py && MESHCALL_BUNDLE_DIR="$dir" MESHCALL_WHEEL_TAG="$tag" uv build --wheel --out-dir dist)
  fi
  if [[ " $TARGETS " == *" npm "* ]]; then
    pkg="meshcall-router-$(node_os "$os")-$(node_cpu "$arch")"
    out="meshcall-ts/dist-npm/$pkg"; rm -rf "$out"; mkdir -p "$out/bin"
    cp "$dir"/meshcall-router* "$dir"/meshcall "$dir"/meshcall.exe "$out/bin/" 2>/dev/null || true
    cp LICENSE "$out/"
    cat >"$out/package.json" <<JSON
{
  "name": "$pkg",
  "version": "$VERSION",
  "description": "MeshCall Router and CLI binaries for $(node_os "$os")/$(node_cpu "$arch"); installed by the meshcall package",
  "license": "MIT",
  "repository": "github:t4wefan/meshcall",
  "os": ["$(node_os "$os")"],
  "cpu": ["$(node_cpu "$arch")"],
  "files": ["bin"]
}
JSON
    printf '# %s\n\nBinaries for the `meshcall` npm package. Do not depend on this package directly.\n' "$pkg" >"$out/README.md"
    optional="$optional\"$pkg\": \"$VERSION\", "
  fi
done
if [[ " $TARGETS " == *" python "* ]]; then
  (cd meshcall-py && uv build --out-dir dist)   # pure wheel + sdist as the fallback
  ls meshcall-py/dist
fi
if [[ " $TARGETS " == *" npm "* ]]; then
  optional="${optional%, }}"
  node -e '
    const fs = require("fs"); const p = "meshcall-ts/package.json";
    const pkg = JSON.parse(fs.readFileSync(p, "utf8"));
    pkg.optionalDependencies = JSON.parse(process.argv[1]);
    fs.writeFileSync(p, JSON.stringify(pkg, null, 2) + "\n");
  ' "$optional"
  ls meshcall-ts/dist-npm
fi
