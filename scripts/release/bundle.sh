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

# Wheel tags per release target. Go binaries are static, so one Linux build
# serves glibc (manylinux) and musl (musllinux) alike: two wheels per arch.
# PyPI has no FreeBSD tag; FreeBSD gets npm packages and the download path.
wheel_tags() { # <os> <arch> → zero or more tags
  case "$1/$2" in
    linux/amd64)   echo manylinux_2_17_x86_64.manylinux2014_x86_64 musllinux_1_2_x86_64 ;;
    linux/arm64)   echo manylinux_2_17_aarch64.manylinux2014_aarch64 musllinux_1_2_aarch64 ;;
    linux/386)     echo manylinux_2_17_i686.manylinux2014_i686 musllinux_1_2_i686 ;;
    linux/armv7)   echo manylinux_2_17_armv7l.manylinux2014_armv7l musllinux_1_2_armv7l ;;
    linux/riscv64) echo manylinux_2_31_riscv64 musllinux_1_2_riscv64 ;;
    linux/ppc64le) echo manylinux_2_17_ppc64le.manylinux2014_ppc64le musllinux_1_2_ppc64le ;;
    linux/s390x)   echo manylinux_2_17_s390x.manylinux2014_s390x musllinux_1_2_s390x ;;
    darwin/amd64)  echo macosx_11_0_x86_64 ;;
    darwin/arm64)  echo macosx_11_0_arm64 ;;
    windows/amd64) echo win_amd64 ;;
    windows/arm64) echo win_arm64 ;;
    windows/386)   echo win32 ;;
    freebsd/*)     ;;
    *) echo "unknown platform $1/$2" >&2; return 1 ;;
  esac
}
node_os() { case "$1" in linux) echo linux ;; darwin) echo darwin ;; windows) echo win32 ;; freebsd) echo freebsd ;; esac; }
node_cpu() {
  case "$1" in
    amd64) echo x64 ;; arm64) echo arm64 ;; 386) echo ia32 ;; armv7) echo arm ;;
    riscv64) echo riscv64 ;; ppc64le) echo ppc64 ;; s390x) echo s390x ;;
  esac
}

SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT
optional="{"
# Archive names end in _<os>_<arch>.tar.gz; the version in between is
# GoReleaser's (a snapshot locally, the tag in a release) and is ignored here.
for archive in "$DIST"/meshcall-router_*_*_*.tar.gz; do
  name=$(basename "$archive" .tar.gz)
  arch=${name##*_}; rest=${name%_*}; os=${rest##*_}
  dir="$SCRATCH/$os-$arch"; mkdir -p "$dir"
  tar -xzf "$archive" -C "$dir"
  if [[ " $TARGETS " == *" python "* ]]; then
    for tag in $(wheel_tags "$os" "$arch"); do
      (cd meshcall-py && MESHCALL_BUNDLE_DIR="$dir" MESHCALL_WHEEL_TAG="$tag" uv build --wheel --out-dir dist)
    done
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
