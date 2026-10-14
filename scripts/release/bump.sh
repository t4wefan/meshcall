#!/usr/bin/env bash
# Set the release version everywhere it lives, then tell you how to ship it.
#
#   scripts/release/bump.sh 0.2.0
#
# The three artifacts share one version: the Python package
# (meshcall-py/pyproject.toml), the npm package (meshcall-ts/package.json) and
# the Go binaries (stamped from the git tag by GoReleaser). The release
# workflow refuses a tag that does not match the first two, so they must move
# together. uv lockfiles that record the in-repo package's version are
# refreshed so `uv run --locked` keeps passing (yarn's frozen lockfile does
# not pin a file: dependency's version, so those need nothing), and
# CHANGELOG.md's Unreleased section becomes the new version.
#
# Only X.Y.Z: a pre-release such as 0.2.0-rc.1 is spelled 0.2.0rc1 by Python
# (PEP 440), which the release workflow's tag check, the npm platform
# package names and the Router download URL would all have to translate.
#
# Nothing is committed or tagged; the script prints those commands.
set -euo pipefail
cd "$(dirname "$0")/../.."

NEW=${1:-}
SEMVER='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
if [[ ! $NEW =~ $SEMVER ]]; then
  echo "usage: scripts/release/bump.sh X.Y.Z" >&2
  exit 2
fi
OLD=$(cd meshcall-py && uv version --short)
TS_OLD=$(node -p "require('./meshcall-ts/package.json').version")

fail() { echo "bump: $*" >&2; exit 1; }
[ "$OLD" = "$TS_OLD" ] || fail "Python ($OLD) and npm ($TS_OLD) versions already disagree; fix that first"
[ "$NEW" != "$OLD" ] || fail "already at $NEW"
[ "$(printf '%s\n%s\n' "$OLD" "$NEW" | sort -V | tail -1)" = "$NEW" ] || fail "$NEW is not after $OLD"
git rev-parse -q --verify "refs/tags/v$NEW" >/dev/null && fail "tag v$NEW already exists"
[ -z "$(git status --porcelain)" ] || fail "working tree is not clean"
grep -q '^## \[Unreleased\]' CHANGELOG.md || fail "CHANGELOG.md has no '## [Unreleased]' section"
grep -q "^## \[$NEW\]" CHANGELOG.md && fail "CHANGELOG.md already has a $NEW section"

echo "==> $OLD -> $NEW"

# Python: uv rewrites [project].version and the package's own lock.
(cd meshcall-py && uv version "$NEW" --no-sync >/dev/null)

# npm: version field only; this repo uses yarn, so no package-lock to touch.
(cd meshcall-ts && npm version "$NEW" --no-git-tag-version --allow-same-version >/dev/null)

# Demos depend on the in-repo packages by path; their locks record the
# version, so a stale lock would fail `uv run --locked` / `--frozen-lockfile`.
for lock in demo/*/uv.lock demo/*/*/uv.lock best-practice/uv.lock; do
  [ -f "$lock" ] || continue
  (cd "$(dirname "$lock")" && uv lock --quiet)
done

# CHANGELOG: Unreleased becomes the release; compare links follow.
python3 - "$OLD" "$NEW" "$(date +%Y-%m-%d)" <<'PY'
import pathlib, re, sys
old, new, today = sys.argv[1:]
path = pathlib.Path("CHANGELOG.md")
text = path.read_text()
text = text.replace("## [Unreleased]\n", f"## [Unreleased]\n\n## [{new}] - {today}\n", 1)
repo = "https://github.com/t4wefan/meshcall"
link = re.compile(r"^\[Unreleased\]: .*$", re.M)
unreleased = f"[Unreleased]: {repo}/compare/v{new}...HEAD"
released = f"[{new}]: {repo}/compare/v{old}...v{new}"
if link.search(text):
    text = link.sub(f"{unreleased}\n{released}", text, count=1)
else:
    text = text.rstrip("\n") + f"\n\n{unreleased}\n{released}\n"
path.write_text(text)
PY

echo "==> changed"
git status --short
cat <<EOF

Review the CHANGELOG entry for $NEW (move anything still under Unreleased),
run the checks, then ship:

  scripts/check.sh
  git commit -am "build: release v$NEW"
  git push origin main
  git tag v$NEW
  git push origin v$NEW
EOF
