#!/usr/bin/env bash
# The one command that verifies the whole repository. CI runs the same steps.
#
#   scripts/check.sh            everything
#   scripts/check.sh go|py|ts|demos|drift   one part
set -euo pipefail
cd "$(dirname "$0")/.."
export MESHCALL_ROUTER_BINARY="$PWD/meshcall-router/bin/meshcall-router"
export MESHCALL_RUN_INTEROP=1
part=${1:-all}

step() { printf '\n==> %s\n' "$*"; }

router() {
  step "build router"
  (cd meshcall-router && CGO_ENABLED=0 go build -trimpath -ldflags="-X main.version=local" -o bin/meshcall-router ./cmd/meshcall-router)
}
go_checks() {
  step "go vet + race tests"
  (cd meshcall-router && test -z "$(gofmt -l .)" && go vet ./... && go test -race -count=1 ./...)
}
py_checks() {
  step "python lint, types, tests (incl. interop)"
  (cd meshcall-py && uv sync --frozen --quiet && uv run ruff check src tests ../demo && uv run ruff format --check src tests \
    && uv run pyright src tests ../demo && uv run pytest -q)
}
ts_checks() {
  step "typescript types + tests"
  (cd meshcall-ts && yarn -s install --frozen-lockfile && yarn -s check && yarn -s test 2>&1 | grep -vE "DeprecationWarning|trace-deprecation")
}
demos() {
  step "demos end to end"
  for d in quickstart py2ts showcase router ts2py; do
    printf '%s... ' "$d"
    if [ "$d" = router ]; then
      (cd demo/$d && env -u MESHCALL_ROUTER_BINARY uv run --locked --reinstall-package meshcall demo >/dev/null)
    else
      (cd demo/$d && uv run --locked --reinstall-package meshcall demo >/dev/null)
    fi
    echo ok
  done
}
drift() {
  step "generated clients match their generators"
  scripts/regenerate.sh --check
}

case $part in
  all)   router; go_checks; py_checks; ts_checks; demos; drift ;;
  go)    router; go_checks ;;
  py)    router; py_checks ;;
  ts)    router; ts_checks ;;
  demos) router; demos ;;
  drift) router; drift ;;
  *) echo "usage: scripts/check.sh [all|go|py|ts|demos|drift]" >&2; exit 2 ;;
esac
printf '\nall checks passed\n'
