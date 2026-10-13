#!/usr/bin/env bash
# The one command that verifies the whole repository. CI runs the same steps.
#
#   scripts/check.sh                  everything
#   scripts/check.sh py demos drift   some parts: go | py | ts | demos | drift | conformance
set -euo pipefail
cd "$(dirname "$0")/.."
export MESHCALL_ROUTER_BINARY="$PWD/meshcall-router/bin/meshcall-router"
export MESHCALL_RUN_INTEROP=1
export NODE_OPTIONS="${NODE_OPTIONS:-} --no-deprecation"

step() { printf '\n==> %s\n' "$*"; }

router() {
  step "build router"
  (cd meshcall-router && CGO_ENABLED=0 go build -trimpath -ldflags="-X main.version=local" -o bin/meshcall-router ./cmd/meshcall-router \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-X main.version=local" -o bin/meshcall ./cmd/meshcall)
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
  (cd meshcall-ts && yarn -s install --frozen-lockfile && yarn -s check && yarn -s test)
}
demos() {
  step "demos end to end (MESHCALL_STRICT=1: contract warnings fail)"
  export MESHCALL_STRICT=1
  # yarn keeps stale copies of file: dependencies whose version did not change.
  rm -rf demo/py2ts/ts-client/node_modules/meshcall demo/ts2py/ts-server/node_modules/meshcall best-practice/ts-client/node_modules/meshcall
  local log
  log=$(mktemp)
  for d in quickstart py2ts showcase router ts2py; do
    printf '%s... ' "$d"
    # uv caches builds of path dependencies; force the ones that live in this repo.
    reinstall="--reinstall-package meshcall"
    [ "$d" = ts2py ] && reinstall="$reinstall --reinstall-package meshcall-demo-ts2py-client"
    if [ "$d" = router ]; then
      (cd demo/$d && env -u MESHCALL_ROUTER_BINARY uv run --locked $reinstall demo >"$log" 2>&1) || { echo FAILED; cat "$log"; return 1; }
    else
      (cd demo/$d && uv run --locked $reinstall demo >"$log" 2>&1) || { echo FAILED; cat "$log"; return 1; }
    fi
    echo ok
  done
  unset MESHCALL_STRICT
}
drift() {
  step "generated clients match their generators"
  scripts/regenerate.sh --check
}
conformance() {
  step "protocol conformance (Python and TypeScript fixtures)"
  (cd meshcall-router && go build -o bin/meshcall-conformance ./cmd/meshcall-conformance)
  (cd meshcall-ts && yarn -s install --frozen-lockfile && yarn -s build)
  (cd meshcall-py && uv sync --frozen --quiet)
  printf 'python... '
  meshcall-router/bin/meshcall-conformance --fixture "uv run --project $PWD/meshcall-py --locked python $PWD/meshcall-py/conformance/fixture.py" >"$PWD/.conformance-py.log" 2>&1 \
    && echo ok || { echo FAILED; cat .conformance-py.log; return 1; }
  printf 'typescript... '
  meshcall-router/bin/meshcall-conformance --fixture "node $PWD/meshcall-ts/dist/conformance/fixture.js" >"$PWD/.conformance-ts.log" 2>&1 \
    && echo ok || { echo FAILED; cat .conformance-ts.log; return 1; }
  rm -f .conformance-py.log .conformance-ts.log
}

parts=${*:-all}
router
for part in $parts; do
  case $part in
    all)   go_checks; py_checks; ts_checks; demos; drift; conformance ;;
    go)    go_checks ;;
    py)    py_checks ;;
    ts)    ts_checks ;;
    demos) demos ;;
    drift) drift ;;
    conformance) conformance ;;
    *) echo "usage: scripts/check.sh [go|py|ts|demos|drift|conformance]..." >&2; exit 2 ;;
  esac
done
printf '\nall checks passed\n'
