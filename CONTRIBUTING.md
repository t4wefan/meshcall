# Contributing

**English** | [简体中文](./CONTRIBUTING.zh-CN.md)

## Layout

| Directory | What | Toolchain |
| --- | --- | --- |
| `meshcall-py/` | Python runtime, contract exporter, `meshcall` launcher | uv, ruff, pyright, pytest |
| `meshcall-ts/` | TypeScript runtime, `meshcall` launcher | Yarn 1, tsc, `node --test` |
| `meshcall-go/` | Go runtime (services, clients, DI, conformance fixture) | Go 1.26, `go test -race` |
| `meshcall-router/` | Go Router, `meshcall` CLI & codegen, conformance runner | Go 1.26, `go test -race` |
| `skills/meshcall/` | Agent skill installed by `meshcall init` | — |
| `docs/` | Protocol, architecture, Router and user guide | — |
| `demo/`, `best-practice/` | Runnable examples, each its own uv / Yarn / Go project | — |

`docs/protocol.md` is the contract between the three runtimes and the Router. Change
it first, then the implementations, then the conformance and interop tests.

## Running everything locally

```bash
scripts/check.sh            # everything CI runs: Router build, Go, Python, TypeScript, demos, drift
scripts/check.sh py         # one part: go | py | ts | demos | drift | conformance
scripts/regenerate.sh       # rewrite every checked-in generated client (and embedded skill)
```

`make check`, `make regenerate` and `make drift` are aliases. The script sets
`MESHCALL_ROUTER_BINARY` and `MESHCALL_RUN_INTEROP=1`; see `AGENTS.md` for
what to export when running a single tool by hand.

CI runs the same steps on every push and pull request.

## Conventions

- Keep the three runtimes behaviourally identical. A feature lands in Go,
  Python and TypeScript together, with a scenario in
  `meshcall-router/conformance/scenarios.go` and a cross-language test in
  `meshcall-py/tests/test_multilang_interop.py` when it crosses generated
  clients.
- Error codes, frame fields and timeouts are specified in `docs/protocol.md`;
  do not invent new ones in one runtime only.
- Commit messages follow `type: summary` (`feat`, `fix`, `docs`, `test`,
  `build`, `refactor`), one logical change per commit.
- Generated client packages under `demo/` and `best-practice/` are checked in;
  run `scripts/regenerate.sh` when codegen output or demo contracts change.

## Releasing

Releases are cut from a tag on `main`:

```bash
# bump the version in meshcall-py/pyproject.toml, meshcall-ts/package.json
# and CHANGELOG.md, commit, then
git tag v0.2.0 && git push origin v0.2.0
```

The `release` workflow (`.github/workflows/release.yml` + `scripts/release/bundle.sh`):

1. Builds `meshcall-router` and `meshcall` binaries across 14 targets (Linux
   `amd64`/`arm64`/`386`/`armv7`/`riscv64`/`ppc64le`/`s390x`, macOS
   `amd64`/`arm64`, Windows `amd64`/`arm64`/`386`, FreeBSD `amd64`/`arm64`)
   and attaches `.tar.gz` archives plus `checksums.txt` to the GitHub Release.
2. Repacks those binaries into platform-specific PyPI wheels (`manylinux` and
   `musllinux` on Linux, plus macOS and Windows wheels, the pure fallback
   wheel, and sdist) and publishes to PyPI via Trusted Publishing (OIDC).
3. Publishes the 14 `meshcall-router-<os>-<cpu>` binary packages and the root
   `meshcall` package to npm with `NPM_TOKEN` (granular automation token with
   2FA bypass).
