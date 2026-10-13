# Contributing

## Layout

| Directory | What | Toolchain |
| --- | --- | --- |
| `meshcall-py/` | Python runtime and `meshcall` CLI | uv, ruff, pyright, pytest |
| `meshcall-ts/` | TypeScript runtime | Yarn 1, tsc, `node --test` |
| `meshcall-router/` | Go Router | Go 1.26, `go test -race` |
| `docs/` | Protocol and architecture | — |
| `demo/`, `best-practice/` | Runnable examples, each its own uv project | — |

`docs/protocol.md` is the contract between the three implementations. Change
it first, then the implementations, then the interop tests.

## Running everything locally

```bash
scripts/check.sh            # everything CI runs: Router build, Go, Python, TypeScript, demos, drift
scripts/check.sh py         # one part: go | py | ts | demos | drift
scripts/regenerate.sh       # rewrite every checked-in generated client
```

`make check`, `make regenerate` and `make drift` are aliases. The script sets
`MESHCALL_ROUTER_BINARY` and `MESHCALL_RUN_INTEROP=1`; see `AGENTS.md` for
what to export when running a single tool by hand.

CI runs the same steps on every push and pull request.

## Conventions

- Keep the three runtimes behaviourally identical. A feature lands in Go,
  Python and TypeScript together, with a cross-language test in
  `meshcall-py/tests/test_multilang_interop.py` when it crosses the wire.
- Error codes, frame fields and timeouts are specified in `docs/protocol.md`;
  do not invent new ones in one runtime only.
- Commit messages follow `type: summary` (`feat`, `fix`, `docs`, `test`,
  `build`, `refactor`), one logical change per commit.
- Generated client packages under `demo/` and `best-practice/` are checked in;
  regenerate them when codegen output changes.

## Releasing

Releases are cut from a tag on `main`:

```bash
# bump the version in meshcall-py/pyproject.toml, meshcall-ts/package.json
# and CHANGELOG.md, commit, then
git tag v0.2.0 && git push origin v0.2.0
```

The `release` workflow builds Router binaries for Linux, macOS and Windows
(amd64 and arm64) with GoReleaser and attaches them plus `checksums.txt` to a
GitHub release, then publishes the Python package to PyPI and the npm package
with provenance. Both registries use trusted publishing (OIDC): configure the
`pypi` and `npm` environments once in the repository settings and register the
workflow as a trusted publisher on each registry. No long-lived tokens are
stored.
