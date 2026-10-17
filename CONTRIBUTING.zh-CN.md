# 贡献指南 (Contributing)

[English](./CONTRIBUTING.md) | **简体中文**

## 仓库目录结构

| 目录 | 说明 | 工具链 |
| --- | --- | --- |
| `meshcall-py/` | Python 运行时、契约导出器、`meshcall` 启动器 | uv, ruff, pyright, pytest |
| `meshcall-ts/` | TypeScript 运行时、`meshcall` 启动器 | Yarn 1, tsc, `node --test` |
| `meshcall-go/` | Go 运行时（服务定义、客户端、依赖注入、一致性测试固件） | Go 1.26, `go test -race` |
| `meshcall-router/` | Go Router、`meshcall` CLI 与代码生成器、一致性测试运行器 | Go 1.26, `go test -race` |
| `skills/meshcall/` | 由 `meshcall init` 安装的 AI Agent 技能源文件 | — |
| `docs/` | 协议规范、运行时架构、Router 指南与使用手册（含 `docs/zh-CN/` 中文版） | — |
| `demo/`、`best-practice/` | 可运行的示例项目，每个目录均为独立的 uv / Yarn / Go 项目 | — |

`docs/protocol.md` 是三个运行时与 Router 之间的契约规范。任何行为变更请先修改协议文档，再同步修改各语言实现，最后补充一致性测试（conformance）与跨语言互操作测试（interop）。

## 在本地运行全量检查

```bash
scripts/check.sh            # 运行 CI 的全部检查：构建 Router、Go、Python、TypeScript、所有示例与生成代码漂移检测
scripts/check.sh py         # 仅运行指定部分：go | py | ts | demos | drift | conformance
scripts/regenerate.sh       # 重新生成所有入库的客户端代码（以及内嵌的 Agent 技能文件）
```

`make check`、`make regenerate` 与 `make drift` 是对应的快捷别名。脚本会自动设置 `MESHCALL_ROUTER_BINARY` 与 `MESHCALL_RUN_INTEROP=1`；如需单独手动运行某个工具，请参阅 `AGENTS.md` 了解所需导出的环境变量。

CI 会在每次 push 与 pull request 时执行完全相同的检查步骤。

## 开发规范

- **保持三个运行时行为完全一致**：新特性需同时在 Go、Python 与 TypeScript 中落地，并在 `meshcall-router/conformance/scenarios.go` 中添加一致性测试场景；若涉及生成的客户端跨语言调用，还需在 `meshcall-py/tests/test_multilang_interop.py` 中补充测试。
- **统一错误码、帧字段与超时语义**：所有错误码、帧字段与超时规则均在 `docs/protocol.md` 中定义，切勿仅在某一个运行时中自行新增。
- **提交信息格式**：遵循 `type: summary`（`feat`、`fix`、`docs`、`test`、`build`、`refactor`），每个 commit 保持单一逻辑变更。
- **生成代码入库校验**：`demo/` 与 `best-practice/` 下生成的客户端代码包均纳入版本控制；当代码生成器输出或示例契约发生变更时，请运行 `scripts/regenerate.sh` 更新。

## 发布流程

版本发布通过 `main` 分支上的 Git 标签触发：

```bash
# 同步更新 meshcall-py/pyproject.toml、meshcall-ts/package.json 与 CHANGELOG.md 中的版本号，提交后执行：
git tag v0.2.0 && git push origin v0.2.0
```

`release` 工作流（`.github/workflows/release.yml` + `scripts/release/bundle.sh`）会自动完成以下工作：

1. 编译覆盖 14 个目标平台（Linux `amd64`/`arm64`/`386`/`armv7`/`riscv64`/`ppc64le`/`s390x`、macOS `amd64`/`arm64`、Windows `amd64`/`arm64`/`386`、FreeBSD `amd64`/`arm64`）的 `meshcall-router` 与 `meshcall` 二进制，并将 `.tar.gz` 压缩包与 `checksums.txt` 上传至 GitHub Release。
2. 将上述二进制重新打包进各平台专属的 PyPI wheel（Linux 的 `manylinux` 与 `musllinux`、macOS 与 Windows 平台 wheel、纯 Python 兜底 wheel 以及源码包 sdist），并通过 Trusted Publishing（OIDC）发布至 PyPI。
3. 使用 `NPM_TOKEN`（启用绕过 2FA 的细粒度自动化令牌）将 14 个 `meshcall-router-<os>-<cpu>` 二进制包与主包 `meshcall` 发布至 npm。
