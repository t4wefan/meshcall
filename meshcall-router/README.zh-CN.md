# meshcall-router

[English](./README.md) | **简体中文**

MeshCall Router 的独立 Go 实现。Python 与 TypeScript SDK 会将该可执行文件作为子进程启动，自身不再重复实现路由逻辑。

## 构建与运行

在仓库根目录下（需 Go 1.26 或更高版本）：

```bash
go -C meshcall-router build -trimpath -o bin/meshcall-router ./cmd/meshcall-router
./meshcall-router/bin/meshcall-router --host 127.0.0.1 --port 8765
```

只有从源码构建时才需要 Go 工具链。每个打标签的 Release 都会发布覆盖 Linux（amd64、arm64、386、armv7、riscv64、ppc64le、s390x）、macOS（amd64、arm64）、Windows（amd64、arm64、386）与 FreeBSD（amd64、arm64）的预编译二进制及 `checksums.txt`，并通过 `scripts/release/bundle.sh` 将相同二进制打包进各语言的 SDK 包中：PyPI 上为每个目标平台提供专属 wheel，npm 上提供 `meshcall` 作为可选依赖引用的 `meshcall-router-<os>-<cpu>` 包。因此安装 SDK 即自动安装二进制。对于没有预编译包的平台，仍可通过下载路径回退：它会将与当前 SDK 版本匹配的 Release 下载至用户缓存（`~/.cache/meshcall/router/<version>/`，遵循 `XDG_CACHE_HOME`），并在解压前校验 SHA-256：

```bash
uv run meshcall router install      # Python
npx meshcall router install         # Node
```

压缩包内含两个并排安装的二进制：`meshcall-router` 与 `meshcall` CLI（提供 `generate`、`export`、`router inspect`、`call`、`init`）。`uv run meshcall …` 与 `npx meshcall …` 是轻量启动器，负责调用该 CLI（首次使用时自动安装），自身仅直接处理 `router install` 与 `router path`。

启动器按以下顺序解析二进制路径：`MESHCALL_ROUTER_BINARY` 或显式传入的 `binary_path` / `binaryPath`；内置二进制（wheel 的 `meshcall/bin/` 或 npm 平台包）；用户级缓存；包含该 SDK 的源码仓库下的 `meshcall-router/bin`；`PATH` 中的 `meshcall-router`。永不搜索当前工作目录。`meshcall router path` 可打印最终解析到的二进制路径。启动器绝不会现场编译或回退到特定语言实现的 Router。

CLI 默认绑定 `127.0.0.1` 与自动分配的端口，绑定成功后在 stdout 打印一行 `router.ready` JSON，诊断日志输出到 stderr。SDK 启动子进程时还会额外传入 `--shutdown-on-stdin-close`，确保父进程意外终止时不会残留孤儿 Router 进程。两个启动器均会等待就绪信号、限制启动/关闭等待时间、回收失败的子进程，并支持在绑定失败后重新启动。

收到 SIGTERM 或 SIGINT 时，Router 先排空再退出：`/readyz` 返回 503，新连接收到 HTTP 503，新调用以可重试的 `unavailable`（`reason: "router_draining"`）失败；空闲连接立即以 WebSocket 1001 关闭，忙碌连接在最后一个调用结束后关闭；超过 `--shutdown-timeout`（默认 `10s`，`0` 表示不等待）仍在进行的调用以 `unavailable`（`reason: "router_shutdown"`）结束，服务实例收到 `call.cancel`。第二次信号跳过等待。由 SDK 启动器托管、通过关闭 stdin 停止的 Router 会立即结束进行中的调用。详见[协议规范：Router 停机](../docs/zh-CN/protocol.md#router-停机router-shutdown)。

`--metrics-listen 127.0.0.1:9464` 在独立监听地址上提供 Prometheus 指标（默认关闭），详见 [Router 指标](../docs/zh-CN/router.md#7-指标metrics)。

在 Linux、macOS 与 FreeBSD 上，本地传输使用 Unix 套接字：用 `--unix-socket /tmp/meshcall.sock` 代替 `--host` / `--port`。已存在的路径绝不会被覆盖；关闭时仅当该套接字文件仍归属当前 Router 实例时才会将其删除。在 Windows 上本地传输改为命名管道：`--pipe meshcall` 监听 `\\.\pipe\meshcall`，仅允许同一用户、SYSTEM 与 Administrators 访问。两个参数在另一类平台上都会被拒绝并给出提示，`router.ready` 中相应报告 `unix_path` 或 `pipe`。客户端与 CLI 分别以 `unix:///tmp/meshcall.sock` 与 `pipe://meshcall` 寻址。

`--deadline-grace`（默认 `500ms`）会在 Router 强制执行超时前叠加到每次调用的 `timeout_ms` 上，使服务实例自身的 `deadline_exceeded` 优先返回，Router 仅在实例停止响应时兜底介入。`--idle-timeout`（默认 `45s`）会关闭在该时长内未发送任何数据的连接；健康的客户端与服务端每 15 秒发送一次协议级 ping 以保持连接活跃。`--breaker-threshold`（默认 `5`）与 `--breaker-cooldown`（默认 `10s`）用于配置按实例维度的熔断器。详见[协议规范](../docs/zh-CN/protocol.md#超时timeouts)。

容量相关参数：

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--max-connections` | `10000` | 同时打开的 WebSocket 连接数上限，超出后新的升级请求收到 `503` 与 `Retry-After`；HTTP 调用不计入 |
| `--max-anonymous-connections` | `1000` | 其中未携带凭据（`anonymous`）的连接上限 |
| `--max-calls-per-connection` | `1000` | 单个连接同时进行的调用数，超出时 `call.open` 以可重试的 `resource_exhausted`（`overloaded`）失败，连接保持；`0` 表示不限 |
| `--max-frame-size` | `64 MiB` | 单帧最大字节数，也是可设置的最大值 |
| `--outbox-size` | `256 MiB` | 为不读取数据的对端排队的字节上限，超出即断开该对端；不得小于 `--max-frame-size` |
| `--shutdown-timeout` | `10s` | 停机时等待进行中调用结束的时长 |

HTTP 请求体在 Router 读取期间必须在 `--idle-timeout` 内持续到达。

HTTP 调用（`/v1/…`，见[协议规范](../docs/zh-CN/protocol.md#http-调用http-calls)）有独立的参数：`--http-default-timeout`（默认 `30s`）、`--http-max-timeout`（默认不设上限）、`--http-max-inflight`（默认 `10000`）与 `--http-cors-origin`（可重复或逗号分隔）。`/healthz` 与 `/readyz` 分别是存活与就绪探针。

## 环境变量

每个参数都可以通过 `MESHCALL_ROUTER_<FLAG>` 设置：全部大写，`-` 换成 `_`（`--idle-timeout` → `MESHCALL_ROUTER_IDLE_TIMEOUT=30s`，`--auth-file` → `MESHCALL_ROUTER_AUTH_FILE`）。命令行参数优先于环境变量；无效值会导致启动失败，错误信息中会指出变量名。`MESHCALL_ROUTER_AUTH` 直接存放鉴权文档本身（即鉴权文件的 JSON 内容），适用于以环境变量注入密钥的平台；与鉴权文件同时设置会报错。由 Python 与 TypeScript 启动器拉起的 Router 继承启动进程的环境变量，因此这些变量同样生效。

## 鉴权

使用 `--auth-file /path/to/router-auth.json`（或 `MESHCALL_ROUTER_AUTH_FILE`，或在 `MESHCALL_ROUTER_AUTH` 中内联文档）启用账户鉴权。关于配置文件格式、受限临时令牌以及 Python/TypeScript 示例，请参阅 [Router 鉴权与管理](../docs/zh-CN/router.md)。未指定鉴权文件时，监听器运行在开发模式，此时 `AuthService` 无法签发令牌。若提供了无效或空的鉴权文件，启动将直接失败。

未携带凭据的调用方是保留用户 `anonymous`（可在鉴权文件中配置，默认没有任何权限）；`--max-anonymous-connections`（默认 `1000`）限制这类连接的数量。`new-api-key ID` 打印一个新的 API Key（仅显示一次）以及应加入鉴权文件 `api_keys` 的条目（其中保存 SHA-256 摘要）。

`hash-password` 从 stdin 读取密码并打印加盐的 PBKDF2-HMAC-SHA256 哈希。如需隐藏回显的交互式密码输入：

```bash
python3 -c 'import getpass,sys; sys.stdout.write(getpass.getpass("Password: "))' \
  | ./meshcall-router/bin/meshcall-router hash-password
```

只需将生成的哈希复制到鉴权文件中。密码与令牌均在 WebSocket HTTP 握手头中发送，绝不出现在 URL 查询字符串或 Router 命令行参数中。跨网络部署时请通过终结 TLS 的反向代理使用 `wss://`；Go 监听器自身仅提供明文 WebSocket。

## Docker

```bash
docker build -t meshcall-router ./meshcall-router
docker run --rm -p 127.0.0.1:8765:8765 \
  --mount type=bind,src=/absolute/path/router-auth.json,dst=/config/auth.json,readonly \
  meshcall-router --host 0.0.0.0 --port 8765 --auth-file /config/auth.json
```

也可以完全通过环境变量配置：

```bash
docker run --rm -p 127.0.0.1:8765:8765 \
  -e MESHCALL_ROUTER_HOST=0.0.0.0 -e MESHCALL_ROUTER_PORT=8765 \
  -e MESHCALL_ROUTER_AUTH="$(cat router-auth.json)" \
  meshcall-router
```

镜像仅包含静态编译的 Go 可执行文件，并以 UID/GID 65532 运行，请确保挂载的鉴权文件对该用户可读。生产环境的 TLS 终结以及多副本部署监管均由该进程外部的基础设施负责。

## 验证

```bash
go -C meshcall-router vet ./...
go -C meshcall-router test -race ./...
```

Go 测试套件覆盖路由所有权、路由清理、身份认证、权限范围委托、过期与吊销、帧公平调度、粘性路由规范化以及套接字清理。Python/TypeScript 测试套件与跨语言互操作测试均使用同一个可执行文件；CI 仅构建一次并将同一产物分发给所有运行时测试任务。
