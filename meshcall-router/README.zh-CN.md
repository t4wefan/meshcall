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

CLI 默认绑定 `127.0.0.1` 与自动分配的端口，绑定成功后在 stdout 打印一行 `router.ready` JSON，诊断日志输出到 stderr，收到 SIGINT/SIGTERM 时停止进程。SDK 启动子进程时还会额外传入 `--shutdown-on-stdin-close`，确保父进程意外终止时不会残留孤儿 Router 进程。两个启动器均会等待就绪信号、限制启动/关闭等待时间、回收失败的子进程，并支持在绑定失败后重新启动。

若使用 Unix 套接字 WebSocket 传输，用 `--unix-socket /tmp/meshcall.sock` 代替 `--host` / `--port`。已存在的路径绝不会被覆盖；关闭时仅当该套接字文件仍归属当前 Router 实例时才会将其删除。

`--deadline-grace`（默认 `500ms`）会在 Router 强制执行超时前叠加到每次调用的 `timeout_ms` 上，使服务实例自身的 `deadline_exceeded` 优先返回，Router 仅在实例停止响应时兜底介入。`--idle-timeout`（默认 `45s`）会关闭在该时长内未发送任何数据的连接；健康的客户端与服务端每 15 秒发送一次协议级 ping 以保持连接活跃。`--breaker-threshold`（默认 `5`）与 `--breaker-cooldown`（默认 `10s`）用于配置按实例维度的熔断器。详见[协议规范](../docs/zh-CN/protocol.md#timeouts)。

## 鉴权

使用 `--auth-file /path/to/router-auth.json` 启用账户鉴权。关于配置文件格式、受限临时令牌以及 Python/TypeScript 示例，请参阅 [Router 鉴权与管理](../docs/zh-CN/router.md)。未指定鉴权文件时，监听器运行在开发模式，此时 `AuthService` 无法签发令牌。若提供了无效或空的鉴权文件，启动将直接失败。

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

镜像仅包含静态编译的 Go 可执行文件，并以 UID/GID 65532 运行，请确保挂载的鉴权文件对该用户可读。生产环境的 TLS 终结以及多副本部署监管均由该进程外部的基础设施负责。

## 验证

```bash
go -C meshcall-router vet ./...
go -C meshcall-router test -race ./...
```

Go 测试套件覆盖路由所有权、路由清理、身份认证、权限范围委托、过期与吊销、帧公平调度、粘性路由规范化以及套接字清理。Python/TypeScript 测试套件与跨语言互操作测试均使用同一个可执行文件；CI 仅构建一次并将同一产物分发给所有运行时测试任务。
