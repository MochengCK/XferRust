**摘要**

- 新增内置 MCP 服务：本机 AI Agent 可用标准 MCP 协议添加、查看与控制下载任务
- 新增 `xferrust mcp` 入口：宿主应用没运行时也能下载，已运行时把请求转给正在跑的引擎
- 命令行界面新增帮助浮层与任务搜索，并且同样可以对外提供 MCP 服务
- MCP 只监听回环地址、鉴权复用 RPC 密钥，默认关闭

## 新特性

### MCP 服务（`--mcp-listen-port`、`POST /mcp`）

- 引擎可在独立端口提供 MCP 服务（JSON-RPC 2.0，Streamable HTTP），只监听 `127.0.0.1`，默认不启用。
- 鉴权与 RPC 共用 `rpc-secret`：客户端发送 `Authorization: Bearer <secret>`；未配置密钥时不校验。
- 提供 13 个工具：`add_url`、`add_magnet`、`add_torrent`、`list_tasks`、`get_task`、`get_task_files`、`pause_task`、`resume_task`、`remove_task`、`get_stats`、`get_engine_info`、`get_options`、`set_options`。
- 工具落到既有原生协议上，任务语义与 RPC 完全一致；工具级失败以 `isError` 返回，不中断会话。
- `engine.getVersion.features` 新增 `mcp`，客户端据此判断当前二进制是否支持。

### stdio 入口（`xferrust mcp`）

- 客户端以子进程方式拉起 `xferrust mcp`，即可通过标准输入输出使用 MCP，适配只支持 stdio 的客户端。
- 本机已有引擎在提供 MCP 时，请求直接转给它，任务与宿主界面共享。
- 检测不到引擎时在进程内直接启动引擎：会话写在 `~/.xfer/mcp/session.json`，下载目录默认 `~/Downloads`（`--dir` 可改），因此不启动宿主应用也能下载。
- 独立模式下回环地址默认绕过系统代理，本机与内网地址不会被代理拦截。

### 命令行界面（TUI）

- `?` 打开帮助浮层，按当前视图（任务列表 / 任务详情 / 设置）列出全部快捷键。
- `/` 搜索任务：按文件名或 gid 过滤，大小写不敏感；Enter 保留关键字，Esc 先清搜索再退出。
- `xfer tui --mcp-listen-port=<端口>` 时同样对外提供 MCP，帮助浮层中显示端点地址。
- Ctrl-C 在搜索、帮助与输入框中同样可以强制退出。

## 行为变更

- MCP 默认关闭：不传 `--mcp-listen-port` 时不新增任何监听端口。
- `xferrust mcp` 独立模式的会话与宿主引擎分开（`~/.xfer/mcp/session.json`），两边的任务列表互不可见。
- 其余命令行参数与既有 RPC 语义保持不变。
