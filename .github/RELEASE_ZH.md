**摘要**

- 新增内置 MCP 服务：本机 AI Agent 可用标准 MCP 协议添加、查看与控制下载任务
- 新增 `xferrust mcp` 入口：宿主应用没运行时也能下载，已运行时把请求转给正在跑的引擎
- 独立模式沿用宿主应用的下载目录与全局设置，AI 下的任务在应用启动后能在列表里看到
- 应用一启动，独立引擎立即暂停任务并让位（必要时把请求转给应用），不会两边同时写同一份文件
- MCP 新增 7 个局域网互传工具（发现设备、发文件、查看/取消互传任务）
- 命令行界面新增帮助浮层与任务搜索，并且同样可以对外提供 MCP 服务
- MCP 只监听回环地址、鉴权复用 RPC 密钥，默认关闭

## 新特性

### MCP 服务（`--mcp-listen-port`、`POST /mcp`）

- 引擎可在独立端口提供 MCP 服务（JSON-RPC 2.0，Streamable HTTP），只监听 `127.0.0.1`，默认不启用。
- 鉴权与 RPC 共用 `rpc-secret`：客户端发送 `Authorization: Bearer <secret>`；未配置密钥时不校验。
- 提供 20 个工具：下载与任务管理 13 个（`add_url`、`add_magnet`、`add_torrent`、`list_tasks`、`get_task`、`get_task_files`、`pause_task`、`resume_task`、`remove_task`、`get_stats`、`get_engine_info`、`get_options`、`set_options`），局域网互传 7 个（`lan_status`、`lan_set_config`、`lan_peers`、`lan_tasks`、`lan_send`、`lan_cancel`、`lan_remove`）。
- 工具落到既有原生协议上，任务语义与 RPC 完全一致；工具级失败以 `isError` 返回，不中断会话。
- `engine.getVersion.features` 新增 `mcp`，客户端据此判断当前二进制是否支持。

### stdio 入口（`xferrust mcp`）

- 客户端以子进程方式拉起 `xferrust mcp`，即可通过标准输入输出使用 MCP，适配只支持 stdio 的客户端。
- 本机已有引擎在提供 MCP 时，请求直接转给它，任务与宿主界面共享。
- 检测不到引擎时在进程内直接启动引擎，并**沿用宿主应用的配置**：下载目录、并发数、代理等取自 `<userData>/system.json`（`--host-config` 可显式指定数据目录，便携版/自定义安装位置用它），只跳过端口、凭据、日志这类进程身份；找不到那份配置时退回 `~/Downloads`。
- 独立会话写在 `<userData>/mcp/session.json`：宿主应用下次启动时把其中的任务并进自己的会话，AI 下的内容因此能在应用里看到；已导入过的 gid 记在 `mcp/imported.json`，用户删掉的记录不会复活。
- 独立模式下回环地址默认绕过系统代理，本机与内网地址不会被代理拦截。

### 局域网互传工具

- `lan_peers` / `lan_tasks` / `lan_send` / `lan_cancel` / `lan_remove` 与 `lan_status` / `lan_set_config` 覆盖"看设备、发文件、看进度、取消"整条链路。
- 独立进程只在**第一次真正用到互传**时才启动发现服务：平时不占 UDP 41234，也不会让之后启动的宿主应用看不见设备。

### 命令行界面（TUI）

- `?` 打开帮助浮层，按当前视图（任务列表 / 任务详情 / 设置）列出全部快捷键。
- `/` 搜索任务：按文件名或 gid 过滤，大小写不敏感；Enter 保留关键字，Esc 先清搜索再退出。
- `xfer tui --mcp-listen-port=<端口>` 时同样对外提供 MCP，帮助浮层中显示端点地址。
- Ctrl-C 在搜索、帮助与输入框中同样可以强制退出。

## 行为变更

- MCP 默认关闭：不传 `--mcp-listen-port` 时不新增任何监听端口。
- `xferrust mcp` 独立模式的会话写在 `<userData>/mcp/session.json`，宿主应用启动时并入应用会话；两个进程不会同时写同一份会话文件，独立进程里的未完成任务在应用里恢复成暂停态（不会自动重新开始）。
- 应用启动时独立引擎会**立即让位**：暂停全部任务并保存会话、停掉自己的互传服务（让出 UDP 41234），随后把请求转给应用的 MCP（应用启用了 MCP 时）。应用没启用 MCP 时，新建与继续下载会被明确拒绝并提示去开启，避免两个引擎各写一份文件。
- 其余命令行参数与既有 RPC 语义保持不变。
