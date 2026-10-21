**Summary**

- Built-in MCP service: local AI agents can add, inspect and control downloads over the standard MCP protocol
- New `xferrust mcp` entry point: agents keep working when the host app is not running, and requests are relayed to a running engine when it is
- Standalone mode reuses the host app's download directory and global settings, and its tasks show up in the app after the next launch
- As soon as the app starts, a standalone engine pauses its tasks and yields (relaying to the app when possible), so the two never write the same files
- 7 new MCP tools for LAN transfer (discover devices, send files, inspect or cancel transfers)
- The command-line UI gains a help overlay and task search, and can serve MCP as well
- MCP listens on loopback only, reuses the RPC secret for authentication, and is off by default

## New Features

### MCP service (`--mcp-listen-port`, `POST /mcp`)

- The engine can expose an MCP service on its own port (JSON-RPC 2.0 over Streamable HTTP), bound to `127.0.0.1` only and disabled by default.
- Authentication reuses `rpc-secret`: clients send `Authorization: Bearer <secret>`; when no secret is configured, requests are not checked.
- 20 tools are available: 13 for downloads and task management (`add_url`, `add_magnet`, `add_torrent`, `list_tasks`, `get_task`, `get_task_files`, `pause_task`, `resume_task`, `remove_task`, `get_stats`, `get_engine_info`, `get_options`, `set_options`) and 7 for LAN transfer (`lan_status`, `lan_set_config`, `lan_peers`, `lan_tasks`, `lan_send`, `lan_cancel`, `lan_remove`).
- Every tool maps onto the existing native protocol, so task semantics match RPC exactly; tool-level failures are returned with `isError` and do not break the session.
- `engine.getVersion.features` adds `mcp`, so clients can tell whether the current binary supports it.

### stdio entry point (`xferrust mcp`)

- Clients can spawn `xferrust mcp` as a child process and speak MCP over stdin / stdout, which covers clients that only support stdio.
- When a local engine already serves MCP, requests are relayed to it and tasks are shared with the host UI.
- When no engine is found, the process starts one itself and **reuses the host app's settings**: download directory, concurrency, proxy and the rest come from `<userData>/system.json` (`--host-config` points at that directory explicitly, which portable and custom installs need); only process-identity keys such as ports, credentials and logging are skipped. Without that config it falls back to `~/Downloads`.
- The standalone session lives in `<userData>/mcp/session.json`: the host app merges its tasks into its own session on the next launch, so AI-added downloads appear in the app; imported gids are recorded in `mcp/imported.json`, so tasks the user deleted never come back.
- In standalone mode loopback addresses bypass the system proxy by default, so local and intranet URLs are never intercepted by a proxy.

### LAN transfer tools

- `lan_status` / `lan_set_config` plus `lan_peers` / `lan_tasks` / `lan_send` / `lan_cancel` / `lan_remove` cover the whole flow: inspect devices, send files, watch progress and cancel.
- The standalone process starts the discovery service only when LAN transfer is actually used for the first time: it does not hold UDP 41234, so a host app started later still sees every device.

### Command-line UI (TUI)

- `?` opens a help overlay listing every shortcut for the current view (task list / task detail / settings).
- `/` searches tasks by file name or gid, case-insensitively; Enter keeps the keyword and Esc clears the search before anything else.
- `xfer tui --mcp-listen-port=<port>` serves MCP as well, and the help overlay shows the endpoint.
- Ctrl-C force-quits from the search box, the help overlay and input dialogs too.

## Behavior Changes

- MCP is off by default: without `--mcp-listen-port` no extra port is opened.
- The standalone `xferrust mcp` session lives in `<userData>/mcp/session.json` and is merged into the app session when the host app starts; the two processes never write the same session file, and unfinished standalone tasks come back as paused in the app (they do not restart on their own).
- When the host app starts, a standalone engine **yields immediately**: it pauses every task, saves the session, stops its own LAN service (releasing UDP 41234) and then relays requests to the app's MCP when that is enabled. Without MCP enabled, new downloads and resumes are rejected with a hint to turn it on, so the two engines never write the same files.
- All other command-line arguments and existing RPC semantics are unchanged.
