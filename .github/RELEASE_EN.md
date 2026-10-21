**Summary**

- Built-in MCP service: local AI agents can add, inspect and control downloads over the standard MCP protocol
- New `xferrust mcp` entry point: agents keep working when the host app is not running, and requests are relayed to a running engine when it is
- The command-line UI gains a help overlay and task search, and can serve MCP as well
- MCP listens on loopback only, reuses the RPC secret for authentication, and is off by default

## New Features

### MCP service (`--mcp-listen-port`, `POST /mcp`)

- The engine can expose an MCP service on its own port (JSON-RPC 2.0 over Streamable HTTP), bound to `127.0.0.1` only and disabled by default.
- Authentication reuses `rpc-secret`: clients send `Authorization: Bearer <secret>`; when no secret is configured, requests are not checked.
- 13 tools are available: `add_url`, `add_magnet`, `add_torrent`, `list_tasks`, `get_task`, `get_task_files`, `pause_task`, `resume_task`, `remove_task`, `get_stats`, `get_engine_info`, `get_options`, `set_options`.
- Every tool maps onto the existing native protocol, so task semantics match RPC exactly; tool-level failures are returned with `isError` and do not break the session.
- `engine.getVersion.features` adds `mcp`, so clients can tell whether the current binary supports it.

### stdio entry point (`xferrust mcp`)

- Clients can spawn `xferrust mcp` as a child process and speak MCP over stdin / stdout, which covers clients that only support stdio.
- When a local engine already serves MCP, requests are relayed to it and tasks are shared with the host UI.
- When no engine is found, the process starts one itself: the session lives in `~/.xfer/mcp/session.json` and the download directory defaults to `~/Downloads` (change it with `--dir`), so downloads work without starting the host app.
- In standalone mode loopback addresses bypass the system proxy by default, so local and intranet URLs are never intercepted by a proxy.

### Command-line UI (TUI)

- `?` opens a help overlay listing every shortcut for the current view (task list / task detail / settings).
- `/` searches tasks by file name or gid, case-insensitively; Enter keeps the keyword and Esc clears the search before anything else.
- `xfer tui --mcp-listen-port=<port>` serves MCP as well, and the help overlay shows the endpoint.
- Ctrl-C force-quits from the search box, the help overlay and input dialogs too.

## Behavior Changes

- MCP is off by default: without `--mcp-listen-port` no extra port is opened.
- The standalone `xferrust mcp` session is separate from the host engine (`~/.xfer/mcp/session.json`), so the two task lists are not shared.
- All other command-line arguments and existing RPC semantics are unchanged.
