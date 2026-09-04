//! xfer：XferRust 引擎命令行界面。
//!
//! 子命令：
//! - `xfer` / `xfer tui [-d 目录] [-j 并发]`  可视化主界面
//!   （任务列表 / 添加下载 / 暂停恢复移除 / 任务详情 / 设置）。
//! - `xfer download <url> [-d 目录] [-o 文件名] [--checksum 算法=摘要]`
//!   进程内引擎直接下载，可视化 TUI 实时进度（q 取消）。
//! - `xfer daemon [--k=v ...]`  启动 RPC 守护进程（同 xferrust 参数）。
//! - `xfer add/tell/list/pause/resume/remove/stat`  通过原生 WS RPC
//!   操作运行中的守护进程（--connect 指定地址，--token 指定密钥）。

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde_json::{json, Value};
use xfer_engine::TaskManager;
use xfer_types::{Gid, ENGINE_NAME, ENGINE_VERSION};

const DEFAULT_RPC: &str = "ws://127.0.0.1:6800/jsonrpc";

// ----------------------------------------------------------------------
// i18n：界面语言（XFER_LANG=zh/en/zh_tw 启动指定；TUI 设置页内切换并持久化）
// ----------------------------------------------------------------------

/// 界面语言。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lang {
    /// 简体中文（默认）。
    Zh,
    /// English。
    En,
    /// 繁体中文。
    ZhTw,
}

/// 全局界面语言：0 = 简体，1 = English，2 = 繁体。
static LANG: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

fn lang() -> Lang {
    match LANG.load(std::sync::atomic::Ordering::Relaxed) {
        1 => Lang::En,
        2 => Lang::ZhTw,
        _ => Lang::Zh,
    }
}

fn set_lang(l: Lang) {
    LANG.store(l as u8, std::sync::atomic::Ordering::Relaxed);
}

/// 语言在界面里的自称（设置页「界面语言」行显示）。
fn lang_display_name(l: Lang) -> &'static str {
    match l {
        Lang::Zh => "简体中文",
        Lang::En => "English",
        Lang::ZhTw => "繁體中文",
    }
}

/// 取界面文案：`tr("中文", "English")`。
///
/// 繁体中文模式下返回简体文案的简→繁转换结果（界面用字经
/// `s2t_char` 映射，未收录的字符原样保留）。
fn tr(zh: &'static str, en: &'static str) -> String {
    match lang() {
        Lang::Zh => zh.to_string(),
        Lang::En => en.to_string(),
        Lang::ZhTw => s2t(zh),
    }
}

/// 纯函数版本（测试用，不触碰全局状态）。
#[cfg(test)]
fn tr_for(l: Lang, zh: &'static str, en: &'static str) -> &'static str {
    match l {
        Lang::Zh => zh,
        Lang::En => en,
        Lang::ZhTw => en, // 繁体为动态转换，纯函数无法给出；En 占位仅用于非繁体断言
    }
}

/// 简→繁：对简体文案逐字转换。
fn s2t(s: &str) -> String {
    s.chars().map(s2t_char).collect()
}

/// 单个简体字 → 繁体字（覆盖界面文案中的简繁异形字；同形字原样返回）。
fn s2t_char(c: char) -> char {
    match c {
        '两' => '兩', '严' => '嚴', '个' => '個', '临' => '臨', '为' => '為',
        '义' => '義', '仪' => '儀', '仅' => '僅', '从' => '從', '优' => '優',
        '会' => '會', '传' => '傳', '体' => '體', '余' => '餘', '侧' => '側',
        '储' => '儲', '内' => '內', '写' => '寫', '冲' => '衝', '准' => '準',
        '则' => '則', '刚' => '剛', '创' => '創', '删' => '刪', '别' => '別',
        '务' => '務', '动' => '動', '区' => '區', '协' => '協', '单' => '單',
        '占' => '佔', '卫' => '衛', '历' => '歷', '压' => '壓', '参' => '參',
        '发' => '發', '变' => '變', '叠' => '疊', '台' => '臺', '号' => '號',
        '后' => '後', '听' => '聽', '启' => '啟', '响' => '響', '唤' => '喚',
        '围' => '圍', '国' => '國', '图' => '圖', '圆' => '圓', '块' => '塊',
        '处' => '處', '备' => '備', '复' => '復', '头' => '頭', '实' => '實',
        '宽' => '寬', '对' => '對', '导' => '導', '将' => '將', '尽' => '盡',
        '层' => '層', '属' => '屬', '带' => '帶', '帧' => '幀', '并' => '並',
        '库' => '庫', '应' => '應', '开' => '開', '弹' => '彈', '强' => '強',
        '归' => '歸', '当' => '當', '录' => '錄', '径' => '徑', '态' => '態',
        '总' => '總', '恒' => '恆', '懒' => '懶', '执' => '執', '护' => '護',
        '择' => '擇', '换' => '換', '据' => '據', '摆' => '擺', '撑' => '撐',
        '数' => '數', '断' => '斷', '无' => '無', '旧' => '舊', '时' => '時',
        '显' => '顯', '暂' => '暫', '杀' => '殺', '条' => '條', '来' => '來',
        '构' => '構', '标' => '標', '栏' => '欄', '样' => '樣', '横' => '橫',
        '残' => '殘', '汉' => '漢', '测' => '測', '滚' => '滾', '满' => '滿',
        '滤' => '濾', '点' => '點', '状' => '狀', '独' => '獨', '环' => '環',
        '现' => '現', '画' => '畫', '监' => '監', '盖' => '蓋', '盘' => '盤',
        '确' => '確', '离' => '離', '种' => '種', '积' => '積', '称' => '稱',
        '稳' => '穩', '竖' => '豎', '签' => '簽', '简' => '簡', '类' => '類',
        '约' => '約', '级' => '級', '纯' => '純', '纵' => '縱', '线' => '線',
        '终' => '終', '经' => '經', '结' => '結', '绘' => '繪', '给' => '給',
        '统' => '統', '继' => '繼', '绪' => '緒', '续' => '續', '缀' => '綴',
        '缓' => '緩', '编' => '編', '节' => '節', '范' => '範', '获' => '獲',
        '补' => '補', '见' => '見', '规' => '規', '视' => '視', '触' => '觸',
        '计' => '計', '订' => '訂', '认' => '認', '议' => '議', '记' => '記',
        '设' => '設', '证' => '證', '识' => '識', '试' => '試', '话' => '話',
        '询' => '詢', '该' => '該', '详' => '詳', '语' => '語', '误' => '誤',
        '说' => '說', '请' => '請', '读' => '讀', '调' => '調', '负' => '負',
        '责' => '責', '败' => '敗', '费' => '費', '车' => '車', '轨' => '軌',
        '转' => '轉', '轮' => '輪', '轻' => '輕', '载' => '載', '辑' => '輯',
        '输' => '輸', '边' => '邊', '过' => '過', '运' => '運', '进' => '進',
        '远' => '遠', '连' => '連', '适' => '適', '选' => '選', '里' => '裡',
        '钥' => '鑰', '钳' => '鉗', '链' => '鏈', '锁' => '鎖', '错' => '錯',
        '键' => '鍵', '长' => '長', '闭' => '閉', '间' => '間', '阅' => '閱',
        '际' => '際', '随' => '隨', '隐' => '隱', '页' => '頁', '顶' => '頂',
        '项' => '項', '顺' => '順', '须' => '須', '预' => '預', '题' => '題',
        '馈' => '饋', '黄' => '黃', '齐' => '齊',
        _ => c,
    }
}

/// 从 XFER_LANG 环境变量初始化界面语言
/// （zh/zh_cn → 简体，en/english → 英文，zh_tw/zh-hant/traditional → 繁体）。
fn init_lang_from_env() {
    let l = std::env::var("XFER_LANG")
        .map(|v| match v.trim().to_ascii_lowercase().as_str() {
            "en" | "en-us" | "english" => Lang::En,
            "zh-tw" | "zh_tw" | "zh-hant" | "traditional" | "繁体" => Lang::ZhTw,
            _ => Lang::Zh,
        })
        .unwrap_or(Lang::Zh);
    set_lang(l);
}

fn main() {
    init_lang_from_env();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match args.first().map(String::as_str) {
        Some("tui") => cmd_tui(&args[1..]),
        Some("download") | Some("dl") => cmd_download(&args[1..]),
        Some("daemon") => cmd_daemon(&args[1..]),
        Some("add") => cmd_add(&args[1..]),
        Some("tell") => cmd_tell(&args[1..]),
        Some("list") | Some("ls") => cmd_list(&args[1..]),
        Some("pause") | Some("resume") | Some("remove") | Some("rm") => {
            cmd_task_action(&args[0], &args[1..])
        }
        Some("stat") => cmd_stat(&args[1..]),
        Some("--version") | Some("-V") => {
            println!("xfer version {ENGINE_VERSION}");
            0
        }
        Some("help") | Some("--help") | Some("-h") => {
            print_usage();
            0
        }
        None => cmd_tui(&[]),
        Some(other) if other.starts_with("http://") || other.starts_with("https://") => {
            cmd_download(&args[..])
        }
        Some(other) => {
            eprintln!("{}: {other}", tr("未知子命令", "unknown subcommand"));
            print_usage();
            2
        }
    };
    std::process::exit(code);
}

fn print_usage() {
    let usage = tr(
        concat!(
            "{ENGINE_NAME} 下载引擎 v{ENGINE_VERSION}\n",
            "\n",
            "用法:\n",
            "  xfer [tui] [-d 目录] [-j 并发]      可视化主界面（任务管理 + 设置，无参数启动同 tui）\n",
            "  xfer download <url> [-d 目录] [-o 文件名] [--checksum 算法=摘要]\n",
            "  xfer <url> ...                     同 download\n",
            "  xfer daemon [--k=v ...]            启动 RPC 守护进程\n",
            "  xfer add <url> [-d 目录] [-o 文件名] [--connect 地址] [--token 密钥]\n",
            "  xfer tell <gid> [--connect 地址]   查看任务详情\n",
            "  xfer list [--scope all|active|waiting|stopped]\n",
            "  xfer pause|resume|remove <gid>\n",
            "  xfer stat                          全局统计\n",
            "\n",
            "默认 RPC 地址: {DEFAULT_RPC}"
        ),
        concat!(
            "{ENGINE_NAME} download engine v{ENGINE_VERSION}\n",
            "\n",
            "Usage:\n",
            "  xfer [tui] [-d dir] [-j jobs]      visual UI (task manager + settings; bare xfer = tui)\n",
            "  xfer download <url> [-d dir] [-o name] [--checksum alg=digest]\n",
            "  xfer <url> ...                     same as download\n",
            "  xfer daemon [--k=v ...]            start RPC daemon\n",
            "  xfer add <url> [-d dir] [-o name] [--connect addr] [--token secret]\n",
            "  xfer tell <gid> [--connect addr]   show task details\n",
            "  xfer list [--scope all|active|waiting|stopped]\n",
            "  xfer pause|resume|remove <gid>\n",
            "  xfer stat                          global stats\n",
            "\n",
            "Default RPC: {DEFAULT_RPC}"
        ),
    )
    .replace("{ENGINE_NAME}", ENGINE_NAME)
    .replace("{ENGINE_VERSION}", ENGINE_VERSION)
    .replace("{DEFAULT_RPC}", DEFAULT_RPC);
    println!("{usage}");
}

// ----------------------------------------------------------------------
// download：进程内引擎 + TUI
// ----------------------------------------------------------------------

fn cmd_download(args: &[String]) -> i32 {
    let Some(url) = positional(args, 0) else {
        eprintln!("{}", tr("缺少下载地址", "missing download URL"));
        return 2;
    };
    let dir = flag_value(args, "-d")
        .or_else(|| flag_value(args, "--dir"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let out = flag_value(args, "-o").or_else(|| flag_value(args, "--out"));
    let checksum = flag_value(args, "--checksum");

    let rt = tokio::runtime::Runtime::new().expect("创建 runtime 失败");
    rt.block_on(async move {
        let mgr = TaskManager::start(dir, 1);
        let mut events = mgr.events().subscribe();
        let mut options = serde_json::Map::new();
        if let Some(o) = out {
            options.insert("out".into(), json!(o));
        }
        if let Some(c) = checksum {
            options.insert("checksum".into(), json!(c));
        }
        let gid = match mgr.add_uri(vec![url], &Value::Object(options), None) {
            Ok(g) => g,
            Err(e) => {
                eprintln!("{}: {e}", tr("添加任务失败", "Failed to add task"));
                return 1;
            }
        };
        tui_loop(mgr, gid, &mut events).await
    })
}

/// TUI 状态：最新状态快照 + 速度历史。
struct TuiState {
    last: Value,
    hist: Vec<u64>,
}

const HIST_MAX: usize = 120; // 300ms × 120 = 最近 60s 速度历史

/// TUI 主循环：订阅进度事件渲染，q/ESC/Ctrl-C 取消。
async fn tui_loop(
    mgr: Arc<TaskManager>,
    gid: Gid,
    events: &mut tokio::sync::broadcast::Receiver<(String, String)>,
) -> i32 {
    use crossterm::{cursor, execute, terminal};
    let mut term = {
        let mut stdout = std::io::stdout();
        let _ = execute!(
            stdout,
            terminal::EnterAlternateScreen,
            terminal::Clear(terminal::ClearType::All),
            // 清除滚动缓冲区：向上滑动不再看到 TUI 启动前的终端输出
            terminal::Clear(terminal::ClearType::Purge),
            cursor::Hide
        );
        let _ = terminal::enable_raw_mode();
        ratatui::Terminal::new(ratatui::backend::CrosstermBackend::new(stdout))
            .expect("TUI 初始化失败")
    };

    let mut state = TuiState {
        last: json!({}),
        hist: Vec::new(),
    };

    let mut last_render = std::time::Instant::now() - Duration::from_secs(1);
    loop {
        // 键盘事件（非阻塞轮询）
        if crossterm::event::poll(Duration::from_millis(50)).unwrap_or(false) {
            if let Ok(crossterm::event::Event::Key(k)) = crossterm::event::read() {
                use crossterm::event::{KeyCode, KeyModifiers};
                let quit = matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)
                    || (k.code == KeyCode::Char('c')
                        && k.modifiers.contains(KeyModifiers::CONTROL));
                if quit {
                    let _ = mgr.remove(&gid);
                    restore(&mut term);
                    eprintln!("{}（gid {}）", tr("已取消", "cancelled"), gid.0);
                    return 130;
                }
            }
        }
        // 引擎事件
        while let Ok(ev) = events.try_recv() {
            if ev.1 == gid.0 && matches!(ev.0.as_str(), "complete" | "error" | "stop") {
                refresh(&mgr, &gid, &mut state);
                let _ = term.draw(|f| {
                    draw(
                        f,
                        &state,
                        &gid,
                        &tr(" q / ESC / Ctrl-C 取消 · 完成后自动退出 ", " q / ESC / Ctrl-C cancel · auto-exit when done "),
                    )
                });
                let exit_code = if ev.0 == "complete" { 0 } else { 1 };
                restore(&mut term);
                finish(&mgr, &gid, exit_code);
                return exit_code;
            }
        }
        // 定时刷新（进度事件 1Hz，UI 每 300ms 平滑一次）
        if last_render.elapsed() >= Duration::from_millis(300) {
            refresh(&mgr, &gid, &mut state);
            let _ = term.draw(|f| {
                draw(
                    f,
                    &state,
                    &gid,
                    &tr(" q / ESC / Ctrl-C 取消 · 完成后自动退出 ", " q / ESC / Ctrl-C cancel · auto-exit when done "),
                )
            });
            last_render = std::time::Instant::now();
        }
    }
}

/// 拉取一次状态快照，追加速度历史。
fn refresh(mgr: &Arc<TaskManager>, gid: &Gid, state: &mut TuiState) {
    if let Ok(v) = mgr.tell_status_native(gid, None) {
        let speed = v["downloadSpeed"].as_u64().unwrap_or(0);
        state.hist.push(speed);
        if state.hist.len() > HIST_MAX {
            let drop = state.hist.len() - HIST_MAX;
            state.hist.drain(0..drop);
        }
        state.last = v;
    }
}

/// 恢复终端（备用屏退出、光标显示、raw mode 关闭）。
fn restore(term: &mut ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>) {
    use crossterm::{cursor, execute, terminal};
    let _ = terminal::disable_raw_mode();
    let _ = execute!(
        term.backend_mut(),
        terminal::LeaveAlternateScreen,
        cursor::Show
    );
}

/// 渲染一帧：任务信息 / 进度仪表 / 实时速度图 / 底栏提示。
fn draw(f: &mut ratatui::Frame, st: &TuiState, gid: &Gid, footer: &str) {
    draw_in_area(f, st, gid, footer, f.area(), true);
}

/// 在指定区域内渲染（`draw` 的子区域版本，供 BT 详情视图分屏使用）。
fn draw_in_area(
    f: &mut ratatui::Frame,
    st: &TuiState,
    gid: &Gid,
    footer: &str,
    area: ratatui::layout::Rect,
    show_footer: bool,
) {
    use ratatui::layout::{Alignment, Constraint, Layout};
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Padding, Paragraph, Sparkline, Wrap};

    let v = &st.last;
    let status = v["status"].as_str().unwrap_or("-");
    let completed = v["completedLength"].as_u64().unwrap_or(0);
    let total = v["totalLength"].as_u64().unwrap_or(0);
    let speed = v["downloadSpeed"].as_u64().unwrap_or(0);
    let conns = v["connections"].as_u64().unwrap_or(0);
    let upload_speed = v["uploadSpeed"].as_u64().unwrap_or(0);
    // numPieces > 0 表示 BT 任务且元数据就绪（磁力取到元信息后才有片布局）
    let is_bt = v["numPieces"].as_u64().unwrap_or(0) > 0;
    let path = v["files"][0]["path"].as_str().unwrap_or("-");
    let dir = v["dir"].as_str().unwrap_or("-");

    let pct = if total > 0 {
        (completed as f64 / total as f64).min(1.0)
    } else {
        0.0
    };
    let eta = if total > completed && speed > 0 {
        fmt_duration((total - completed) / speed)
    } else {
        "--:--".to_string()
    };
    let peak = st.hist.iter().copied().max().unwrap_or(0);

    let dim = ui_dim();
    let accent = ui_accent();

    let mut constraints = vec![
        Constraint::Length(6), // 任务信息
        Constraint::Length(7), // 进度仪表
        Constraint::Min(7),    // 速度图
    ];
    if show_footer {
        constraints.push(Constraint::Length(1)); // 快捷键
    }
    let rows = Layout::vertical(constraints).split(area);

    // 任务信息
    let mut line1 = vec![
        Span::styled("GID      ", dim),
        Span::raw(gid.0.clone()),
        Span::raw("  "),
        Span::styled(tr("状态 ", "Status "), dim),
        Span::styled(status, status_style(status)),
        Span::raw("  "),
        Span::styled(tr("连接数 ", "Conns "), dim),
        Span::raw(conns.to_string()),
    ];
    if is_bt {
        // BT 任务显示上传速度（下载中做种/完成后 seed 模式均可见）
        line1.push(Span::raw("  "));
        line1.push(Span::styled(tr("上传 ", "Up "), dim));
        line1.push(Span::raw(format!("{}/s", fmt_size(upload_speed))));
    }
    let info = Paragraph::new(vec![
        Line::from(line1),
        Line::from(vec![
            Span::styled(tr("文件     ", "File     "), dim),
            Span::raw(path),
        ]),
        Line::from(vec![
            Span::styled(tr("目录     ", "Dir      "), dim),
            Span::raw(dir),
        ]),
    ])
    .block(
        ui_card()
            .title(Span::styled(tr(" 任务 ", " Task "), accent))
            .padding(Padding::horizontal(1)),
    )
    .wrap(Wrap { trim: false });
    f.render_widget(info, rows[0]);

    // 进度：仪表条 + 底部信息行（左：大小 │ 百分比，右：剩余时间）
    let prog_block = ui_card()
        .title(Span::styled(tr(" 进度 ", " Progress "), accent))
        .padding(Padding::new(1, 1, 1, 1));
    let inner = prog_block.inner(rows[1]);
    f.render_widget(prog_block, rows[1]);
    let total_label = if total > 0 {
        fmt_size(total)
    } else {
        tr("未知", "unknown").to_string()
    };
    let plines = Layout::vertical([
        Constraint::Length(1), // 进度条
        Constraint::Min(0),    // 留白（信息行上方空间）
        Constraint::Length(1), // 信息行
    ])
    .split(inner);
    let bar = mini_bar(pct, plines[0].width as usize);
    let bar_line = Paragraph::new(Line::from(Span::styled(bar, gauge_style(status))));
    f.render_widget(bar_line, plines[0]);
    // 信息行：左 = 已完成 / 总大小 │ 百分比；右 = 剩余时间
    let right_text = format!("{} {}", tr("剩余", "ETA"), eta);
    let info_cols = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(disp_w(&right_text) as u16),
    ])
    .split(plines[2]);
    let info_left = Paragraph::new(Line::from(vec![
        Span::raw(format!("{} / {}", fmt_size(completed), total_label)),
        Span::styled(" │ ", dim),
        Span::styled(format!("{:.1}%", pct * 100.0), Style::new().fg(Color::Cyan)),
    ]));
    f.render_widget(info_left, info_cols[0]);
    let info_right = Paragraph::new(Line::from(vec![
        Span::styled(tr("剩余 ", "ETA "), dim),
        Span::raw(eta),
    ]))
    .alignment(Alignment::Right);
    f.render_widget(info_right, info_cols[1]);

    // 实时速度图（最近 60s）
    let chart = Sparkline::default()
        .data(&st.hist)
        .style(Style::new().fg(Color::LightGreen))
        .block(
            ui_card()
                .title(vec![
                    Span::styled(tr(" 下载速度 ", " Down Speed "), accent),
                    Span::styled(
                        format!(
                            "↓ {}/s · {} {}/s",
                            fmt_size(speed),
                            tr("峰值", "peak"),
                            fmt_size(peak)
                        ),
                        dim,
                    ),
                ])
                .padding(Padding::horizontal(1)),
        );
    f.render_widget(chart, rows[2]);

    // 底栏提示（仅独立视图渲染；BT 详情分屏由外层统一画底栏）
    if show_footer {
        let foot =
            Paragraph::new(Line::from(Span::styled(footer, dim))).alignment(Alignment::Center);
        f.render_widget(foot, rows[3]);
    }
}

fn status_style(s: &str) -> ratatui::style::Style {
    use ratatui::style::{Color, Style};
    match s {
        "active" => Style::new().fg(Color::LightGreen),
        "waiting" | "paused" => Style::new().fg(Color::Yellow),
        "complete" => Style::new().fg(Color::Cyan),
        "error" => Style::new().fg(Color::Red),
        _ => Style::new().fg(Color::Gray),
    }
}

fn gauge_style(status: &str) -> ratatui::style::Style {
    use ratatui::style::{Color, Style};
    match status {
        "active" => Style::new().fg(Color::LightGreen),
        "complete" => Style::new().fg(Color::Cyan),
        "error" | "stopped" => Style::new().fg(Color::Red),
        _ => Style::new().fg(Color::Yellow),
    }
}

// ----------------------------------------------------------------------
// 设计系统：统一配色 / 圆角卡片 / 快捷键提示行
// ----------------------------------------------------------------------

/// 次要文字（标签、表头、说明）。
fn ui_dim() -> ratatui::style::Style {
    ratatui::style::Style::new().fg(ratatui::style::Color::Gray)
}

/// 强调色（标题、当前项、按键）。
fn ui_accent() -> ratatui::style::Style {
    ratatui::style::Style::new().fg(ratatui::style::Color::Cyan)
}

/// 圆角卡片（普通态：暗色边框，轻量融入背景）。
fn ui_card() -> ratatui::widgets::Block<'static> {
    ratatui::widgets::Block::bordered()
        .border_set(ratatui::symbols::border::ROUNDED)
        .border_style(ratatui::style::Style::new().fg(ratatui::style::Color::DarkGray))
}

/// 圆角卡片（聚焦态：亮边框标记当前焦点区域）。
fn ui_card_focused() -> ratatui::widgets::Block<'static> {
    ratatui::widgets::Block::bordered()
        .border_set(ratatui::symbols::border::ROUNDED)
        .border_style(ratatui::style::Style::new().fg(ratatui::style::Color::Yellow))
}

/// 主区边框：无标题，左右两侧上下均连成圆角，把侧栏与任务表格包裹在内。
/// 上下横线左端以 ╭/╰、右端以 ╮/╯ 与竖线连成圆角（左右各缩 1 列，
/// 与 logo/顶栏对齐）；左竖线贴左缘，侧栏分隔竖线在 side_x，
/// 右竖线贴右缘缩 1 列。
fn draw_main_borders(f: &mut ratatui::Frame, area: ratatui::layout::Rect, side_x: u16) {
    use ratatui::style::Style;
    use ratatui::text::{Line, Span};
    use ratatui::widgets::Paragraph;

    if area.width < 4 || area.height < 3 {
        return;
    }
    let style = Style::new().fg(ratatui::style::Color::DarkGray);
    // 上下横线：左右两端均为圆角（╭╮/╰╯），左端与 logo 对齐、右端缩 1 列
    let h_len = area.width.saturating_sub(4) as usize; // 两侧圆角之间的 ─ 数量
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            format!("╭{}╮", "─".repeat(h_len)),
            style,
        ))),
        ratatui::layout::Rect {
            x: area.x + 1,
            y: area.y,
            width: area.width - 2,
            height: 1,
        },
    );
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            format!("╰{}╯", "─".repeat(h_len)),
            style,
        ))),
        ratatui::layout::Rect {
            x: area.x + 1,
            y: area.bottom().saturating_sub(1),
            width: area.width - 2,
            height: 1,
        },
    );
    // 左竖线（连入圆角）+ 侧栏分隔竖线 + 右竖线（连入圆角）
    let v_len = area.height.saturating_sub(2) as usize;
    let vline: Vec<Line> = (0..v_len)
        .map(|_| Line::from(Span::styled("│", style)))
        .collect();
    let left_x = area.x + 1;
    let right_x = area.right().saturating_sub(2);
    let mut xs = vec![left_x];
    if side_x > left_x && side_x < right_x {
        xs.push(side_x);
    }
    xs.push(right_x);
    for x in xs {
        f.render_widget(
            Paragraph::new(vline.clone()),
            ratatui::layout::Rect {
                x,
                y: area.y + 1,
                width: 1,
                height: area.height - 2,
            },
        );
    }
}

/// 排空当前积压的终端按键事件（系统选择框打开期间产生）。
fn drain_pending_events() {
    while crossterm::event::poll(Duration::from_millis(0)).unwrap_or(false) {
        let _ = crossterm::event::read();
    }
}

/// 快捷键提示行：按键加亮 + 说明弱化，中点分隔（替代旧的一整串灰字）。
fn hint_line(pairs: &[(&'static str, String)]) -> ratatui::text::Line<'static> {
    let dim = ui_dim();
    let key_style = ui_accent().add_modifier(ratatui::style::Modifier::BOLD);
    let mut spans = vec![ratatui::text::Span::raw(" ")];
    for (i, (k, label)) in pairs.iter().enumerate() {
        if i > 0 {
            spans.push(ratatui::text::Span::styled(" · ", dim));
        }
        spans.push(ratatui::text::Span::styled(*k, key_style));
        spans.push(ratatui::text::Span::styled(format!(" {label}"), dim));
    }
    ratatui::text::Line::from(spans)
}

fn finish(mgr: &Arc<TaskManager>, gid: &Gid, exit_code: i32) {
    let st = mgr.tell_status_native(gid, None).unwrap_or(json!({}));
    match exit_code {
        0 => println!(
            "{}: {}",
            tr("下载完成", "Download complete"),
            st["files"][0]["path"].as_str().unwrap_or("-")
        ),
        _ => eprintln!(
            "{}: [{}] {}",
            tr("下载失败", "Download failed"),
            st["errorCode"].as_i64().unwrap_or(-1),
            st["errorMessage"]
                .as_str()
                .map(String::from)
                .unwrap_or_else(|| tr("未知错误", "unknown error"))
        ),
    }
}

// ----------------------------------------------------------------------
// 主 TUI：任务管理界面（`xfer` / `xfer tui` 启动）
// ----------------------------------------------------------------------

/// 主界面当前视图。
#[derive(Clone)]
enum MainView {
    List,
    Detail(Gid),
    Settings,
}

/// 设置项标识。
#[derive(Clone, Copy)]
enum SettingKey {
    /// 最大并发下载数（1-32），同时下载的任务数。
    MaxConcurrent,
    /// 预分配连接数（1-128），每个任务的自适应调度上限。
    SplitConnections,
    /// BT 预分配连接数（1-200），BT 智能调度上限（独立于 HTTP split）。
    BtConnections,
    /// 全局下载限速（KB/s，0 = 不限制）。
    MaxDownloadLimit,
    /// 全局上传限速（KB/s，0 = 不限制）。
    MaxUploadLimit,
    /// 下载目录（运行时修改，对新增任务生效）。
    DownloadDir,
    /// 单服务器连接数（0 = 默认）。
    MaxConnPerServer,
    /// 最小分片大小（字节，0 = 默认）。
    MinSplitSize,
}

/// 输入弹窗类型。
#[derive(Clone)]
enum InputKind {
    /// 修改设置项取值。
    EditSetting(SettingKey),
    /// 向 BT 任务添加 tracker。
    AddTracker(Gid),
    /// 添加全局 Tracker 服务器。
    AddGlobalTracker,
    /// 添加 Tracker 订阅源。
    AddSubscription,
}

/// 主界面状态。
#[derive(Clone)]
struct App {
    mgr: Arc<TaskManager>,
    view: MainView,
    selected: usize,
    /// 设置视图当前选中项（0 并发 / 1 连接 / 2 BT连接 / 3 下载限速 / 4 上传限速 / 5 目录）。
    settings_sel: usize,
    /// 设置页焦点区域：0 = 参数，1 = Tracker 列表，2 = 订阅源列表。
    settings_area: u8,
    /// Some((类型, 缓冲)) 表示正在输入。
    input: Option<(InputKind, String)>,
    /// 语言选择弹窗：选中的语言选项索引（0 简体 / 1 繁体 / 2 English）。
    lang_picker: Option<usize>,
    /// 退出确认弹窗激活中。
    confirm_quit: bool,
    /// 移除确认弹窗：待移除任务（勾选删除文件状态独立记录）。
    confirm_remove: Option<Gid>,
    /// 移除弹窗中"同时删除已下载文件"复选框。
    remove_del_files: bool,
    /// 全量任务快照（active → waiting → stopped 顺序）。
    tasks: Vec<Value>,
    /// gid → 速度历史（300ms 采样，最多 HIST_MAX 点）。
    hist: std::collections::HashMap<String, Vec<u64>>,
    started: std::time::Instant,
    /// 操作反馈消息（显示 2s）。
    message: Option<(String, std::time::Instant)>,
    /// 全局设置快照（设置视图显示）。
    max_concurrent: usize,
    /// 预分配连接数（自适应调度上限）。
    split_connections: usize,
    /// BT 预分配连接数（智能调度上限，独立于 HTTP 的 split）。
    bt_max_peers: usize,
    /// 全局下载限速（KB/s，0 = 不限制）。
    dl_limit_kbs: u64,
    /// 全局上传限速（KB/s，0 = 不限制）。
    ul_limit_kbs: u64,
    download_dir: String,
    /// 会话文件路径（设置视图显示；空串表示未开启持久化）。
    session_path: String,
    /// 全局 Tracker 服务器列表（设置页配置，所有 BT 任务自动注入）。
    global_trackers: Vec<String>,
    /// 设置页 Tracker 列表选中索引。
    tracker_sel: usize,
    /// Tracker 订阅源列表。
    subscriptions: Vec<SubRow>,
    /// 设置页订阅源列表选中索引。
    sub_sel: usize,
    /// 详情视图当前任务的 tracker 列表快照。
    detail_trackers: Vec<String>,
    /// 详情视图当前任务的 peer 列表快照。
    detail_peers: Vec<PeerRow>,
    /// peer 表滚动偏移 (行, 列)；进入详情页时归零。
    peer_scroll: (u16, u16),
    /// 详情页 tracker 表滚动偏移 (行, 列)；进入详情页时归零。
    tracker_scroll: (u16, u16),
    /// 详情页焦点：true = peer 表，false = tracker 表（Tab 切换）。
    detail_focus_peers: bool,
    /// 任务列表侧边栏当前分类。
    category: Category,
    /// 焦点是否在侧边栏（false = 任务列表）。
    sidebar_focus: bool,
    /// BT 加密模式原始值（adaptive/force/plain）。
    bt_encryption: String,
    /// BT 传输协议原始值（tcp+utp/tcp/utp）。
    bt_protocol: String,
    /// BT 智能调度开关。
    bt_adaptive: bool,
    /// 单服务器连接数（0 = 引擎默认）。
    max_conn_per_server: u64,
    /// 最小分片大小（字节，0 = 引擎默认）。
    min_split_size: u64,
    /// 新建任务弹窗（地址 + 目录 / 磁力解析 / 文件选择，模态）。
    add_task: Option<AddTaskDialog>,
}

/// TUI 详情页单行 peer 信息。
#[derive(Clone)]
struct PeerRow {
    addr: String,
    client: String,
    source: String,
    seed: bool,
    downloaded: u64,
    connected: bool,
    encrypted: bool,
    /// 本端向该对端上传的字节数。
    uploaded: u64,
    protocol: String,
    connected_secs: u64,
    /// 对端下载进度（0-100），未知（磁力元数据未就绪）为 None。
    progress: Option<f32>,
    /// 国家/地区显示文本（GeoIP 解析，未知为 "-"）。
    country: String,
}

/// 任务列表侧边栏分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Category {
    All,
    Downloading,
    Seeding,
    Complete,
    Error,
}

const CATEGORIES: [Category; 5] = [
    Category::All,
    Category::Downloading,
    Category::Seeding,
    Category::Complete,
    Category::Error,
];

impl Category {
    fn label(self) -> String {
        match self {
            Category::All => tr("全部", "All"),
            Category::Downloading => tr("下载", "Downloading"),
            Category::Seeding => tr("做种", "Seeding"),
            Category::Complete => tr("完成", "Done"),
            Category::Error => tr("错误", "Error"),
        }
    }

    /// 任务是否属于该分类（All 恒真）。
    /// 下载中 = active 且未完成；做种 = active 且已完成（BT 做种）；
    /// 完成/错误 = 对应终态；waiting/paused 仅在「全部」可见。
    fn matches(self, t: &serde_json::Value) -> bool {
        let status = t["status"].as_str().unwrap_or("");
        let completed = t["completedLength"].as_u64().unwrap_or(0);
        let total = t["totalLength"].as_u64().unwrap_or(0);
        match self {
            Category::All => true,
            Category::Downloading => status == "active" && completed < total,
            Category::Seeding => status == "active" && total > 0 && completed >= total,
            Category::Complete => status == "complete",
            Category::Error => status == "error",
        }
    }
}

/// 分类步进（循环），delta = ±1。
fn category_step(cat: Category, delta: i32) -> Category {
    let pos = CATEGORIES.iter().position(|c| *c == cat).unwrap_or(0) as i32;
    let next = (pos + delta).rem_euclid(CATEGORIES.len() as i32) as usize;
    CATEGORIES[next]
}

/// 当前分类下任务在 app.tasks 中的索引（选中/渲染均以过滤后序号为准）。
fn filtered_indices(app: &App) -> Vec<usize> {
    app.tasks
        .iter()
        .enumerate()
        .filter(|(_, t)| app.category.matches(t))
        .map(|(i, _)| i)
        .collect()
}

/// TUI 设置页单行订阅源信息。
#[derive(Clone)]
#[allow(dead_code)]
struct SubRow {
    id: String,
    name: String,
    url: String,
    enabled: bool,
    last_count: usize,
    last_error: String,
}

/// 磁力文件选择表格单行。
#[derive(Clone, Debug)]
struct MagnetFileRow {
    /// 文件索引（0 起算，select_files 入参）。
    index: usize,
    /// 相对种子根目录的显示路径。
    path: String,
    /// 文件大小（字节）。
    length: u64,
}

/// 新建任务弹窗焦点字段。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AddField {
    /// 下载地址（URL / 磁力链接 / .torrent 路径）。
    Url,
    /// 目录（空 = 使用全局下载目录，仅对本任务生效）。
    Dir,
}

/// 新建任务弹窗阶段：输入 →（磁力）解析中 → 向下扩展文件选择。
#[derive(Clone, Debug)]
enum AddStage {
    /// 地址 / 目录输入。
    Input,
    /// 磁力元数据解析中（用户输入后直接解析，弹窗保持打开）。
    Parsing { gid: Gid, started: std::time::Instant },
    /// 解析完成：弹窗向下扩展，显示文件选择表。
    Selecting {
        gid: Gid,
        /// 种子根目录名（单文件任务为文件名）。
        name: String,
        files: Vec<MagnetFileRow>,
        /// 与 files 一一对应的勾选状态。
        checked: Vec<bool>,
        cursor: usize,
    },
}

/// 新建任务弹窗（模态）：地址 + 目录两字段；
/// 磁力链接解析与文件选择在同一弹窗内完成（解析后向下扩展文件表格）。
#[derive(Clone, Debug)]
struct AddTaskDialog {
    /// 下载地址。
    url: String,
    /// 目录（空 = 使用全局下载目录）。
    dir: String,
    /// 当前焦点字段（Tab / ↑↓ 切换）。
    field: AddField,
    /// 当前阶段。
    stage: AddStage,
}

/// TUI 模式日志写入器：写入文件，避免日志混入终端渲染。
struct LogWriter {
    inner: Arc<Mutex<std::fs::File>>,
}

impl LogWriter {
    fn new(path: &std::path::Path) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap_or_else(|_| {
                // 文件打开失败时用平台空设备避免日志混入终端
                // （Windows 无 /dev/null，对应设备名为 NUL）
                let null = if cfg!(windows) { "NUL" } else { "/dev/null" };
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(null)
                    .expect("打开空设备失败")
            });
        Self {
            inner: Arc::new(Mutex::new(file)),
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogWriter {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter {
            inner: self.inner.clone(),
        }
    }
}

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner
            .lock()
            .map_err(|_| std::io::Error::other("日志锁中毒"))?
            .write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner
            .lock()
            .map_err(|_| std::io::Error::other("日志锁中毒"))?
            .flush()
    }
}

fn cmd_tui(args: &[String]) -> i32 {
    // 初始化日志（TUI 模式写入文件，避免日志输出混入终端渲染区域）
    let log_path = std::env::temp_dir().join("xfer-tui.log");
    let log_writer = LogWriter::new(&log_path);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(log_writer)
        .try_init();

    // 未显式指定时沿用会话设置（再退默认值）
    let dir = flag_value(args, "-d")
        .or_else(|| flag_value(args, "--dir"))
        .map(std::path::PathBuf::from);
    let conc: Option<usize> = flag_value(args, "-j")
        .or_else(|| flag_value(args, "--max-concurrent"))
        .and_then(|v| v.parse().ok());
    let rt = tokio::runtime::Runtime::new().expect("创建 runtime 失败");
    rt.block_on(async move {
        let mgr = TaskManager::start_with_session(dir, conc, xfer_engine::default_session_path());
        app_loop(mgr).await
    })
}

/// 渲染线程共享槽：latest-wins 快照 + 退出握手。
///
/// 渲染独占一个线程是为了隔离 stdout 写阻塞：全屏首帧（约百 KB 的
/// ANSI 序列）可能超过 pty 输出缓冲（约 64KB），读端不消费时
/// `write` 会永久阻塞——若在主线程同步渲染，键盘处理与引擎操作将
/// 全部停摆（TUI 卡死、退出后积压按键误杀刚启动的任务）。
struct RenderHub {
    /// 最新待渲染快照（新快照覆盖旧的，天然跳帧不积压）。
    slot: std::sync::Mutex<Option<App>>,
    cv: std::sync::Condvar,
    /// 请求渲染线程退出。
    quit: std::sync::atomic::AtomicBool,
    /// 渲染线程已完成终端恢复。
    done: std::sync::atomic::AtomicBool,
}

/// 发布一帧 App 快照（覆盖旧快照并唤醒渲染线程）。
fn publish(hub: &RenderHub, app: &App) {
    *hub.slot.lock().unwrap() = Some(app.clone());
    hub.cv.notify_all();
}

/// 渲染线程主体：独占 Terminal，只负责画。
/// stdout 写阻塞（pty 背压）只拖慢渲染，不影响主循环。
fn render_loop(
    mut term: ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
    hub: Arc<RenderHub>,
) {
    loop {
        let app = {
            let mut slot = hub.slot.lock().unwrap();
            loop {
                if hub.quit.load(std::sync::atomic::Ordering::Acquire) {
                    restore(&mut term);
                    hub.done.store(true, std::sync::atomic::Ordering::Release);
                    return;
                }
                match slot.take() {
                    Some(app) => break app,
                    None => slot = hub.cv.wait(slot).unwrap(),
                }
            }
        };
        let _ = term.draw(|f| draw_app(f, &app));
    }
}

/// 主界面循环：50ms 轮询键盘，300ms 刷新任务快照；渲染在独立线程。
async fn app_loop(mgr: Arc<TaskManager>) -> i32 {
    use crossterm::{cursor, execute, terminal};
    // 终端初始化：启动序列仅约百字节，不会撑满 pty 缓冲
    let _ = execute!(
        std::io::stdout(),
        terminal::EnterAlternateScreen,
        terminal::Clear(terminal::ClearType::All),
        // 清除滚动缓冲区：向上滑动不再看到 TUI 启动前的终端输出
        terminal::Clear(terminal::ClearType::Purge),
        cursor::Hide
    );
    let _ = terminal::enable_raw_mode();
    let term = ratatui::Terminal::new(ratatui::backend::CrosstermBackend::new(std::io::stdout()))
        .expect("TUI 初始化失败");

    let hub = Arc::new(RenderHub {
        slot: std::sync::Mutex::new(None),
        cv: std::sync::Condvar::new(),
        quit: std::sync::atomic::AtomicBool::new(false),
        done: std::sync::atomic::AtomicBool::new(false),
    });
    let _renderer = {
        let hub = hub.clone();
        std::thread::Builder::new()
            .name("tui-render".into())
            .spawn(move || render_loop(term, hub))
            .expect("渲染线程创建失败")
    };

    let mut app = App {
        mgr,
        view: MainView::List,
        selected: 0,
        settings_sel: 0,
        settings_area: 0,
        input: None,
        lang_picker: None,
        confirm_quit: false,
        confirm_remove: None,
        remove_del_files: false,
        tasks: Vec::new(),
        hist: std::collections::HashMap::new(),
        started: std::time::Instant::now(),
        message: None,
        max_concurrent: 1,
        split_connections: 16,
        bt_max_peers: 50,
        dl_limit_kbs: 0,
        ul_limit_kbs: 0,
        download_dir: String::new(),
        session_path: String::new(),
        global_trackers: Vec::new(),
        tracker_sel: 0,
        subscriptions: Vec::new(),
        sub_sel: 0,
        detail_trackers: Vec::new(),
        detail_peers: Vec::new(),
        peer_scroll: (0, 0),
        tracker_scroll: (0, 0),
        detail_focus_peers: true,
        category: Category::All,
        sidebar_focus: false,
        bt_encryption: "adaptive".to_string(),
        bt_protocol: "tcp+utp".to_string(),
        bt_adaptive: true,
        max_conn_per_server: 0,
        min_split_size: 0,
        add_task: None,
    };
    refresh_app(&mut app);
    // 启动恢复：上次会话解析完成、等待文件选择的磁力任务重新弹窗
    if let Some(t) = app
        .tasks
        .iter()
        .rev()
        .find(|t| {
            t["awaitingSelection"].as_bool().unwrap_or(false)
                && t["files"].as_array().is_some_and(|a| !a.is_empty())
        })
        .cloned()
    {
        let gid = Gid::from(t["gid"].as_str().unwrap_or(""));
        app.add_task = Some(add_task_dialog_selecting(&t, gid));
    }
    publish(&hub, &app);

    let mut last_render = std::time::Instant::now() - Duration::from_secs(1);
    loop {
        if crossterm::event::poll(Duration::from_millis(50)).unwrap_or(false) {
            if let Ok(crossterm::event::Event::Key(k)) = crossterm::event::read() {
                if !handle_key(&mut app, &k) {
                    break;
                }
                // 按键后立即刷新重绘，保证操作即时可见
                refresh_app(&mut app);
                publish(&hub, &app);
                last_render = std::time::Instant::now();
            }
        }
        if last_render.elapsed() >= Duration::from_millis(300) {
            refresh_app(&mut app);
            publish(&hub, &app);
            last_render = std::time::Instant::now();
        }
    }

    // 退出：通知渲染线程恢复终端并保存会话。
    hub.quit.store(true, std::sync::atomic::Ordering::Release);
    hub.cv.notify_all();
    let _ = app.mgr.save_session();
    // 等渲染线程恢复终端（最多 2s）：读端不消费输出时渲染线程可能
    // 卡在 write——超时则强制关闭 raw mode，避免用户终端残留 raw 状态。
    // 渲染线程可能永远卡住，因此不 join，进程退出时终止它。
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !hub.done.load(std::sync::atomic::Ordering::Acquire) {
        if std::time::Instant::now() >= deadline {
            let _ = terminal::disable_raw_mode();
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    0
}

/// 拉取全量任务列表与全局统计，更新速度历史。
fn refresh_app(app: &mut App) {
    let arr = app.mgr.list_native("all", 0, -1, None);
    app.tasks = arr.as_array().cloned().unwrap_or_default();
    // 新建任务弹窗状态机：磁力元数据就绪（引擎自动暂停等待选择）→ 向下扩展文件表格
    if let Some(d) = app.add_task.clone() {
        if let AddStage::Parsing { gid, .. } = d.stage {
            let found = app
                .tasks
                .iter()
                .find(|t| t["gid"].as_str() == Some(gid.0.as_str()))
                .cloned();
            match found {
                None => app.add_task = None, // 任务被移除
                Some(t) => {
                    let status = t["status"].as_str().unwrap_or("");
                    let has_files = t["files"].as_array().is_some_and(|a| !a.is_empty());
                    let awaiting = t["awaitingSelection"].as_bool().unwrap_or(false);
                    if status == "error" {
                        let msg = t["errorMessage"].as_str().unwrap_or("").to_string();
                        app.message = Some((
                            format!("{}: {msg}", tr("磁力解析失败", "Magnet parse failed")),
                            std::time::Instant::now(),
                        ));
                        app.add_task = None;
                    } else if awaiting && has_files {
                        app.add_task = Some(add_task_dialog_selecting(&t, gid));
                    }
                }
            }
        }
    }
    // 全局设置快照（设置视图显示）
    let opts = app.mgr.get_global_option();
    app.max_concurrent = opts["max-concurrent-downloads"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    app.split_connections = opts["split"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    app.bt_max_peers = opts["bt-max-peers"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    app.bt_encryption = opts["bt-encryption"]
        .as_str()
        .unwrap_or("adaptive")
        .to_string();
    app.bt_protocol = opts["bt-protocol"].as_str().unwrap_or("tcp+utp").to_string();
    app.bt_adaptive = opts["bt-adaptive"]
        .as_str()
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);
    // 未设置(0)即引擎默认：归一到引擎导出默认值，设置页直接显示数值
    app.max_conn_per_server = opts["max-connection-per-server"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(xfer_engine::DEFAULT_SPLIT_CONNECTIONS as u64);
    app.min_split_size = opts["min-split-size"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(xfer_engine::DEFAULT_MIN_SPLIT_SIZE);
    // 界面语言：会话里显式保存过才应用（否则沿用 XFER_LANG / 默认）
    if let Some(l) = opts["lang"].as_str() {
        match l {
            "en" => set_lang(Lang::En),
            "zh_tw" | "zh-tw" | "zh-hant" => set_lang(Lang::ZhTw),
            _ => set_lang(Lang::Zh),
        }
    }
    // 全局限速：选项存 bytes/s，TUI 显示/编辑 KB/s（0 = 不限制）
    app.dl_limit_kbs = opts["max-overall-download-limit"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
        / 1024;
    app.ul_limit_kbs = opts["max-overall-upload-limit"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
        / 1024;
    app.download_dir = opts["dir"].as_str().unwrap_or(".").to_string();
    app.session_path = opts["session-path"].as_str().unwrap_or("").to_string();
    app.global_trackers = opts["bt-trackers"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|t| t.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    if app.tracker_sel >= app.global_trackers.len() {
        app.tracker_sel = app.global_trackers.len().saturating_sub(1);
    }
    // 订阅源
    app.subscriptions = app
        .mgr
        .get_subscriptions()
        .into_iter()
        .map(|s| SubRow {
            id: s.id,
            name: s.name,
            url: s.url,
            enabled: s.enabled,
            last_count: s.last_count,
            last_error: s.last_error,
        })
        .collect();
    if app.sub_sel >= app.subscriptions.len() {
        app.sub_sel = app.subscriptions.len().saturating_sub(1);
    }
    let len = filtered_indices(app).len();
    if app.selected >= len {
        app.selected = len.saturating_sub(1);
    }
    for t in &app.tasks {
        let gid = t["gid"].as_str().unwrap_or("").to_string();
        let sp = t["downloadSpeed"].as_u64().unwrap_or(0);
        let h = app.hist.entry(gid).or_default();
        h.push(sp);
        if h.len() > HIST_MAX {
            let drop = h.len() - HIST_MAX;
            h.drain(0..drop);
        }
    }
    // 清理已不在列表中的任务历史
    app.hist.retain(|g, _| {
        app.tasks
            .iter()
            .any(|t| t["gid"].as_str() == Some(g.as_str()))
    });
    // 详情任务被移除后回列表
    if let MainView::Detail(g) = &app.view {
        if !app
            .tasks
            .iter()
            .any(|t| t["gid"].as_str() == Some(g.0.as_str()))
        {
            app.view = MainView::List;
        } else {
            // 刷新 tracker 列表 + peer 列表（BT 任务）
            app.detail_trackers = app
                .mgr
                .get_trackers(g)
                .ok()
                .and_then(|v| {
                    v.as_array().map(|a| {
                        a.iter()
                            .filter_map(|t| t["url"].as_str().map(String::from))
                            .collect()
                    })
                })
                .unwrap_or_default();
            app.detail_peers = app
                .mgr
                .get_peers(g)
                .ok()
                .and_then(|v| {
                    v.as_array().map(|a| {
                        a.iter()
                            .map(|p| {
                                let addr = p["addr"].as_str().unwrap_or("?").to_string();
                                PeerRow {
                                    country: geo_lookup(ip_of_addr(&addr)),
                                    addr,
                                    client: p["client"].as_str().unwrap_or("").to_string(),
                                    source: p["source"].as_str().unwrap_or("").to_string(),
                                    seed: p["seed"].as_bool().unwrap_or(false),
                                    downloaded: p["downloaded"].as_u64().unwrap_or(0),
                                    connected: p["connected"].as_bool().unwrap_or(false),
                                    encrypted: p["encrypted"].as_bool().unwrap_or(false),
                                    uploaded: p["uploaded"].as_u64().unwrap_or(0),
                                    protocol: p["protocol"]
                                        .as_str()
                                        .unwrap_or("tcp")
                                        .to_string(),
                                    connected_secs: p["connectedSecs"].as_u64().unwrap_or(0),
                                    progress: p["progress"].as_f64().map(|v| v as f32),
                                }
                            })
                            .collect()
                    })
                })
                .unwrap_or_default();
        }
    } else {
        app.detail_trackers.clear();
        app.detail_peers.clear();
    }
}

/// 处理一次按键。返回 false 表示退出。
fn handle_key(app: &mut App, k: &crossterm::event::KeyEvent) -> bool {
    use crossterm::event::{KeyCode, KeyModifiers};
    // 新建任务弹窗（模态，优先处理）
    if let Some(d) = app.add_task.clone() {
        return handle_add_task_key(app, d, k);
    }
    // 输入模式：编辑缓冲（并发数只接受数字）
    if let Some((kind, buf)) = app.input.as_mut() {
        match k.code {
            KeyCode::Char(c) => {
                let digit_only = matches!(
                    kind,
                    InputKind::EditSetting(SettingKey::MaxConcurrent)
                        | InputKind::EditSetting(SettingKey::SplitConnections)
                        | InputKind::EditSetting(SettingKey::BtConnections)
                        | InputKind::EditSetting(SettingKey::MaxDownloadLimit)
                        | InputKind::EditSetting(SettingKey::MaxUploadLimit)
                );
                if !digit_only || c.is_ascii_digit() {
                    buf.push(c);
                }
            }
            KeyCode::Backspace => {
                buf.pop();
            }
            KeyCode::Enter => {
                let kind = kind.clone();
                let val = buf.trim().to_string();
                app.input = None;
                match kind {
                    InputKind::EditSetting(key) => submit_setting(app, key, &val),
                    InputKind::AddTracker(gid) => submit_tracker(app, gid, &val),
                    InputKind::AddGlobalTracker => submit_global_tracker(app, &val),
                    InputKind::AddSubscription => submit_subscription(app, &val),
                }
            }
            KeyCode::Esc => app.input = None,
            _ => {}
        }
        return true;
    }
    // 退出确认弹窗：y/Enter/q 确认退出，其余（n/Esc/任意键）取消。
    if app.confirm_quit {
        match k.code {
            KeyCode::Char('y') | KeyCode::Char('q') | KeyCode::Enter => return false,
            KeyCode::Char('n') | KeyCode::Esc => app.confirm_quit = false,
            _ => app.confirm_quit = false,
        }
        return true;
    }
    // 移除确认弹窗：空格/Tab 切换"删除文件"复选框，
    // y/Enter 确认移除，n/Esc 取消。
    if app.confirm_remove.is_some() {
        match k.code {
            KeyCode::Char(' ')
            | KeyCode::Tab
            | KeyCode::Down
            | KeyCode::Up
            | KeyCode::Char('d') => app.remove_del_files = !app.remove_del_files,
            KeyCode::Char('y') | KeyCode::Enter => {
                let gid = app.confirm_remove.take().unwrap();
                let del = app.remove_del_files;
                app.remove_del_files = false;
                submit_remove(app, gid, del);
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                app.confirm_remove = None;
                app.remove_del_files = false;
            }
            _ => {
                app.confirm_remove = None;
                app.remove_del_files = false;
            }
        }
        return true;
    }
    // Ctrl-C 强制退出，不弹确认弹窗
    if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) {
        return false;
    }
    // 语言选择弹窗：↑↓ 移动，Enter 确认，Esc/q 取消
    if app.lang_picker.is_some() {
        let sel = app.lang_picker.as_mut().unwrap();
        match k.code {
            KeyCode::Up | KeyCode::Char('k') => *sel = sel.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => *sel = (*sel + 1).min(2),
            KeyCode::Enter => {
                let l = [Lang::Zh, Lang::ZhTw, Lang::En][*sel];
                set_lang(l);
                let key = match l {
                    Lang::Zh => "zh",
                    Lang::ZhTw => "zh_tw",
                    Lang::En => "en",
                };
                let msg = apply_global_option(
                    app,
                    "lang",
                    key,
                    tr("界面语言", "Language"),
                );
                app.message = Some((msg, std::time::Instant::now()));
                app.lang_picker = None;
            }
            KeyCode::Esc | KeyCode::Char('q') => app.lang_picker = None,
            _ => {}
        }
        return true;
    }
    match &app.view {
        MainView::List => match k.code {
            KeyCode::Char('q') => app.confirm_quit = true,
            KeyCode::Esc => {
                if app.sidebar_focus {
                    app.sidebar_focus = false;
                } else {
                    app.confirm_quit = true;
                }
            }
            KeyCode::Tab => app.sidebar_focus = !app.sidebar_focus,
            KeyCode::Up | KeyCode::Char('k') => {
                if app.sidebar_focus {
                    app.category = category_step(app.category, -1);
                } else {
                    app.selected = app.selected.saturating_sub(1);
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if app.sidebar_focus {
                    app.category = category_step(app.category, 1);
                } else {
                    app.selected = (app.selected + 1)
                        .min(filtered_indices(app).len().saturating_sub(1));
                }
            }
            KeyCode::Right | KeyCode::Char('l') => {
                if app.sidebar_focus {
                    app.sidebar_focus = false;
                }
            }
            KeyCode::Enter => {
                if app.sidebar_focus {
                    app.sidebar_focus = false;
                } else if let Some(g) = gid_at(app, app.selected) {
                    app.view = MainView::Detail(g);
                    app.peer_scroll = (0, 0);
                    app.tracker_scroll = (0, 0);
                    app.detail_focus_peers = true;
                }
            }
            KeyCode::Char(c) if ('1'..='5').contains(&c) => {
                if let Some(cat) = CATEGORIES.get(c.to_digit(10).unwrap_or(1) as usize - 1) {
                    app.category = *cat;
                    app.selected = 0;
                }
            }
            KeyCode::Char('a') => {
                app.add_task = Some(AddTaskDialog {
                    // 目录预填全局下载目录（可改，仅对本任务生效）
                    url: String::new(),
                    dir: app.download_dir.clone(),
                    field: AddField::Url,
                    stage: AddStage::Input,
                });
            }
            KeyCode::Char('s') => app.view = MainView::Settings,
            KeyCode::Char('r') => app_action(app, "toggle"),
            KeyCode::Char('x') => {
                if let Some(g) = current_gid(app) {
                    app.confirm_remove = Some(g);
                    app.remove_del_files = false;
                }
            }
            KeyCode::Char('c') => {
                let _ = app.mgr.purge_download_result();
                app.message = Some((
                    tr("已清除完成记录", "Cleared finished results").to_string(),
                    std::time::Instant::now(),
                ));
            }
            _ => {}
        },
        MainView::Detail(_) => match k.code {
            KeyCode::Char('q') => app.confirm_quit = true,
            KeyCode::Esc | KeyCode::Enter => {
                app.view = MainView::List;
            }
            // Tab：tracker 表 / peer 表焦点切换
            KeyCode::Tab => app.detail_focus_peers = !app.detail_focus_peers,
            // 滚动：方向键作用于当前聚焦的表格（tracker 表 / peer 表）
            KeyCode::Up => {
                if app.detail_focus_peers {
                    app.peer_scroll.0 = app.peer_scroll.0.saturating_sub(1);
                } else {
                    app.tracker_scroll.0 = app.tracker_scroll.0.saturating_sub(1);
                }
            }
            KeyCode::Down => {
                if app.detail_focus_peers {
                    app.peer_scroll.0 = app.peer_scroll.0.saturating_add(1);
                } else {
                    app.tracker_scroll.0 = app.tracker_scroll.0.saturating_add(1);
                }
            }
            KeyCode::PageUp => {
                if app.detail_focus_peers {
                    app.peer_scroll.0 = app.peer_scroll.0.saturating_sub(10);
                } else {
                    app.tracker_scroll.0 = app.tracker_scroll.0.saturating_sub(10);
                }
            }
            KeyCode::PageDown => {
                if app.detail_focus_peers {
                    app.peer_scroll.0 = app.peer_scroll.0.saturating_add(10);
                } else {
                    app.tracker_scroll.0 = app.tracker_scroll.0.saturating_add(10);
                }
            }
            KeyCode::Left | KeyCode::Char('h') => {
                if app.detail_focus_peers {
                    app.peer_scroll.1 = app.peer_scroll.1.saturating_sub(2)
                } else {
                    app.tracker_scroll.1 = app.tracker_scroll.1.saturating_sub(2)
                }
            }
            KeyCode::Right | KeyCode::Char('l') => {
                if app.detail_focus_peers {
                    app.peer_scroll.1 = app.peer_scroll.1.saturating_add(2)
                } else {
                    app.tracker_scroll.1 = app.tracker_scroll.1.saturating_add(2)
                }
            }
            KeyCode::Char('r') => app_action(app, "toggle"),
            KeyCode::Char('t') => {
                if let MainView::Detail(g) = app.view.clone() {
                    // 只有 BT 任务才能添加 tracker
                    if app.mgr.is_bt_task(&g) {
                        app.input = Some((InputKind::AddTracker(g), String::new()));
                    } else {
                        app.message = Some((
                            tr("非 BT 任务，无 tracker", "Not a BT task, no trackers")
                                .to_string(),
                            std::time::Instant::now(),
                        ));
                    }
                }
            }
            KeyCode::Char('x') => {
                if let Some(g) = current_gid(app) {
                    app.confirm_remove = Some(g);
                    app.remove_del_files = false;
                }
            }
            _ => {}
        },
        MainView::Settings => match k.code {
            KeyCode::Char('q') => app.confirm_quit = true,
            KeyCode::Esc => app.view = MainView::List,
            // Tab 在参数(0) / Tracker列表(1) / 订阅源列表(2) 之间循环切换焦点
            KeyCode::Tab => {
                app.settings_area = (app.settings_area + 1) % 3;
            }
            KeyCode::Up | KeyCode::Char('k') => match app.settings_area {
                0 => app.settings_sel = app.settings_sel.saturating_sub(1),
                1 => {
                    if app.tracker_sel > 0 {
                        app.tracker_sel -= 1;
                    }
                }
                _ => {
                    if app.sub_sel > 0 {
                        app.sub_sel -= 1;
                    }
                }
            },
            KeyCode::Down | KeyCode::Char('j') => match app.settings_area {
                // 参数区共 12 项（0..=11，末项为「界面语言」），上限必须到 11
                0 => app.settings_sel = (app.settings_sel + 1).min(11),
                1 => {
                    if app.tracker_sel + 1 < app.global_trackers.len() {
                        app.tracker_sel += 1;
                    }
                }
                _ => {
                    if app.sub_sel + 1 < app.subscriptions.len() {
                        app.sub_sel += 1;
                    }
                }
            },
            KeyCode::Left | KeyCode::Char('-') => {
                if app.settings_area == 0 {
                    adjust_concurrency(app, -1);
                }
            }
            KeyCode::Right | KeyCode::Char('+') | KeyCode::Char('=') => {
                if app.settings_area == 0 {
                    adjust_concurrency(app, 1);
                }
            }
            KeyCode::Enter => {
                if app.settings_area == 0 {
                    if app.settings_sel == 11 {
                        // 界面语言：三个选项全部摆出，直接选择
                        app.lang_picker = Some(match lang() {
                            Lang::Zh => 0,
                            Lang::ZhTw => 1,
                            Lang::En => 2,
                        });
                    } else if !matches!(app.settings_sel, 5 | 6 | 7 | 8) {
                        let (kind, init) = match app.settings_sel {
                            0 => (
                                InputKind::EditSetting(SettingKey::MaxConcurrent),
                                app.max_concurrent.to_string(),
                            ),
                            1 => (
                                InputKind::EditSetting(SettingKey::SplitConnections),
                                app.split_connections.to_string(),
                            ),
                            2 => (
                                InputKind::EditSetting(SettingKey::BtConnections),
                                app.bt_max_peers.to_string(),
                            ),
                            3 => (
                                InputKind::EditSetting(SettingKey::MaxDownloadLimit),
                                app.dl_limit_kbs.to_string(),
                            ),
                            4 => (
                                InputKind::EditSetting(SettingKey::MaxUploadLimit),
                                app.ul_limit_kbs.to_string(),
                            ),
                            9 => (
                                InputKind::EditSetting(SettingKey::MaxConnPerServer),
                                app.max_conn_per_server.to_string(),
                            ),
                            _ => (
                                InputKind::EditSetting(SettingKey::MinSplitSize),
                                app.min_split_size.to_string(),
                            ),
                        };
                        app.input = Some((kind, init));
                    } else if app.settings_sel == 5 {
                        // 下载目录：直接调用系统目录选择框
                        let picked = pick_directory_via_os();
                        drain_pending_events();
                        if let Some(p) = picked {
                            submit_setting(app, SettingKey::DownloadDir, &p);
                        }
                    }
                }
            }
            // a: 添加（焦点在 Tracker 列表→添加 tracker；在订阅列表→添加订阅源）
            KeyCode::Char('a') if app.settings_area == 1 => {
                app.input = Some((InputKind::AddGlobalTracker, String::new()));
            }
            KeyCode::Char('a') if app.settings_area == 2 => {
                app.input = Some((InputKind::AddSubscription, String::new()));
            }
            // d: 删除选中
            KeyCode::Char('d') if app.settings_area == 1 => {
                if let Some(url) = app.global_trackers.get(app.tracker_sel).cloned() {
                    let r = app
                        .mgr
                        .remove_global_tracker(&url)
                        .map(|_| {
                            format!(
                                "{}: {url}",
                                tr("已移除 tracker", "Tracker removed")
                            )
                        })
                        .unwrap_or_else(|e| e);
                    app.message = Some((r, std::time::Instant::now()));
                }
            }
            KeyCode::Char('d') if app.settings_area == 2 => {
                if let Some(sub) = app.subscriptions.get(app.sub_sel).cloned() {
                    let r = app
                        .mgr
                        .remove_subscription(&sub.id)
                        .map(|_| {
                            format!(
                                "{}: {}",
                                tr("已移除订阅源", "Subscription removed"),
                                sub.name
                            )
                        })
                        .unwrap_or_else(|e| e);
                    app.message = Some((r, std::time::Instant::now()));
                }
            }
            // t: 切换订阅源启用/禁用
            KeyCode::Char('t') if app.settings_area == 2 => {
                if let Some(sub) = app.subscriptions.get(app.sub_sel).cloned() {
                    let r = app
                        .mgr
                        .toggle_subscription(&sub.id)
                        .map(|_| {
                            format!(
                                "{} {}: {}",
                                tr("已", ""),
                                if sub.enabled {
                                    tr("禁用订阅源", "Disabled subscription")
                                } else {
                                    tr("启用订阅源", "Enabled subscription")
                                },
                                sub.name
                            )
                        })
                        .unwrap_or_else(|e| e);
                    app.message = Some((r, std::time::Instant::now()));
                }
            }
            // r: 刷新选中的订阅源
            KeyCode::Char('r') if app.settings_area == 2 => {
                if let Some(sub) = app.subscriptions.get(app.sub_sel).cloned() {
                    let mgr = app.mgr.clone();
                    let id = sub.id.clone();
                    let name = sub.name.clone();
                    let r = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current()
                            .block_on(async { mgr.refresh_subscription(&id).await })
                    })
                    .map(|n| {
                        format!(
                            "{} {name}：{} {n} {}",
                            tr("已刷新订阅源", "Refreshed subscription"),
                            tr("获取", "got"),
                            tr("个 tracker", "trackers")
                        )
                    })
                    .unwrap_or_else(|e| {
                        format!("{}: {e}", tr("刷新失败", "Refresh failed"))
                    });
                    // refresh_subscription 内部会同步更新全局 trackers
                    app.message = Some((r, std::time::Instant::now()));
                    refresh_app(app);
                }
            }
            // R: 刷新所有订阅源
            KeyCode::Char('R') if app.settings_area == 2 => {
                let mgr = app.mgr.clone();
                let r = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(async { mgr.refresh_all_subscriptions().await })
                })
                .map(|n| {
                    format!(
                        "{}：{} {n} {}",
                        tr("已刷新所有订阅源", "Refreshed all subscriptions"),
                        tr("共获取", "got"),
                        tr("个 tracker", "trackers")
                    )
                })
                .unwrap_or_else(|e| format!("{}: {e}", tr("刷新失败", "Refresh failed")));
                app.message = Some((r, std::time::Instant::now()));
                refresh_app(app);
            }
            _ => {}
        },
    }
    true
}

/// 从任务状态快照构建文件选择阶段（元数据就绪、任务自动暂停后调用）。
///
/// 多文件任务剥离公共根目录段作为弹窗标题；文件索引取
/// `files[].index - 1`（0 起算），默认全部勾选。
fn magnet_selection_from_status(t: &Value, gid: Gid) -> AddStage {
    let files_arr = t["files"].as_array().cloned().unwrap_or_default();
    let paths: Vec<String> = files_arr
        .iter()
        .filter_map(|f| f["path"].as_str().map(String::from))
        .collect();
    let root = paths
        .first()
        .and_then(|p| p.split('/').next())
        .unwrap_or("")
        .to_string();
    let multi = !root.is_empty() && paths.iter().all(|p| p.starts_with(&format!("{root}/")));
    let name = if multi {
        root.clone()
    } else {
        paths.first().cloned().unwrap_or_else(|| gid.0.clone())
    };
    let files: Vec<MagnetFileRow> = files_arr
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let path = f["path"].as_str().unwrap_or("").to_string();
            let display = if multi && path.starts_with(&format!("{root}/")) {
                path[root.len() + 1..].to_string()
            } else {
                path
            };
            MagnetFileRow {
                index: f["index"]
                    .as_u64()
                    .map(|n| n as usize)
                    .unwrap_or(i + 1)
                    .saturating_sub(1),
                path: display,
                length: f["length"].as_u64().unwrap_or(0),
            }
        })
        .collect();
    let checked = vec![true; files.len()];
    AddStage::Selecting {
        gid,
        name,
        files,
        checked,
        cursor: 0,
    }
}

/// 构建处于文件选择阶段的新建任务弹窗（启动恢复用；目录取任务快照）。
fn add_task_dialog_selecting(t: &Value, gid: Gid) -> AddTaskDialog {
    AddTaskDialog {
        url: String::new(),
        dir: t["dir"].as_str().unwrap_or("").to_string(),
        field: AddField::Url,
        stage: magnet_selection_from_status(t, gid),
    }
}

/// 勾选汇总：(已选个数, 总个数, 已选字节, 总字节)。
fn magnet_selected_summary(files: &[MagnetFileRow], checked: &[bool]) -> (usize, usize, u64, u64) {
    let total_bytes = files.iter().map(|f| f.length).sum();
    let mut sel_n = 0usize;
    let mut sel_bytes = 0u64;
    for (f, c) in files.iter().zip(checked.iter()) {
        if *c {
            sel_n += 1;
            sel_bytes += f.length;
        }
    }
    (sel_n, files.len(), sel_bytes, total_bytes)
}

/// 调用系统目录选择框（阻塞至用户选择或取消）。
/// macOS 用 osascript `choose folder`；Windows 用 PowerShell
/// FolderBrowserDialog（-STA）；Linux 依次尝试 zenity / kdialog。
/// 返回所选目录绝对路径；取消或无可用组件时返回 None。
#[cfg(target_os = "macos")]
fn pick_directory_via_os() -> Option<String> {
    let title = tr("选择下载目录", "Choose download folder");
    let script = format!(
        "POSIX path of (choose folder with prompt \"{}\")",
        title.replace('\\', "\\\\").replace('"', "\\\"")
    );
    let out = std::process::Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .output()
        .ok()?;
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if out.status.success() && !path.is_empty() {
        let trimmed = path.trim_end_matches('/');
        Some(if trimmed.is_empty() { "/".to_string() } else { trimmed.to_string() })
    } else {
        None
    }
}

/// Windows：FolderBrowserDialog 需要 STA 线程；UTF-8 输出避免中文路径乱码。
#[cfg(windows)]
fn pick_directory_via_os() -> Option<String> {
    let title = tr("选择下载目录", "Choose download folder").replace('\'', "''");
    let ps = format!(
        "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; \
         Add-Type -AssemblyName System.Windows.Forms | Out-Null; \
         $f = New-Object System.Windows.Forms.FolderBrowserDialog; \
         $f.Description = '{title}'; \
         if ($f.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {{ $f.SelectedPath }}"
    );
    let out = std::process::Command::new("powershell")
        .args(["-NoProfile", "-STA", "-Command", &ps])
        .output()
        .ok()?;
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if out.status.success() && !path.is_empty() {
        Some(path)
    } else {
        None
    }
}

/// Linux：优先 zenity（GTK 桌面），其次 kdialog（KDE 桌面）。
#[cfg(all(unix, not(target_os = "macos")))]
fn pick_directory_via_os() -> Option<String> {
    let title = tr("选择下载目录", "Choose download folder");
    let candidates: [(&str, Vec<&str>); 2] = [
        (
            "zenity",
            vec!["--file-selection", "--directory", "--title", &title],
        ),
        (
            "kdialog",
            vec!["--getexistingdirectory", ".", "--title", &title],
        ),
    ];
    for (prog, args) in candidates {
        if let Ok(out) = std::process::Command::new(prog).args(args).output() {
            let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if out.status.success() && !path.is_empty() {
                return Some(path);
            }
        }
    }
    None
}

/// 新建任务弹窗按键处理。返回 true 继续运行。
fn handle_add_task_key(
    app: &mut App,
    mut d: AddTaskDialog,
    k: &crossterm::event::KeyEvent,
) -> bool {
    use crossterm::event::KeyCode;
    match d.stage.clone() {
        // 输入阶段：编辑地址 / 目录两字段
        AddStage::Input => match k.code {
            KeyCode::Tab | KeyCode::Up | KeyCode::Down => {
                d.field = if d.field == AddField::Url {
                    AddField::Dir
                } else {
                    AddField::Url
                };
                app.add_task = Some(d);
            }
            KeyCode::Backspace => {
                if d.field == AddField::Url {
                    d.url.pop();
                } else {
                    d.dir.pop();
                }
                app.add_task = Some(d);
            }
            KeyCode::Char(c) => {
                if d.field == AddField::Url {
                    d.url.push(c);
                } else {
                    d.dir.push(c);
                }
                app.add_task = Some(d);
            }
            KeyCode::Enter => {
                // 目录字段：Enter 打开系统目录选择框；地址字段：Enter 提交
                if d.field == AddField::Dir {
                    let picked = pick_directory_via_os();
                    // 排空选择框打开期间积压的终端按键，避免误触弹窗
                    drain_pending_events();
                    if let Some(p) = picked {
                        d.dir = p;
                    }
                    app.add_task = Some(d);
                } else {
                    submit_add_task(app, &d);
                }
            }
            KeyCode::Esc => app.add_task = None,
            _ => app.add_task = Some(d),
        },
        // 解析中：Esc/q 取消（移除任务，不留残骸）
        AddStage::Parsing { gid, .. } => match k.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                let _ = app.mgr.remove(&gid);
                app.add_task = None;
                app.message = Some((
                    tr("已取消解析", "Parsing cancelled").to_string(),
                    std::time::Instant::now(),
                ));
            }
            _ => {}
        },
        AddStage::Selecting {
            gid,
            name,
            files,
            mut checked,
            mut cursor,
        } => {
            let n = files.len();
            let mut close = false;
            match k.code {
                KeyCode::Up | KeyCode::Char('k') => cursor = cursor.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    cursor = (cursor + 1).min(n.saturating_sub(1));
                }
                KeyCode::PageUp => cursor = cursor.saturating_sub(10),
                KeyCode::PageDown => cursor = (cursor + 10).min(n.saturating_sub(1)),
                KeyCode::Home => cursor = 0,
                KeyCode::End => cursor = n.saturating_sub(1),
                KeyCode::Char(' ') => {
                    if let Some(c) = checked.get_mut(cursor) {
                        *c = !*c;
                    }
                }
                KeyCode::Char('a') | KeyCode::Char('A') => {
                    let all_on = checked.iter().all(|c| *c);
                    let next = !all_on;
                    for c in checked.iter_mut() {
                        *c = next;
                    }
                }
                // 确认：写回文件选择并恢复下载
                KeyCode::Enter => {
                    let sel: Vec<usize> = files
                        .iter()
                        .zip(checked.iter())
                        .filter(|(_, c)| **c)
                        .map(|(f, _)| f.index)
                        .collect();
                    if sel.is_empty() {
                        app.message = Some((
                            tr("至少选择一个文件", "Select at least one file").to_string(),
                            std::time::Instant::now(),
                        ));
                    } else {
                        let (sel_n, total_n, sel_bytes, _total) =
                            magnet_selected_summary(&files, &checked);
                        match app.mgr.select_files(&gid, &sel) {
                            Ok(()) => {
                                // 暂停等待中的任务 → 确认后立即恢复下载
                                let r = app
                                    .mgr
                                    .unpause(&gid)
                                    .map(|_| tr("开始下载", "downloading").to_string())
                                    .unwrap_or_else(|e| e);
                                app.message = Some((
                                    format!(
                                        "{} {sel_n}/{total_n} · {} — {r}",
                                        tr("已选", "Selected"),
                                        fmt_size(sel_bytes),
                                    ),
                                    std::time::Instant::now(),
                                ));
                                close = true;
                            }
                            Err(e) => {
                                app.message = Some((
                                    format!("{}: {e}", tr("设置失败", "Failed to apply")),
                                    std::time::Instant::now(),
                                ));
                            }
                        }
                    }
                }
                // 取消：移除任务
                KeyCode::Esc | KeyCode::Char('q') => {
                    let _ = app.mgr.remove(&gid);
                    app.message = Some((
                        tr("已取消下载", "Download cancelled").to_string(),
                        std::time::Instant::now(),
                    ));
                    close = true;
                }
                _ => {}
            }
            if close {
                app.add_task = None;
            } else {
                d.stage = AddStage::Selecting {
                    gid,
                    name,
                    files,
                    checked,
                    cursor,
                };
                app.add_task = Some(d);
            }
        }
    }
    true
}

/// 提交新建任务：磁力链接输入后直接解析（弹窗保持打开并进入解析态）；
/// URL / .torrent 添加成功后关闭弹窗。目录非空时作为任务级目录。
fn submit_add_task(app: &mut App, d: &AddTaskDialog) {
    let url = d.url.trim().to_string();
    if url.is_empty() {
        app.message = Some((
            tr("地址为空", "Empty URL").to_string(),
            std::time::Instant::now(),
        ));
        return;
    }
    // 目录：仅对本任务生效，不修改全局下载目录
    let dir = d.dir.trim().to_string();
    let mut opts = serde_json::Map::new();
    if !dir.is_empty() {
        opts.insert("dir".into(), json!(dir));
    }
    // 磁力链接：用户输入后直接解析 → 文件表格勾选 → 确认后开始下载
    if url.starts_with("magnet:") {
        opts.insert("bt-file-selection".into(), json!("true"));
        match app.mgr.add_uri(vec![url], &Value::Object(opts), None) {
            Ok(gid) => {
                app.add_task = Some(AddTaskDialog {
                    stage: AddStage::Parsing {
                        gid,
                        started: std::time::Instant::now(),
                    },
                    ..d.clone()
                });
                app.message = Some((
                    tr("正在解析磁力链接…", "Parsing magnet link…").to_string(),
                    std::time::Instant::now(),
                ));
            }
            Err(e) => {
                app.message = Some((
                    format!("{}: {e}", tr("添加失败", "Add failed")),
                    std::time::Instant::now(),
                ));
            }
        }
        return;
    }
    // 本地 .torrent 文件 → BT 任务；否则 URL（add_uri 自动识别协议）
    let r = if url.ends_with(".torrent") && std::path::Path::new(&url).is_file() {
        use base64::Engine;
        std::fs::read(&url)
            .map_err(|e| {
                format!(
                    "{}: {e}",
                    tr("读取种子文件失败", "Failed to read torrent file")
                )
            })
            .and_then(|b| {
                app.mgr
                    .add_torrent(
                        &base64::engine::general_purpose::STANDARD.encode(b),
                        &Value::Object(opts),
                        None,
                    )
                    .map(|g| format!("{} {}", tr("已添加任务", "Task added"), g.0))
                    .map_err(|e| format!("{}: {e}", tr("添加失败", "Add failed")))
            })
    } else {
        app.mgr
            .add_uri(vec![url], &Value::Object(opts), None)
            .map(|g| format!("{} {}", tr("已添加任务", "Task added"), g.0))
            .map_err(|e| format!("{}: {e}", tr("添加失败", "Add failed")))
    };
    match r {
        Ok(msg) => {
            app.add_task = None;
            app.message = Some((msg, std::time::Instant::now()));
        }
        Err(e) => {
            app.message = Some((e, std::time::Instant::now()));
        }
    }
}

/// 提交输入的 tracker URL，添加到 BT 任务。
fn submit_tracker(app: &mut App, gid: Gid, val: &str) {
    let val = val.trim();
    if val.is_empty() {
        app.message = Some((
            tr("tracker 地址为空", "Empty tracker URL").to_string(),
            std::time::Instant::now(),
        ));
        return;
    }
    // 支持空格/逗号分隔批量输入
    let trackers: Vec<String> = val
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let r = app
        .mgr
        .add_trackers(&gid, trackers)
        .map(|_| tr("已添加 tracker", "Trackers added").to_string())
        .unwrap_or_else(|e| format!("{}: {e}", tr("添加失败", "Add failed")));
    app.message = Some((r, std::time::Instant::now()));
}

/// 提交全局 Tracker 服务器添加。
fn submit_global_tracker(app: &mut App, val: &str) {
    let val = val.trim();
    if val.is_empty() {
        app.message = Some((
            tr("tracker 地址为空", "Empty tracker URL").to_string(),
            std::time::Instant::now(),
        ));
        return;
    }
    // 支持空格/逗号分隔批量输入
    let urls: Vec<&str> = val
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let mut ok_count = 0u32;
    let mut errors = Vec::new();
    for url in urls {
        match app.mgr.add_global_tracker(url) {
            Ok(()) => ok_count += 1,
            Err(e) => errors.push(e),
        }
    }
    let msg = if ok_count > 0 && errors.is_empty() {
        format!(
            "{} {ok_count} {}",
            tr("已添加", "Added"),
            tr("个 tracker", "trackers")
        )
    } else if ok_count > 0 {
        format!(
            "{} {ok_count}，{}: {}",
            tr("已添加", "Added"),
            tr("部分失败", "some failed"),
            errors.join("; ")
        )
    } else {
        format!(
            "{}: {}",
            tr("添加失败", "Add failed"),
            errors.join("; ")
        )
    };
    app.message = Some((msg, std::time::Instant::now()));
}

/// 提交输入的订阅源 URL，添加到全局订阅列表。
fn submit_subscription(app: &mut App, val: &str) {
    let val = val.trim();
    if val.is_empty() {
        app.message = Some((
            tr("订阅源地址为空", "Empty subscription URL").to_string(),
            std::time::Instant::now(),
        ));
        return;
    }
    // 格式：名称 URL 或仅 URL
    let (name, url) = if let Some(space_pos) = val.find(' ') {
        let (n, u) = val.split_at(space_pos);
        (n.trim().to_string(), u.trim().to_string())
    } else {
        (String::new(), val.to_string())
    };
    match app.mgr.add_subscription(&name, &url, true) {
        Ok(sub) => {
            // 引擎侧已排队立即刷新（add_subscription 内部 kick 后台
            // 刷新循环），tracker 列表随 300ms 轮询自动更新
            let msg = format!(
                "{}: {}（{}）",
                tr("已添加订阅源", "Subscription added"),
                sub.name,
                tr("正在获取 tracker", "fetching trackers")
            );
            app.message = Some((msg, std::time::Instant::now()));
            refresh_app(app);
        }
        Err(e) => {
            app.message = Some((
                format!("{}: {e}", tr("添加失败", "Add failed")),
                std::time::Instant::now(),
            ));
        }
    }
}

/// 提交输入的设置项取值。
fn submit_setting(app: &mut App, key: SettingKey, val: &str) {
    let msg = match key {
        SettingKey::MaxConcurrent => match val.parse::<usize>() {
            Ok(n) if (1..=32).contains(&n) => apply_global_option(
                app,
                "max-concurrent-downloads",
                &n.to_string(),
                tr("最大并发数", "max concurrent"),
            ),
            _ => tr("并发数须为 1-32 的整数", "Concurrency must be 1-32").into(),
        },
        SettingKey::SplitConnections => match val.parse::<usize>() {
            Ok(n) if (1..=128).contains(&n) => apply_global_option(
                app,
                "split",
                &n.to_string(),
                tr("预分配连接数", "split connections"),
            ),
            _ => tr(
                "连接数须为 1-128 的整数",
                "Connections must be 1-128",
            )
            .into(),
        },
        SettingKey::BtConnections => match val.parse::<usize>() {
            Ok(n) if (1..=200).contains(&n) => apply_global_option(
                app,
                "bt-max-peers",
                &n.to_string(),
                tr("BT 预分配连接数", "BT peers"),
            ),
            _ => tr("BT 连接数须为 1-200 的整数", "BT peers must be 1-200").into(),
        },
        SettingKey::MaxDownloadLimit => submit_rate_limit(
            app,
            val,
            "max-overall-download-limit",
            tr("全局下载限速", "download limit"),
        ),
        SettingKey::MaxUploadLimit => submit_rate_limit(
            app,
            val,
            "max-overall-upload-limit",
            tr("全局上传限速", "upload limit"),
        ),
        SettingKey::DownloadDir => {
            if val.is_empty() {
                tr("目录为空", "Empty directory").into()
            } else {
                apply_global_option(app, "dir", val, tr("下载目录", "download dir"))
            }
        }
        SettingKey::MaxConnPerServer => match val.parse::<u64>() {
            Ok(n) if n <= 128 => apply_global_option(
                app,
                "max-connection-per-server",
                &n.to_string(),
                tr("单服务器连接数", "conns per server"),
            ),
            _ => tr(
                "连接数须为 0-128 的整数（0 = 默认）",
                "Conns per server must be 0-128 (0 = default)",
            )
            .into(),
        },
        SettingKey::MinSplitSize => match val.parse::<u64>() {
            Ok(n) => apply_global_option(
                app,
                "min-split-size",
                &n.to_string(),
                tr("最小分片大小", "min split size"),
            ),
            _ => tr(
                "分片大小须为非负整数（字节，0 = 默认）",
                "Split size must be a non-negative integer (bytes, 0 = default)",
            )
            .into(),
        },
    };
    app.message = Some((msg, std::time::Instant::now()));
}

/// 应用全局选项，返回反馈消息。
fn apply_global_option(app: &App, key: &str, val: &str, label: String) -> String {
    app.mgr
        .change_global_option(&json!({ key: val }))
        .map(|_| {
            format!(
                "{} {label} = {val}",
                tr("已设置", "Set"),
            )
        })
        .unwrap_or_else(|e| format!("{}: {e}", tr("设置失败", "Failed to set")))
}

/// 全局限速输入上限（KB/s）：10 GiB/s，等同实际不限速。
const MAX_LIMIT_KBS: u64 = 10 * 1024 * 1024;

/// 提交限速设置：输入为 KB/s（0 = 不限制），存储为 bytes/s。
fn submit_rate_limit(app: &App, val: &str, key: &str, label: String) -> String {
    match val.parse::<u64>() {
        Ok(n) if n <= MAX_LIMIT_KBS => {
            let bytes = (n * 1024).to_string();
            app.mgr
                .change_global_option(&json!({ key: bytes }))
                .map(|_| {
                    if n == 0 {
                        format!(
                            "{}{label}{}",
                            tr("已取消", "Removed "),
                            tr("（不限速）", " limit")
                        )
                    } else {
                        format!(
                            "{} {label} = {n} KB/s",
                            tr("已设置", "Set")
                        )
                    }
                })
                .unwrap_or_else(|e| format!("{}: {e}", tr("设置失败", "Failed to set")))
        }
        _ => tr(
            "限速须为 0-{MAX_LIMIT_KBS} 的整数（KB/s，0 = 不限制）",
            "Limit must be 0-{MAX_LIMIT_KBS} KB/s (0 = unlimited)",
        )
        .replace("{MAX_LIMIT_KBS}", &MAX_LIMIT_KBS.to_string()),
    }
}

/// ←→/-/+ 直接调整设置项数值（立即生效）。
/// 并发数 1-32、HTTP 预分配连接数 1-128、BT 连接数 1-200。
fn adjust_concurrency(app: &mut App, delta: i32) {
    match app.settings_sel {
        0 => {
            let n = (app.max_concurrent as i32 + delta).clamp(1, 32) as usize;
            if n == app.max_concurrent {
                return;
            }
            let msg = apply_global_option(
                app,
                "max-concurrent-downloads",
                &n.to_string(),
                tr("最大并发数", "max concurrent"),
            );
            app.message = Some((msg, std::time::Instant::now()));
        }
        1 => {
            let n = (app.split_connections as i32 + delta).clamp(1, 128) as usize;
            if n == app.split_connections {
                return;
            }
            let msg = apply_global_option(
                app,
                "split",
                &n.to_string(),
                tr("预分配连接数", "split connections"),
            );
            app.message = Some((msg, std::time::Instant::now()));
        }
        2 => {
            let n = (app.bt_max_peers as i32 + delta).clamp(1, 200) as usize;
            if n == app.bt_max_peers {
                return;
            }
            let msg = apply_global_option(
                app,
                "bt-max-peers",
                &n.to_string(),
                tr("BT 预分配连接数", "BT peers"),
            );
            app.message = Some((msg, std::time::Instant::now()));
        }
        3 => adjust_rate_limit(app, delta, true),
        4 => adjust_rate_limit(app, delta, false),
        6 => {
            // BT 加密模式循环：优先加密 → 强制加密 → 仅明文
            let order = ["adaptive", "force", "plain"];
            let cur = order
                .iter()
                .position(|&v| v == app.bt_encryption)
                .unwrap_or(0);
            let next = ((cur as i32 + delta).rem_euclid(order.len() as i32)) as usize;
            if next == cur {
                return;
            }
            let msg = apply_global_option(
                app,
                "bt-encryption",
                order[next],
                tr("BT 加密模式", "BT encryption"),
            );
            app.message = Some((msg, std::time::Instant::now()));
        }
        7 => {
            // BT 传输协议循环：TCP+uTP → 仅TCP → 仅uTP
            let order = ["tcp+utp", "tcp", "utp"];
            let cur = order.iter().position(|&v| v == app.bt_protocol).unwrap_or(0);
            let next = ((cur as i32 + delta).rem_euclid(order.len() as i32)) as usize;
            if next == cur {
                return;
            }
            let msg = apply_global_option(
                app,
                "bt-protocol",
                order[next],
                tr("BT 传输协议", "BT transport"),
            );
            app.message = Some((msg, std::time::Instant::now()));
        }
        8 => {
            // BT 智能调度开关
            let next = !app.bt_adaptive;
            let val = if next { "true" } else { "false" };
            let msg = apply_global_option(
                app,
                "bt-adaptive",
                val,
                tr("BT 智能调度", "BT adaptive"),
            );
            app.message = Some((msg, std::time::Instant::now()));
        }
        9 => {
            let n = (app.max_conn_per_server as i32 + delta).clamp(0, 128) as u64;
            if n == app.max_conn_per_server {
                return;
            }
            let msg = apply_global_option(
                app,
                "max-connection-per-server",
                &n.to_string(),
                tr("单服务器连接数", "conns per server"),
            );
            app.message = Some((msg, std::time::Instant::now()));
        }
        10 => {
            // 最小分片大小步长 1 MiB
            let step = (1024 * 1024) as i64 * delta as i64;
            let n = ((app.min_split_size as i64 + step).max(0)) as u64;
            if n == app.min_split_size {
                return;
            }
            let msg = apply_global_option(
                app,
                "min-split-size",
                &n.to_string(),
                tr("最小分片大小", "min split size"),
            );
            app.message = Some((msg, std::time::Instant::now()));
        }
        _ => {}
    }
}

/// ←→ 调整限速：步长 100 KB/s，0 = 不限制。
fn adjust_rate_limit(app: &mut App, delta: i32, is_download: bool) {
    let current = if is_download {
        app.dl_limit_kbs
    } else {
        app.ul_limit_kbs
    };
    let step = 100i64 * delta as i64;
    let n = (current as i64 + step).clamp(0, MAX_LIMIT_KBS as i64) as u64;
    if n == current {
        return;
    }
    let (key, label) = if is_download {
        (
            "max-overall-download-limit",
            tr("全局下载限速", "download limit"),
        )
    } else {
        (
            "max-overall-upload-limit",
            tr("全局上传限速", "upload limit"),
        )
    };
    let msg = submit_rate_limit(app, &n.to_string(), key, label);
    app.message = Some((msg, std::time::Instant::now()));
}

/// 提交移除：弹窗确认后调用引擎，可选择连带删除已下载文件。
fn submit_remove(app: &mut App, gid: Gid, delete_files: bool) {
    let r = app.mgr.remove_with_files(&gid, delete_files).map(|_| {
        if delete_files {
            tr("已移除任务（含已下载文件）", "Task removed (with files)").to_string()
        } else {
            tr("已移除任务", "Task removed").to_string()
        }
    });
    let msg = r.unwrap_or_else(|e| e);
    app.message = Some((msg, std::time::Instant::now()));
    if let MainView::Detail(g) = &app.view {
        if *g == gid {
            app.view = MainView::List;
        }
    }
}

/// 对当前任务（列表选中项或详情任务）执行暂停/恢复切换。
fn app_action(app: &mut App, act: &str) {
    let Some(gid) = current_gid(app) else {
        return;
    };
    let r = match act {
        // r 切换：下载中/等待 → 暂停；已暂停 → 恢复；终态不可操作
        "toggle" => {
            let status = app
                .tasks
                .iter()
                .find(|t| t["gid"].as_str() == Some(gid.0.as_str()))
                .and_then(|t| t["status"].as_str())
                .unwrap_or("")
                .to_string();
            match status.as_str() {
                "active" | "waiting" => app
                    .mgr
                    .pause(&gid)
                    .map(|_| tr("已暂停", "Paused").to_string()),
                "paused" => app
                    .mgr
                    .unpause(&gid)
                    .map(|_| tr("已恢复", "Resumed").to_string()),
                _ => Err(tr("任务已结束，无需切换", "Task finished, nothing to toggle").into()),
            }
        }
        _ => return,
    };
    let msg = r.unwrap_or_else(|e| e);
    app.message = Some((msg, std::time::Instant::now()));
}

/// 当前操作目标：列表视图取选中项，详情视图取详情任务。
fn current_gid(app: &App) -> Option<Gid> {
    match &app.view {
        MainView::List => gid_at(app, app.selected),
        MainView::Detail(g) => Some(g.clone()),
        MainView::Settings => None,
    }
}

fn gid_at(app: &App, i: usize) -> Option<Gid> {
    filtered_indices(app)
        .get(i)
        .and_then(|&idx| app.tasks.get(idx))
        .and_then(|t| t["gid"].as_str())
        .map(Gid::from)
}

/// 主界面调度：按视图分发渲染，各弹窗叠加。
fn draw_app(f: &mut ratatui::Frame, app: &App) {
    match &app.view {
        MainView::List => draw_list(f, app),
        MainView::Detail(g) => draw_detail(f, app, g),
        MainView::Settings => draw_settings(f, app),
    }
    if app.input.is_some() {
        draw_input_popup(f, app);
    }
    if app.lang_picker.is_some() {
        draw_lang_picker(f, app);
    }
    if let Some(gid) = &app.confirm_remove {
        draw_confirm_remove_popup(f, app, gid);
    }
    if app.confirm_quit {
        draw_confirm_quit_popup(f);
    }
    if app.add_task.is_some() {
        draw_add_task_dialog(f, app);
    }
}

/// 新建任务弹窗：地址 + 目录输入 →（磁力）解析中 → 向下扩展文件选择。
///
/// 弹窗顶边固定（输入态以 6 行高度垂直居中定位），磁力解析完成后
/// 底部向下扩展出文件选择表格，顶边不再移动。
fn draw_add_task_dialog(f: &mut ratatui::Frame, app: &App) {
    use ratatui::layout::Alignment;
    use ratatui::style::{Color, Modifier, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Clear, Padding, Paragraph};

    let Some(d) = &app.add_task else {
        return;
    };
    let dim = ui_dim();
    let accent = ui_accent();
    let area_all = f.area();

    // 顶边固定：输入态高度（4 行内容 + 2 行边框）的垂直居中位置
    const BASE_H: u16 = 6;
    let y0 = area_all.height.saturating_sub(BASE_H) / 2;

    let mut lines: Vec<Line> = Vec::new();
    let title: String;
    let focused_card: bool;
    let mut area_w: u16 = (area_all.width * 7 / 10).clamp(44, 96);

    match &d.stage {
        AddStage::Input => {
            title = format!(" {} ", tr("新建任务", "New task"));
            focused_card = false;
            let url_label = tr("地址", "URL");
            let dir_label = tr("目录", "Dir");
            let label_w = disp_w(&url_label).max(disp_w(&dir_label));
            let value_w =
                area_w.saturating_sub(6) as usize - label_w - 2; // 边框+内边距+冒号+空格
            // 地址行（焦点态带光标）
            let url_focus = d.field == AddField::Url;
            let shown_url = truncate_head(&d.url, value_w);
            let mut url_spans = vec![Span::styled(
                format!("{}:{}", url_label, " ".repeat(label_w - disp_w(&url_label) + 1)),
                if url_focus { accent } else { dim },
            )];
            if url_focus {
                url_spans.push(Span::raw(shown_url));
                url_spans.push(Span::styled("▌", accent));
            } else {
                url_spans.push(Span::styled(shown_url, Style::new()));
            }
            lines.push(Line::from(url_spans));
            // 目录行（空值显示占位说明）
            let dir_focus = d.field == AddField::Dir;
            let dir_pad = " ".repeat(label_w - disp_w(&dir_label) + 1);
            let mut dir_spans = vec![Span::styled(
                format!("{dir_label}:{dir_pad}"),
                if dir_focus { accent } else { dim },
            )];
            if d.dir.is_empty() {
                dir_spans.push(Span::styled(
                    tr("（空 = 使用默认下载目录）", "(empty = default download dir)"),
                    dim,
                ));
            } else {
                dir_spans.push(Span::styled(truncate_head(&d.dir, value_w), Style::new()));
            }
            if dir_focus {
                dir_spans.push(Span::styled("▌", accent));
            }
            lines.push(Line::from(dir_spans));
            lines.push(Line::from(""));
            let action = if d.field == AddField::Dir {
                tr("选择目录", "choose folder")
            } else if d.url.trim_start().starts_with("magnet:") {
                tr("解析", "parse")
            } else {
                tr("添加", "add")
            };
            lines.push(hint_line(&[
                ("Enter", action),
                (
                    "Tab/↑↓",
                    tr("切换字段", "switch field"),
                ),
                ("Esc", tr("取消", "cancel").to_string()),
            ]));
        }
        AddStage::Parsing { gid, started } => {
            title = format!(
                " {} ",
                tr("新建任务 · 解析中", "New task · parsing")
            );
            focused_card = true;
            // 两字段灰显回显
            let url_label = tr("地址", "URL");
            let dir_label = tr("目录", "Dir");
            let label_w = disp_w(&url_label).max(disp_w(&dir_label));
            let value_w = area_w.saturating_sub(6) as usize - label_w - 2;
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{}:{}", url_label, " ".repeat(label_w - disp_w(&url_label) + 1)),
                    dim,
                ),
                Span::styled(truncate_head(&d.url, value_w), dim),
            ]));
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{dir_label}:{}", " ".repeat(label_w - disp_w(&dir_label) + 1)),
                    dim,
                ),
                Span::styled(
                    if d.dir.is_empty() {
                        tr("（默认下载目录）", "(default download dir)").to_string()
                    } else {
                        truncate_head(&d.dir, value_w)
                    },
                    dim,
                ),
            ]));
            // 解析状态行
            let t = app
                .tasks
                .iter()
                .find(|t| t["gid"].as_str() == Some(gid.0.as_str()));
            let status = t.map(|t| t["status"].as_str().unwrap_or("")).unwrap_or("");
            let conns = t.and_then(|t| t["connections"].as_u64()).unwrap_or(0);
            let state_text = match status {
                "waiting" => tr("排队等待调度…", "Queued…").to_string(),
                "active" => format!(
                    "{}（{} {}）",
                    tr("正在连接 peer 获取元数据", "Connecting peers for metadata"),
                    conns,
                    tr("个连接", "conns")
                ),
                _ => tr("准备中…", "Preparing…").to_string(),
            };
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("⟳ ", accent),
                Span::styled(state_text, Style::new()),
                Span::styled(format!(" · {}s", started.elapsed().as_secs()), dim),
            ]));
            lines.push(hint_line(&[("Esc", tr("取消解析", "cancel parse").to_string())]));
        }
        AddStage::Selecting {
            gid: _,
            name,
            files,
            checked,
            cursor,
        } => {
            title = format!(" {} ", tr("选择要下载的文件", "Select files to download"));
            focused_card = true;
            area_w = area_all.width.saturating_sub(4).clamp(40, 96);
            let n = files.len();
            // 内容固定行：名称 + 目录 + 空行 + 空行 + 汇总 + 提示 = 6，边框 2，
            // 滚动指示行预留 1 → 共 9；可见行数随剩余高度伸缩（1..=14），
            // 弹窗整体向下扩展不超屏
            let visible = (area_all.height.saturating_sub(y0 + 9) as usize)
                .clamp(1, 14)
                .min(n);
            // 行窗口跟随光标滚动
            let start = if n <= visible {
                0
            } else {
                cursor
                    .saturating_sub(visible / 2)
                    .min(n - visible)
            };
            let end = (start + visible).min(n);

            lines.push(Line::from(vec![
                Span::styled(name.clone(), Style::new().fg(Color::Cyan)),
                Span::styled(format!(" · {} {}", n, tr("个文件", "files")), dim),
            ]));
            lines.push(Line::from(vec![
                Span::styled(format!("{}: ", tr("目录", "Dir")), dim),
                Span::styled(
                    if d.dir.is_empty() {
                        tr("默认下载目录", "default download dir").to_string()
                    } else {
                        truncate_head(&d.dir, (area_w.saturating_sub(8)) as usize)
                    },
                    dim,
                ),
            ]));
            lines.push(Line::from(""));

            // 大小列宽（最长尺寸字符串）
            let size_w = files
                .iter()
                .map(|f| fmt_size(f.length).len())
                .max()
                .unwrap_or(5);
            let inner_w = area_w.saturating_sub(10) as usize; // 边框/内边距/前缀/复选框
            let name_w = inner_w.saturating_sub(size_w + 2).max(10);

            for (i, f) in files.iter().enumerate().take(end).skip(start) {
                let cur = i == *cursor;
                let on = checked.get(i).copied().unwrap_or(false);
                let check = if on { "[x]" } else { "[ ]" };
                let check_style = Style::new()
                    .fg(if on { Color::LightGreen } else { Color::Gray })
                    .add_modifier(Modifier::BOLD);
                let shown = truncate_to_width(&f.path, name_w);
                let pad = " ".repeat(name_w.saturating_sub(disp_w(&shown)));
                let prefix = if cur { "▸ " } else { "  " };
                let name_style = if cur {
                    accent.add_modifier(Modifier::BOLD)
                } else {
                    Style::new()
                };
                lines.push(Line::from(vec![
                    Span::styled(prefix, if cur { accent } else { dim }),
                    Span::styled(format!("{check} "), check_style),
                    Span::styled(shown, name_style),
                    Span::raw(pad),
                    Span::styled(" ", Style::new()),
                    Span::styled(fmt_size(f.length), dim),
                ]));
            }
            if n > visible {
                lines.push(Line::from(Span::styled(
                    format!("{} {}/{}", tr("第", "row"), cursor + 1, n),
                    dim,
                )));
            }
            lines.push(Line::from(""));
            let (sel_n, total_n, sel_bytes, total_bytes) =
                magnet_selected_summary(files, checked);
            lines.push(Line::from(vec![Span::styled(
                format!(
                    "{} {sel_n}/{total_n} · {} / {}",
                    tr("已选", "Selected"),
                    fmt_size(sel_bytes),
                    fmt_size(total_bytes)
                ),
                Style::new().fg(Color::Cyan),
            )]));
            lines.push(hint_line(&[
                ("↑↓", tr("移动", "move").to_string()),
                ("Space", tr("勾选", "toggle").to_string()),
                ("a", tr("全选/反选", "all/none").to_string()),
                ("Enter", tr("确认下载", "download").to_string()),
                ("Esc", tr("取消", "cancel").to_string()),
            ]));
        }
    }

    // 高度 = 内容行 + 边框；顶边固定，底部向下扩展
    let content_h = lines.len() as u16;
    let area_h = content_h + 2;
    let area_h = area_h.min(area_all.height.saturating_sub(y0));
    let x = (area_all.width.saturating_sub(area_w)) / 2;
    let area = ratatui::layout::Rect {
        x,
        y: y0,
        width: area_w,
        height: area_h,
    };
    f.render_widget(Clear, area);
    let block = if focused_card {
        ui_card_focused()
    } else {
        ui_card()
    };
    let popup = Paragraph::new(lines)
        .block(
            block
                .title(Span::styled(
                    title,
                    if focused_card {
                        Style::new().fg(Color::Yellow)
                    } else {
                        accent
                    },
                ))
                .padding(Padding::horizontal(2)),
        )
        .alignment(Alignment::Left);
    f.render_widget(popup, area);
}

/// 语言选择弹窗：简体中文 / 繁體中文 / English 三个选项全部摆出，
/// ↑↓ 移动，Enter 直接确认（不循环切换）。
fn draw_lang_picker(f: &mut ratatui::Frame, app: &App) {
    use ratatui::style::{Modifier, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Clear, Padding, Paragraph};

    let Some(sel) = app.lang_picker else {
        return;
    };
    let area = centered_rect(38, 8, f.area());
    f.render_widget(Clear, area);

    let options = [Lang::Zh, Lang::ZhTw, Lang::En];
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            tr("选择界面语言", "Select language"),
            ui_dim(),
        )),
        Line::from(""),
    ];
    for (i, l) in options.iter().enumerate() {
        let cur = *l == lang();
        lines.push(Line::from(vec![
            Span::raw(if i == sel { "▸ " } else { "  " }),
            Span::styled(
                lang_display_name(*l),
                if i == sel {
                    ui_accent().add_modifier(Modifier::BOLD)
                } else {
                    Style::new()
                },
            ),
            Span::raw(if cur { "  ●" } else { "" }),
        ]));
    }
    let mut foot = vec![Line::from("")];
    foot.push(Line::from(Span::styled(
        tr("↑↓ 选择 · Enter 确认 · Esc 取消", "↑↓ select · Enter confirm · Esc cancel"),
        ui_dim(),
    )));

    let body: Vec<Line> = lines.into_iter().chain(foot).collect();
    let popup = Paragraph::new(body)
        .block(
            ui_card_focused()
                .title(Span::styled(
                    format!(" {} ", tr("界面语言", "Language")),
                    Style::new().fg(ratatui::style::Color::Yellow),
                ))
                .padding(Padding::horizontal(2)),
        )
        .alignment(ratatui::layout::Alignment::Left);
    f.render_widget(popup, area);
}

/// 消息行内容（2s 内显示操作反馈，否则空行）。
fn message_line(app: &App) -> ratatui::text::Line<'static> {
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    match &app.message {
        Some((m, t)) if t.elapsed() < Duration::from_secs(2) => {
            Line::from(Span::styled(format!(" {m}"), Style::new().fg(Color::Cyan)))
        }
        _ => Line::from(""),
    }
}

/// 顶部全局信息栏：左 = 品牌 logo；右 = ↓/↑ 全局速率 │ 任务计数。
/// 列表页与详情页共用（详情页不隐藏全局信息）。
fn draw_top_bar(f: &mut ratatui::Frame, app: &App, area: ratatui::layout::Rect) {
    use ratatui::layout::{Constraint, Layout};
    use ratatui::style::{Color, Modifier, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::Paragraph;

    let dim = ui_dim();
    let accent = ui_accent();
    let stat = app.mgr.global_stat_native();

    // 顶栏：左 = 品牌；右 = ↓/↑ 全局速率 │ 任务计数
    let dl = stat["downloadSpeed"].as_u64().unwrap_or(0);
    let ul = stat["uploadSpeed"].as_u64().unwrap_or(0);
    let rate_spans: Vec<Span> = vec![
        Span::styled("↓ ", Style::new().fg(Color::LightGreen)),
        Span::styled(
            format!("{}/s", fmt_size(dl)),
            Style::new().fg(Color::LightGreen).add_modifier(Modifier::BOLD),
        ),
        // 全局上传吞吐：始终显示（无上传时显示 0，保持列位稳定）
        Span::raw("   "),
        Span::styled("↑ ", Style::new().fg(Color::LightMagenta)),
        Span::styled(
            format!("{}/s", fmt_size(ul)),
            Style::new().fg(Color::LightMagenta).add_modifier(Modifier::BOLD),
        ),
    ];
    let stat_spans: Vec<Span> = vec![
        Span::styled(tr("活动", "Active"), dim),
        Span::raw(format!(" {}   ", stat["numActive"])),
        Span::styled(tr("等待", "Waiting"), dim),
        Span::raw(format!(" {}   ", stat["numWaiting"])),
        Span::styled(tr("停止", "Stopped"), dim),
        Span::raw(format!(" {}", stat["numStopped"])),
        // 右端留 1 列间距
        Span::raw(" "),
    ];
    let rate_w: usize = rate_spans.iter().map(|s| disp_w(s.content.as_ref())).sum();
    let stat_w: usize = stat_spans.iter().map(|s| disp_w(s.content.as_ref())).sum();
    let top = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(rate_w as u16),
        Constraint::Length(2), // 分隔竖线（右侧 1 列空隙）
        Constraint::Length(stat_w as u16),
    ])
    .split(area);
    let brand = Paragraph::new(Line::from(vec![
        // 与顶/底横线左端对齐（logo 右移一点）
        Span::raw(" "),
        Span::styled(ENGINE_NAME.to_string(), accent.add_modifier(Modifier::BOLD)),
        Span::styled(format!(" v{ENGINE_VERSION}"), dim),
    ]));
    f.render_widget(brand, top[0]);
    f.render_widget(Paragraph::new(Line::from(rate_spans)), top[1]);
    // 速率与任务计数之间的分隔竖线（紧贴速率区，右侧 1 列空隙）
    f.render_widget(
        Paragraph::new(Line::from(Span::styled("│ ", dim))),
        top[2],
    );
    f.render_widget(Paragraph::new(Line::from(stat_spans)), top[3]);
}

/// 列表视图（现代下载器布局）：
/// 无边框顶栏（品牌 + 全局速率/计数，融入背景）/ 分类侧栏 + 任务表格 /
/// 消息行 / 快捷键底栏（按键加亮）。
fn draw_list(f: &mut ratatui::Frame, app: &App) {
    use ratatui::layout::{Alignment, Constraint, Layout};
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Block, List, ListState, Padding, Paragraph};

    let dim = ui_dim();

    let rows = Layout::vertical([
        Constraint::Length(1), // 顶栏（无边框，内容行 y=0）
        Constraint::Min(5),    // 任务区域
        Constraint::Length(1), // 消息行
        Constraint::Length(1), // 底栏
    ])
    .split(f.area());

    draw_top_bar(f, app, rows[0]);

    // 任务区域：左侧分类栏 + 右侧任务卡片（外框左右均为圆角，侧栏包裹在内）
    // 侧栏宽度随语言自适应（英文分类名更长）
    let sidebar_w: u16 = if lang() == Lang::En { 22 } else { 16 };
    let cols = Layout::horizontal([
        Constraint::Length(sidebar_w), // 分类侧边栏
        Constraint::Min(20),           // 任务列表
    ])
    .split(rows[1]);

    // 侧边栏：分类（每行整条背景，选中项加亮、焦点态反色，不用指针）。
    // 背景条与左右竖线各留 1 格间距（对称）；文字再右移 1 格与竖线拉开。
    let inner_w = (sidebar_w as usize).saturating_sub(2); // 左右各 1 列内边距
    let bar_bg = Color::Rgb(38, 38, 38); // 未选中项整行背景条
    let mut cat_items = vec![Line::from("")];
    let mut bar_rows: Vec<(u16, Color)> = Vec::new(); // (分类行 y, 背景色)
    for (i, c) in CATEGORIES.iter().enumerate() {
        let count = app.tasks.iter().filter(|t| c.matches(t)).count();
        let active = *c == app.category;
        let count_s = format!("({count})");
        let pad = inner_w
            .saturating_sub(2 + disp_w(c.label().as_str()) + disp_w(&count_s))
            .max(1);
        let bg = if active {
            if app.sidebar_focus {
                Color::Cyan
            } else {
                // 默认选中项：背景高亮
                Color::DarkGray
            }
        } else {
            bar_bg
        };
        let style = if active && app.sidebar_focus {
            Style::new().fg(Color::Black).bg(bg)
        } else {
            dim.bg(bg)
        };
        // 首格（x=1）留在背景条外：与左竖线留 1 格间距，和右侧对称
        cat_items.push(Line::from(vec![
            Span::raw(" "),
            Span::styled(format!(" {}{}{}", c.label(), " ".repeat(pad), count_s), style),
        ]));
        bar_rows.push((rows[1].y + 1 + i as u16, bg));
    }
    let sidebar =
        List::new(cat_items).block(Block::default().padding(Padding::horizontal(1)));
    f.render_widget(sidebar, cols[0]);

    // 任务列表（按当前分类过滤）：圆角边框（横线横跨全宽、左右均圆角）+ 列对齐表格
    let shown = filtered_indices(app);
    draw_main_borders(f, rows[1], cols[1].x);
    // 背景条补全：CJK 宽字符的续格会被 ratatui reset 掉背景（条裂成数段），
    // 逐格 set_bg 补齐（只改背景、保留字符与前景色）。
    // 条范围 x=2..=sidebar_w-2：与左右竖线各留 1 格，不触碰边框。
    let bar_last = rows[1].bottom().saturating_sub(1); // 底边框行不含
    let bar_x0 = cols[0].x + 2;
    let bar_x1 = cols[0].x + sidebar_w - 2;
    for (y, bg) in &bar_rows {
        if *y >= bar_last {
            break;
        }
        #[allow(unused_imports)]
        use std::io::Write as _;
        eprintln!(
            "[dbg] fill y={y} x={}..={}",
            bar_x0, bar_x1
        );
        for x in bar_x0..=bar_x1 {
            if let Some(cell) = f.buffer_mut().cell_mut((x, *y)) {
                cell.set_bg(*bg);
            }
        }
    }
    let content_block = Block::default().padding(Padding::new(2, 2, 1, 1));
    if shown.is_empty() {
        // 空状态：文字在任务框内垂直 + 水平居中
        let inner = content_block.inner(cols[1]);
        let mid = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .split(inner)[1];
        let hint = if app.tasks.is_empty() {
            tr("（无任务，按 a 添加下载）", "(no tasks, press a to add)")
        } else {
            tr("（该分类无任务）", "(no tasks in this category)")
        };
        let empty = Paragraph::new(Line::from(Span::styled(hint, dim)))
            .alignment(Alignment::Center);
        f.render_widget(empty, mid);
    } else {
        // 列宽：状态 / 进度(条+百分比) / 文件名(吃剩余宽度) / 大小 / 速度。
        // 全部经 pad_str 按显示宽度对齐（CJK 名不再把后续列顶歪），
        // 表头与数据行同宽 → 列严格对上。
        // 行宽 = inner 再左右各缩 1 格 → 与左右竖线间距一致（对称）
        let inner_w = (cols[1].width as usize).saturating_sub(4); // 左右内边距 2+2
        let row_w = inner_w.saturating_sub(1);
        const ST_W: usize = 8; // 状态
        const BAR_W: usize = 12; // 进度条
        const PCT_W: usize = 6; // 百分比
        const SIZE_W: usize = 23; // 大小（"999.9 GiB / 999.9 GiB"）
        const SPD_W: usize = 10; // 速度（"999.9MiB/s"）
        let name_w = row_w
            .saturating_sub(ST_W + 1 + BAR_W + 1 + PCT_W + 1 + SIZE_W + 1 + SPD_W)
            .max(10);

        // 表头
        let header_line = Line::from(vec![
            Span::styled(pad_str(&tr("状态", "Status"), ST_W), dim),
            Span::raw(" "),
            Span::styled(pad_str(&tr("进度", "Progress"), BAR_W + PCT_W), dim),
            Span::raw(" "),
            Span::styled(pad_str(&tr("文件名", "Name"), name_w), dim),
            Span::raw(" "),
            Span::styled(pad_str(&tr("大小", "Size"), SIZE_W), dim),
            Span::raw(" "),
            Span::styled(pad_str(&tr("速度", "Speed"), SPD_W), dim),
        ]);

        // 数据行：各列与表头同宽
        let item_lines: Vec<Line> = shown
            .iter()
            .map(|&idx| {
                let t = &app.tasks[idx];
                let status = t["status"].as_str().unwrap_or("-");
                let completed = t["completedLength"].as_u64().unwrap_or(0);
                let total = t["totalLength"].as_u64().unwrap_or(0);
                let speed = t["downloadSpeed"].as_u64().unwrap_or(0);
                let name = t["files"][0]["path"]
                    .as_str()
                    .unwrap_or("")
                    .rsplit(['/', '\\'])
                    .next()
                    .unwrap_or("")
                    .to_string();
                let pct = if total > 0 {
                    (completed as f64 / total as f64).min(1.0)
                } else {
                    0.0
                };
                let speed_s = if status == "active" {
                    format!("{}/s", fmt_size(speed))
                } else {
                    "--".to_string()
                };
                let size_s = format!(
                    "{} / {}",
                    fmt_size(completed),
                    if total > 0 {
                        fmt_size(total)
                    } else {
                        "?".to_string()
                    }
                );
                Line::from(vec![
                    Span::styled(pad_str(status, ST_W), status_style(status)),
                    Span::raw(" "),
                    Span::styled(mini_bar(pct, BAR_W), gauge_style(status)),
                    Span::styled(pad_str(&format!("{:>5.1}%", pct * 100.0), PCT_W), dim),
                    Span::raw(" "),
                    Span::styled(pad_str(&name, name_w), Style::new()),
                    Span::raw(" "),
                    Span::styled(pad_str(&size_s, SIZE_W), Style::new()),
                    Span::raw(" "),
                    Span::styled(pad_str(&speed_s, SPD_W), Style::new()),
                ])
            })
            .collect();

        // 表头 + 分隔横线（与表头/数据行同宽，左右各距竖线 1 格）+ 数据行
        let sep_line = Line::from(Span::styled(
            "─".repeat(row_w),
            Style::new().fg(Color::DarkGray),
        ));
        let mut items = vec![header_line, sep_line];
        items.extend(item_lines);
        let list = List::new(items)
            .block(content_block)
            .highlight_style(Style::new().bg(Color::DarkGray));
        let mut ls = ListState::default();
        ls.select(Some(app.selected + 2));
        f.render_stateful_widget(list, cols[1], &mut ls);
    }

    // 消息行（2s 内显示操作反馈）
    f.render_widget(Paragraph::new(message_line(app)), rows[2]);

    // 底栏：按键加亮 + 说明弱化
    let foot = Paragraph::new(hint_line(&[
        ("1-5", tr("分类", "category")),
        ("Tab", tr("侧栏", "sidebar")),
        ("a", tr("添加", "add")),
        ("Enter", tr("详情", "detail")),
        ("↑↓", tr("选择", "select")),
        ("r", tr("暂停/继续", "pause/resume")),
        ("x", tr("移除", "remove")),
        ("c", tr("清除完成", "clear")),
        ("s", tr("设置", "settings")),
        ("q", tr("退出", "quit")),
    ]))
    .alignment(Alignment::Center);
    f.render_widget(foot, rows[3]);
}

/// 列表行迷你进度条（w 格块字符）。
fn mini_bar(pct: f64, w: usize) -> String {
    let filled = ((pct * w as f64).round() as usize).min(w);
    format!("{}{}", "█".repeat(filled), "░".repeat(w - filled))
}

/// 详情视图：复用下载界面的仪表布局，底栏换为返回提示。
fn draw_detail(f: &mut ratatui::Frame, app: &App, gid: &Gid) {
    use ratatui::layout::{Alignment, Constraint, Layout};
    use ratatui::widgets::Paragraph;

    let last = app
        .tasks
        .iter()
        .find(|t| t["gid"].as_str() == Some(gid.0.as_str()))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let hist = app.hist.get(&gid.0).cloned().unwrap_or_default();
    let st = TuiState { last, hist };
    let footer = tr(
        " Esc/回车 返回 · Tab 切换焦点 · ↑↓←→ 滚动 · PgUp/PgDn 翻页 · r 暂停/继续 · t 添加tracker · x 移除 · q 退出 ",
        " Esc/Enter back · Tab focus · ↑↓←→ scroll · PgUp/PgDn page · r pause/resume · t add tracker · x remove · q quit ",
    );

    let is_bt = app.mgr.is_bt_task(gid);
    if !is_bt {
        // 非 BT 详情：顶部全局信息栏（1 行）+ 原全屏视图（区域整体下移 1 行）
        let areas = Layout::vertical([Constraint::Length(1), Constraint::Min(5)]).split(f.area());
        draw_top_bar(f, app, areas[0]);
        let sub = ratatui::layout::Rect {
            y: f.area().y + 1,
            height: f.area().height.saturating_sub(1),
            ..f.area()
        };
        draw_in_area(f, &st, gid, &footer, sub, true);
        return;
    }

    // BT 任务：顶部全局信息栏 + 上方主视图 + 中间 tracker 列表 + 下方 peer 列表 + 底栏
    let areas = Layout::vertical([
        Constraint::Length(1), // 顶栏：全局信息（品牌 + 速率/计数）
        Constraint::Min(18),   // 上方：任务信息 + 进度 + 速度图
        Constraint::Min(4),    // 中间：tracker 列表
        Constraint::Min(6),    // 下方：peer 列表
        Constraint::Length(1), // 底栏
    ])
    .split(f.area());

    draw_top_bar(f, app, areas[0]);
    draw_in_area(f, &st, gid, &footer, areas[1], false);
    draw_detail_trackers(f, app, areas[2]);
    draw_detail_peers(f, app, areas[3]);

    // 底栏：按键加亮 + 说明弱化
    let foot = Paragraph::new(hint_line(&[
        ("Esc/Enter", tr("返回", "back")),
        ("Tab", tr("切焦点", "focus")),
        ("↑↓←→", tr("滚动", "scroll")),
        ("PgUp/PgDn", tr("翻页", "page")),
        ("r", tr("暂停/继续", "pause/resume")),
        ("t", tr("加tracker", "add tracker")),
        ("x", tr("移除", "remove")),
        ("q", tr("退出", "quit")),
    ]))
    .alignment(Alignment::Center);
    f.render_widget(foot, areas[4]);
}

/// 详情视图 tracker 列表区块（支持上下/横向滚动，聚焦卡片亮边框标记）。
fn draw_detail_trackers(f: &mut ratatui::Frame, app: &App, area: ratatui::layout::Rect) {
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Padding, Paragraph};

    let dim = ui_dim();
    let accent = ui_accent();
    let trackers = &app.detail_trackers;
    let focused = !app.detail_focus_peers;

    let block = if focused {
        ui_card_focused()
    } else {
        ui_card()
    }
    .title(Span::styled(
        format!(" Trackers（{}）", trackers.len()),
        if focused { Style::new().fg(Color::Yellow) } else { accent },
    ))
    .padding(Padding::horizontal(1));

    if trackers.is_empty() {
        let p = Paragraph::new(Line::from(Span::styled(
            tr(
                "（无 tracker，前往设置页添加或订阅）",
                "(no trackers; add in Settings or subscribe)",
            ),
            dim,
        )))
        .block(block)
        .alignment(ratatui::layout::Alignment::Center);
        f.render_widget(p, area);
    } else {
        let lines: Vec<Line> = trackers
            .iter()
            .enumerate()
            .map(|(i, url)| {
                Line::from(vec![
                    Span::styled(format!("{:>3}. ", i + 1), dim),
                    Span::raw(url.clone()),
                ])
            })
            .collect();
        // 滚动钳制：行不超过末行；列保留至少 1 列可见
        let content_w = trackers
            .iter()
            .map(|u| disp_w(u))
            .max()
            .unwrap_or(0)
            .saturating_sub(1) as u16;
        let scroll_y = app.tracker_scroll.0.min(trackers.len() as u16);
        let scroll_x = app.tracker_scroll.1.min(content_w);
        let p = Paragraph::new(lines).block(block).scroll((scroll_y, scroll_x));
        f.render_widget(p, area);
    }
}

/// 详情视图 peer 列表区块。
///
/// 列宽策略：除 Client 外全部定宽，Client 吃掉终端剩余宽度（修复旧版
/// 固定 16 字符截断 + 右侧大片空白）；终端过窄时按优先级从尾部丢列。
fn draw_detail_peers(f: &mut ratatui::Frame, app: &App, area: ratatui::layout::Rect) {
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Padding, Paragraph};

    #[derive(Clone, Copy, PartialEq)]
    enum Col {
        Ip,
        Country,
        Client,
        Enc,
        Proto,
        Time,
        Up,
        Down,
        Prog,
        Source,
        Flags,
    }

    let dim = ui_dim();
    let green = Style::new().fg(Color::Green);
    let yellow = Style::new().fg(Color::Yellow);
    let accent = ui_accent();
    let peers = &app.detail_peers;
    let focused = app.detail_focus_peers;

    let block = if focused {
        ui_card_focused()
    } else {
        ui_card()
    }
    .title(Span::styled(
        format!(" Peers（{}）", peers.len()),
        if focused { Style::new().fg(Color::Yellow) } else { accent },
    ))
    .padding(Padding::horizontal(1));

    if peers.is_empty() {
        let p = Paragraph::new(Line::from(Span::styled(
            tr("（暂无已连接的 peer）", "(no connected peers)"),
            dim,
        )))
        .block(block)
        .alignment(ratatui::layout::Alignment::Center);
        f.render_widget(p, area);
        return;
    }

    const GAP: usize = 2;
    // (列, 表头, 定宽)；Client 宽度后置计算。丢列顺序 = 从尾部开始。
    // 定宽合计 + 间隔 = 表头与单元格公用的列宽（i18n 表头同样按显示宽度对齐）。
    let mut cols: Vec<(Col, String, usize)> = vec![
        (Col::Ip, tr("IP:Port", "IP:Port"), 21),
        (Col::Country, tr("国家/地区", "Country"), 10),
        (Col::Client, tr("客户端", "Client"), 14),
        (Col::Enc, tr("加密", "Enc"), 6),
        (Col::Proto, tr("协议", "Proto"), 5),
        (Col::Time, tr("时长", "Time"), 8),
        (Col::Up, tr("上传", "Up"), 7),
        (Col::Down, tr("下载", "Down"), 7),
        (Col::Prog, tr("进度", "Prog"), 6),
        (Col::Source, tr("来源", "Source"), 8),
        (Col::Flags, tr("标志", "Flag"), 4),
    ];
    let inner_w = (area.width as usize).saturating_sub(4); // 边框 2 + 水平内边距 2
    while cols.len() > 3 {
        let need = cols.iter().map(|c| c.2).sum::<usize>() + GAP * (cols.len() - 1);
        if need <= inner_w {
            break;
        }
        cols.pop();
    }
    // Client 吃剩余宽度（保底 10 列）
    let fixed_need: usize = cols
        .iter()
        .filter(|(k, _, _)| *k != Col::Client)
        .map(|c| c.2)
        .sum::<usize>()
        + GAP * (cols.len() - 1);
    let client_w = inner_w.saturating_sub(fixed_need).max(10);
    for (k, _, w) in cols.iter_mut() {
        if *k == Col::Client {
            *w = client_w;
        }
    }

    let cell = |p: &PeerRow, k: Col| -> (String, Style) {
        match k {
            Col::Ip => (p.addr.clone(), if p.connected { green } else { dim }),
            Col::Country => (p.country.clone(), Style::new()),
            Col::Client => {
                let c = if p.client.is_empty() { "-" } else { &p.client };
                (c.to_string(), Style::new())
            }
            Col::Enc => {
                if p.encrypted {
                    (tr("加密", "enc").to_string(), green)
                } else {
                    (tr("明文", "plain").to_string(), dim)
                }
            }
            Col::Proto => (p.protocol.to_uppercase(), Style::new()),
            Col::Time => (fmt_duration(p.connected_secs), dim),
            Col::Up => (format_bytes(p.uploaded), Style::new()),
            Col::Down => (format_bytes(p.downloaded), Style::new()),
            Col::Prog => (
                match p.progress {
                    Some(v) => format!("{:.1}%", v),
                    None => "-".to_string(),
                },
                Style::new(),
            ),
            Col::Source => (
                p.source.clone(),
                if p.source == "tracker" { yellow } else { dim },
            ),
            Col::Flags => (if p.seed { "seed".to_string() } else { String::new() }, green),
        }
    };

    let mut lines: Vec<Line> = Vec::with_capacity(peers.len() + 1);
    let header_spans: Vec<Span> = cols
        .iter()
        .enumerate()
        .flat_map(|(i, (_, h, w))| {
            let mut v = vec![Span::styled(pad_str(h, *w), dim)];
            if i + 1 < cols.len() {
                v.push(Span::raw(" ".repeat(GAP)));
            }
            v
        })
        .collect();
    lines.push(Line::from(header_spans));
    for p in peers {
        let spans: Vec<Span> = cols
            .iter()
            .enumerate()
            .flat_map(|(i, (k, _, w))| {
                let (text, style) = cell(p, *k);
                let mut v = vec![Span::styled(pad_str(&truncate_to_width(&text, *w), *w), style)];
                if i + 1 < cols.len() {
                    v.push(Span::raw(" ".repeat(GAP)));
                }
                v
            })
            .collect();
        lines.push(Line::from(spans));
    }
    // 滚动偏移：行不超过最后一行，列保留至少 1 列可见
    let content_w: usize = cols.iter().map(|c| c.2).sum::<usize>() + GAP * (cols.len() - 1);
    let scroll_y = app.peer_scroll.0.min(peers.len() as u16);
    let scroll_x = app.peer_scroll.1.min(content_w.saturating_sub(1) as u16);
    let p = Paragraph::new(lines)
        .block(block)
        .scroll((scroll_y, scroll_x));
    f.render_widget(p, area);
}

/// 将字节数格式化为人类可读的简短字符串（如 1.2M, 345K）。
fn format_bytes(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}G", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.0}K", n as f64 / 1_000.0)
    } else {
        format!("{}B", n)
    }
}

/// 将字符串截断到指定显示宽度并右侧补空格对齐（感知 CJK 全角宽度）。
fn pad_str(s: &str, width: usize) -> String {
    use unicode_width::UnicodeWidthStr;
    let w = s.width();
    if w <= width {
        let mut out = String::from(s);
        out.push_str(&" ".repeat(width - w));
        return out;
    }
    truncate_to_width(s, width)
}

/// 按显示宽度截断字符串（不补齐空格），全角字符按 2 列计。
fn truncate_to_width(s: &str, width: usize) -> String {
    use unicode_width::UnicodeWidthChar;
    let mut out = String::new();
    let mut used = 0usize;
    for ch in s.chars() {
        let cw = ch.width().unwrap_or(0);
        if used + cw > width {
            break;
        }
        out.push(ch);
        used += cw;
    }
    out
}

/// 从 "ip:port"（IPv6 形如 "[::1]:6881"）中取出纯 IP。
fn ip_of_addr(addr: &str) -> &str {
    let ip = addr.rsplit_once(':').map(|(ip, _)| ip).unwrap_or(addr);
    ip.trim_start_matches('[').trim_end_matches(']')
}

/// GeoIP 库句柄（懒加载，加载失败记 None 不再重试）。
static GEO_V4: OnceLock<Option<xfer_geo::GeoDb>> = OnceLock::new();
static GEO_V6: OnceLock<Option<xfer_geo::GeoDb>> = OnceLock::new();
/// IP → 国家/地区文本缓存（300ms 刷新下避免重复查库）。
static GEO_CACHE: OnceLock<Mutex<std::collections::HashMap<String, String>>> = OnceLock::new();

fn load_geo_db(file: &str) -> Option<xfer_geo::GeoDb> {
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(dir) = std::env::var("XFER_GEO_DIR") {
        candidates.push(std::path::PathBuf::from(dir).join(file));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("data").join(file));
            candidates.push(dir.join("../data").join(file));
        }
    }
    candidates.push(std::path::PathBuf::from("data").join(file));
    for c in candidates {
        if let Ok(db) = xfer_geo::GeoDb::load(&c) {
            return Some(db);
        }
    }
    None
}

/// 查询 IP 的国家/地区显示文本；库缺失或无记录返回 "-"。
fn geo_lookup(ip: &str) -> String {
    if ip.is_empty() {
        return "-".to_string();
    }
    let cache = GEO_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    if let Some(v) = cache.lock().unwrap().get(ip) {
        return v.clone();
    }
    let is_v6 = ip.contains(':');
    let slot = if is_v6 { &GEO_V6 } else { &GEO_V4 };
    let db = slot.get_or_init(|| {
        load_geo_db(if is_v6 { "ip2region_v6.xdb" } else { "ip2region_v4.xdb" })
    });
    let display = db
        .as_ref()
        .and_then(|d| d.search(ip))
        .map(|r| {
            if !r.country.is_empty() {
                r.country
            } else if !r.province.is_empty() {
                r.province
            } else if !r.city.is_empty() {
                r.city
            } else {
                "-".to_string()
            }
        })
        .unwrap_or_else(|| "-".to_string());
    cache.lock().unwrap().insert(ip.to_string(), display.clone());
    display
}

/// 设置视图：可编辑项（并发/连接/限速/目录）+ Tracker 服务器列表 + 引擎信息 / 消息行 / 快捷键底栏。
fn draw_settings(f: &mut ratatui::Frame, app: &App) {
    use ratatui::layout::{Alignment, Constraint, Layout};
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{List, ListState, Padding, Paragraph};

    let dim = ui_dim();
    let accent = ui_accent();

    let rows = Layout::vertical([
        Constraint::Min(10),   // 设置面板
        Constraint::Length(1), // 消息行
        Constraint::Length(1), // 底栏
    ])
    .split(f.area());

    // 分区卡片：参数 / Tracker 服务器 / 订阅源 / 引擎信息
    // （聚焦分区亮边框 + 黄色标题，替代旧的"◄ 焦点"文字标记）
    let areas = Layout::vertical([
        Constraint::Length(15), // 参数（13 行 + 边框 2）
        Constraint::Length(1),  // 空行
        Constraint::Min(5),     // Tracker 服务器列表
        Constraint::Length(1),  // 空行
        Constraint::Min(5),     // 订阅源列表
        Constraint::Length(1),  // 空行
        Constraint::Length(5),  // 引擎信息（3 行 + 边框 2）
    ])
    .split(rows[0]);

    // 参数卡片
    let param_focused = app.settings_area == 0;
    let param_block = if param_focused {
        ui_card_focused()
    } else {
        ui_card()
    }
    .title(Span::styled(
        format!(" {} ", tr("参数", "Params")),
        if param_focused {
            Style::new().fg(Color::Yellow)
        } else {
            accent
        },
    ))
    .padding(Padding::horizontal(2));
    let param_inner = param_block.inner(areas[0]);
    f.render_widget(param_block, areas[0]);

    // 可编辑项：值列宽 = 内宽 - 选中标记(2) - 标签列
    let val_w = param_inner.width.saturating_sub(2 + label_col_w() as u16) as usize;
    let limit_str = |kbs: u64| {
        if kbs == 0 {
            tr("不限制", "unlimited").to_string()
        } else {
            format!("{kbs} KB/s")
        }
    };
    let enc_str = match app.bt_encryption.as_str() {
        "force" => tr("强制加密", "Force"),
        "plain" => tr("仅明文", "Plain only"),
        _ => tr("优先加密", "Prefer encryption"),
    };
    let proto_str = match app.bt_protocol.as_str() {
        "tcp" => tr("仅 TCP", "TCP only"),
        "utp" => tr("仅 uTP", "uTP only"),
        _ => tr("TCP + uTP", "TCP + uTP"),
    };
    let size_str = |n: u64| {
        if n == 0 {
            // 0 = 引擎默认：直接显示默认值而不是「默认」文字
            format_bytes(xfer_engine::DEFAULT_MIN_SPLIT_SIZE)
        } else {
            format_bytes(n)
        }
    };
    let conn_str = if app.max_conn_per_server == 0 {
        // 0 = 引擎默认：直接显示默认值而不是「默认」文字
        xfer_engine::DEFAULT_SPLIT_CONNECTIONS.to_string()
    } else {
        app.max_conn_per_server.to_string()
    };
    let vals = [
        app.max_concurrent.to_string(),
        app.split_connections.to_string(),
        app.bt_max_peers.to_string(),
        limit_str(app.dl_limit_kbs),
        limit_str(app.ul_limit_kbs),
        truncate_head(&app.download_dir, val_w),
        enc_str.to_string(),
        proto_str.to_string(),
        (if app.bt_adaptive {
            tr("开", "on")
        } else {
            tr("关", "off")
        })
        .to_string(),
        conn_str,
        size_str(app.min_split_size),
        lang_display_name(lang()).to_string(),
    ];
    let items: Vec<Line> = vals
        .iter()
        .enumerate()
        .map(|(i, val)| {
            let selected = i == app.settings_sel && app.settings_area == 0;
            let label = match i {
                0 => tr("最大并发下载数", "Max concurrent"),
                1 => tr("预分配连接数", "Split connections"),
                2 => tr("BT 连接数", "BT peers"),
                3 => tr("全局下载限速", "Download limit"),
                4 => tr("全局上传限速", "Upload limit"),
                5 => tr("下载目录", "Download dir"),
                6 => tr("BT 加密模式", "BT encryption"),
                7 => tr("BT 传输协议", "BT transport"),
                8 => tr("BT 智能调度", "BT adaptive"),
                9 => tr("单服务器连接数", "Conns per server"),
                10 => tr("最小分片大小", "Min split size"),
                _ => tr("界面语言", "Language"),
            };
            Line::from(vec![
                Span::raw(if selected { "▸ " } else { "  " }),
                Span::styled(pad_label(&label), dim),
                Span::raw(val.clone()),
            ])
        })
        .collect();
    let list = List::new(items).highlight_style(Style::new().bg(Color::DarkGray));
    let mut ls = ListState::default();
    if app.settings_area == 0 {
        ls.select(Some(app.settings_sel));
    }
    f.render_stateful_widget(list, param_inner, &mut ls);

    // Tracker 服务器卡片
    let trackers = &app.global_trackers;
    let tracker_focused = app.settings_area == 1;
    let tracker_block = if tracker_focused {
        ui_card_focused()
    } else {
        ui_card()
    }
    .title(Span::styled(
        format!(
            " {}（{}）",
            tr("Tracker 服务器", "Tracker servers"),
            trackers.len()
        ),
        if tracker_focused {
            Style::new().fg(Color::Yellow)
        } else {
            accent
        },
    ))
    .padding(Padding::horizontal(1));
    let tracker_inner = tracker_block.inner(areas[2]);
    f.render_widget(tracker_block, areas[2]);

    if trackers.is_empty() {
        let p = Paragraph::new(Line::from(Span::styled(
            tr("（无 tracker，按 a 添加）", "(no trackers, press a to add)"),
            dim,
        )))
        .alignment(Alignment::Center);
        f.render_widget(p, tracker_inner);
    } else {
        let lines: Vec<Line> = trackers
            .iter()
            .enumerate()
            .map(|(i, url)| {
                let selected = i == app.tracker_sel && app.settings_area == 1;
                Line::from(vec![
                    Span::raw(if selected { "▸ " } else { "  " }),
                    Span::raw(url.clone()),
                ])
            })
            .collect();
        // 滚动跟随选中项（选中项滚出可视区时下移偏移）
        let visible = tracker_inner.height as usize;
        let offset = if visible > 0 {
            app.tracker_sel.saturating_sub(visible - 1)
        } else {
            0
        } as u16;
        let p = Paragraph::new(lines).scroll((offset, 0));
        f.render_widget(p, tracker_inner);
    }

    // 订阅源卡片
    let subs = &app.subscriptions;
    let sub_focused = app.settings_area == 2;
    let sub_block = if sub_focused {
        ui_card_focused()
    } else {
        ui_card()
    }
    .title(Span::styled(
        format!(" {}（{}）", tr("订阅源", "Subscriptions"), subs.len()),
        if sub_focused {
            Style::new().fg(Color::Yellow)
        } else {
            accent
        },
    ))
    .padding(Padding::horizontal(1));
    let sub_inner = sub_block.inner(areas[4]);
    f.render_widget(sub_block, areas[4]);

    if subs.is_empty() {
        let p = Paragraph::new(Line::from(Span::styled(
            tr("（无订阅源，按 a 添加）", "(no subscriptions, press a to add)"),
            dim,
        )))
        .alignment(Alignment::Center);
        f.render_widget(p, sub_inner);
    } else {
        let green = Style::new().fg(Color::Green);
        let red = Style::new().fg(Color::Red);
        let lines: Vec<Line> = subs
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let selected = i == app.sub_sel && app.settings_area == 2;
                let status_span = if !s.last_error.is_empty() {
                    Span::styled(format!("✗ {}", s.last_count), red)
                } else if s.enabled {
                    Span::styled(format!("● {}", s.last_count), green)
                } else {
                    Span::styled(format!("○ {}", s.last_count), dim)
                };
                let name_display = if s.name.len() > 30 {
                    format!("{}…", &s.name[..27])
                } else {
                    s.name.clone()
                };
                Line::from(vec![
                    Span::raw(if selected { "▸ " } else { "  " }),
                    Span::raw(format!("{:<30}", name_display)),
                    Span::raw(" "),
                    status_span,
                ])
            })
            .collect();
        // 滚动跟随选中项
        let visible = sub_inner.height as usize;
        let offset = if visible > 0 {
            app.sub_sel.saturating_sub(visible - 1)
        } else {
            0
        } as u16;
        let p = Paragraph::new(lines).scroll((offset, 0));
        f.render_widget(p, sub_inner);
    }

    // 引擎信息卡片（只读）
    let engine_block = ui_card()
        .title(Span::styled(
            format!(" {} ", tr("引擎", "Engine")),
            accent,
        ))
        .padding(Padding::horizontal(2));
    let engine_inner = engine_block.inner(areas[6]);
    f.render_widget(engine_block, areas[6]);
    let info = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(pad_label(&tr("引擎", "Engine")), dim),
            Span::raw(format!("{ENGINE_NAME} v{ENGINE_VERSION}")),
        ]),
        Line::from(vec![
            Span::styled(pad_label(&tr("运行时间", "Uptime")), dim),
            Span::raw(fmt_duration(app.started.elapsed().as_secs())),
        ]),
    ]);
    f.render_widget(info, engine_inner);

    // 消息行
    f.render_widget(Paragraph::new(message_line(app)), rows[1]);

    // 底栏：按焦点分区给出对应快捷键（按键加亮 + 说明弱化）
    let foot = match app.settings_area {
        0 => Paragraph::new(hint_line(&[
            ("↑↓", tr("选择", "select")),
            ("←→/-/+", tr("调整/切换", "adjust")),
            ("Enter", tr("输入", "edit")),
            ("Tab", tr("Tracker", "Trackers")),
            ("Esc", tr("返回", "back")),
            ("q", tr("退出", "quit")),
        ])),
        1 => Paragraph::new(hint_line(&[
            ("↑↓", tr("选择", "select")),
            ("a", tr("添加", "add")),
            ("d", tr("删除", "delete")),
            ("Tab", tr("订阅源", "Subscriptions")),
            ("Esc", tr("返回", "back")),
            ("q", tr("退出", "quit")),
        ])),
        _ => Paragraph::new(hint_line(&[
            ("↑↓", tr("选择", "select")),
            ("a", tr("添加", "add")),
            ("d", tr("删除", "delete")),
            ("t", tr("切换", "toggle")),
            ("r", tr("刷新", "refresh")),
            ("R", tr("全部刷新", "refresh all")),
            ("Esc", tr("返回", "back")),
            ("q", tr("退出", "quit")),
        ])),
    }
    .alignment(Alignment::Center);
    f.render_widget(foot, rows[2]);
}

/// 字符串显示宽度（ASCII 1 列，其余按 2 列计）。
fn disp_w(s: &str) -> usize {
    s.chars().map(|c| if c.is_ascii() { 1 } else { 2 }).sum()
}

/// 设置页标签列宽度（英文标签更长）。
fn label_col_w() -> usize {
    if lang() == Lang::En {
        26
    } else {
        14
    }
}

/// 标签按显示宽度（中文 2 列）补齐到标签列宽。
fn pad_label(s: &str) -> String {
    let w = disp_w(s);
    let pad = label_col_w().saturating_sub(w) / 2;
    format!("{s}{}", "　".repeat(pad))
}

/// 超宽字符串保留尾部（路径尾部更有意义），前缀省略号。
fn truncate_head(s: &str, max: usize) -> String {
    let width = |c: char| if c.is_ascii() { 1 } else { 2 };
    let w: usize = s.chars().map(width).sum();
    if w <= max || max < 2 {
        return s.to_string();
    }
    let mut keep: Vec<char> = Vec::new();
    let mut w = 1; // … 占 1 列
    for c in s.chars().rev() {
        let cw = width(c);
        if w + cw > max {
            break;
        }
        keep.push(c);
        w += cw;
    }
    keep.reverse();
    format!("…{}", keep.into_iter().collect::<String>())
}

/// 输入弹窗：居中显示 URL / 设置值输入框。
fn draw_input_popup(f: &mut ratatui::Frame, app: &App) {
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Clear, Padding, Paragraph};

    let Some((kind, buf)) = &app.input else {
        return;
    };
    let title = match kind {
        InputKind::EditSetting(SettingKey::MaxConcurrent) => {
            tr(" 最大并发下载数（1-32） ", " Max concurrent downloads (1-32) ")
        }
        InputKind::EditSetting(SettingKey::SplitConnections) => tr(
            " 预分配连接数（1-128，自适应上限） ",
            " Split connections (1-128, adaptive cap) ",
        ),
        InputKind::EditSetting(SettingKey::BtConnections) => tr(
            " BT 预分配连接数（1-200，智能调度上限） ",
            " BT peers (1-200, adaptive cap) ",
        ),
        InputKind::EditSetting(SettingKey::MaxDownloadLimit) => tr(
            " 全局下载限速（KB/s，0 = 不限制） ",
            " Download limit (KB/s, 0 = unlimited) ",
        ),
        InputKind::EditSetting(SettingKey::MaxUploadLimit) => tr(
            " 全局上传限速（KB/s，0 = 不限制） ",
            " Upload limit (KB/s, 0 = unlimited) ",
        ),
        InputKind::EditSetting(SettingKey::DownloadDir) => {
            tr(" 下载目录（新任务生效） ", " Download dir (new tasks) ")
        }
        InputKind::EditSetting(SettingKey::MaxConnPerServer) => {
            tr(" 单服务器连接数（0 = 默认） ", " Conns per server (0 = default) ")
        }
        InputKind::EditSetting(SettingKey::MinSplitSize) => tr(
            " 最小分片大小（字节，0 = 默认） ",
            " Min split size (bytes, 0 = default) ",
        ),
        InputKind::AddTracker(_) => tr(
            " 添加 Tracker（支持空格/逗号分隔批量输入） ",
            " Add trackers (space/comma separated) ",
        ),
        InputKind::AddGlobalTracker => tr(
            " 添加 Tracker 服务器（全局，支持空格/逗号分隔） ",
            " Add global trackers (space/comma separated) ",
        ),
        InputKind::AddSubscription => tr(
            " 添加订阅源（格式：名称 URL，或仅输入 URL） ",
            " Add subscription (name URL, or just URL) ",
        ),
    };
    let area = centered_rect(70, 3, f.area());
    f.render_widget(Clear, area);
    let input = Paragraph::new(Line::from(vec![
        Span::raw(buf.clone()),
        Span::styled("▌", ui_accent()),
    ]))
    .block(
        ui_card()
            .title(Span::styled(title, ui_accent()))
            .padding(Padding::horizontal(1)),
    );
    f.render_widget(input, area);
    // 提示行放在弹窗正下方
    if area.y + area.height < f.area().height {
        let hint_area = ratatui::layout::Rect {
            x: area.x,
            y: area.y + area.height,
            width: area.width,
            height: 1,
        };
        let hint = Paragraph::new(Line::from(Span::styled(
            tr(" Enter 确认 · ESC 取消 ", " Enter confirm · ESC cancel "),
            Style::new().fg(Color::Gray),
        )));
        f.render_widget(hint, hint_area);
    }
}

/// 居中矩形（宽 pct%，高 h 行）。
fn centered_rect(pct_x: u16, h: u16, r: ratatui::layout::Rect) -> ratatui::layout::Rect {
    use ratatui::layout::{Constraint, Layout};
    let vert = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(h),
        Constraint::Fill(1),
    ])
    .split(r);
    Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Percentage(pct_x),
        Constraint::Fill(1),
    ])
    .split(vert[1])[1]
}

/// 移除确认弹窗：任务名 + "同时删除已下载文件"复选框。
fn draw_confirm_remove_popup(f: &mut ratatui::Frame, app: &App, gid: &Gid) {
    use ratatui::layout::Alignment;
    use ratatui::style::{Color, Modifier, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Clear, Padding, Paragraph};

    // 任务文件名（超长保留尾部）
    let name = app
        .tasks
        .iter()
        .find(|t| t["gid"].as_str() == Some(gid.0.as_str()))
        .and_then(|t| t["files"][0]["path"].as_str())
        .map(|p| p.rsplit(['/', '\\']).next().unwrap_or(p).to_string())
        .unwrap_or_else(|| gid.0.clone());
    let box_w = f.area().width.min(64);
    let name_w = box_w.saturating_sub(2 + 20) as usize; // 边框 + "移除任务" 前缀
    let name = truncate_head(&name, name_w.max(8));

    let check = if app.remove_del_files { "[x]" } else { "[ ]" };
    let warn = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                tr("移除任务", "Remove task"),
                Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::styled(name.clone(), Style::new().fg(Color::Cyan)),
            Span::raw(tr(" ？", " ?")),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                format!("{check} "),
                Style::new()
                    .fg(if app.remove_del_files {
                        Color::LightRed
                    } else {
                        Color::Gray
                    })
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                tr("同时删除已下载的文件", "Also delete downloaded files"),
                Style::new().fg(if app.remove_del_files {
                    Color::LightRed
                } else {
                    Color::Gray
                }),
            ),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            tr(
                "Enter/y 确认 · 空格 勾选 · n 取消",
                "Enter/y confirm · Space check · n cancel",
            ),
            Style::new().fg(Color::Gray),
        )),
    ])
    .block(
        ui_card_focused()
            .title(Span::styled(
                tr(" 移除 ", " Remove "),
                Style::new().fg(Color::Yellow),
            ))
            .padding(Padding::horizontal(2)),
    )
    .alignment(Alignment::Left);
    let area = centered_rect(70, 7, f.area());
    f.render_widget(Clear, area);
    f.render_widget(warn, area);
}

/// 退出确认弹窗：居中提示，y/Enter/q 确认，n/Esc 取消。
fn draw_confirm_quit_popup(f: &mut ratatui::Frame) {
    use ratatui::style::{Color, Modifier, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Clear, Padding, Paragraph};

    let area = centered_rect(50, 3, f.area());
    f.render_widget(Clear, area);
    let warn = Paragraph::new(Line::from(vec![
        Span::styled(
            tr("确认退出？", "Quit?"),
            Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(
            tr("y 确认 · n 取消", "y yes · n no"),
            Style::new().fg(Color::Gray),
        ),
    ]))
    .block(
        ui_card_focused()
            .title(Span::styled(
                tr(" 退出 ", " Quit "),
                Style::new().fg(Color::Yellow),
            ))
            .padding(Padding::horizontal(1)),
    )
    .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(warn, area);
}

// ----------------------------------------------------------------------
// daemon：RPC 守护进程
// ----------------------------------------------------------------------

fn cmd_daemon(args: &[String]) -> i32 {
    let parsed = xfer_engine::parse_args(args.iter().cloned());
    for opt in &parsed.ignored {
        eprintln!("{}: {opt}", tr("忽略未实现选项", "Ignored unimplemented option"));
    }
    let cfg = parsed.config;
    let rt = tokio::runtime::Runtime::new().expect("创建 runtime 失败");
    rt.block_on(async move {
        // daemon 与 TUI 共用同一会话文件。仅当用户显式传入
        // --dir / --max-concurrent-downloads 时才覆盖会话设置——
        // 否则 daemon 的 parse_args 默认值会随周期保存把用户在
        // 界面里改好的设置覆盖回初始值（重进后"设置回退"）。
        // 任务历史始终从会话恢复。
        let explicit = |k: &str| args.iter().any(|a| a.starts_with(&format!("--{k}=")));
        let dir_flag = explicit("dir");
        let conc_flag = explicit("max-concurrent-downloads");
        let manager = TaskManager::start_with_session(
            dir_flag.then(|| cfg.download_dir.clone()),
            conc_flag.then_some(cfg.max_concurrent),
            xfer_engine::default_session_path(),
        );
        let router = std::sync::Arc::new(xfer_rpc::Router::new(
            cfg.rpc_secret.clone(),
            manager.clone(),
            manager.events(),
        ));
        let bind: std::net::SocketAddr = ([127, 0, 0, 1], cfg.rpc_listen_port).into();
        let shutdown = manager.shutdown_token();
        let sig = shutdown.clone();
        eprintln!(
            "{} http://{bind}/jsonrpc（Ctrl-C {}）",
            tr("RPC 监听于", "RPC listening at"),
            tr("退出", "to quit")
        );
        tokio::select! {
            r = xfer_rpc::serve(bind, router, shutdown) => {
                if let Err(e) = r { eprintln!("{}: {e}", tr("服务错误", "Server error")); }
            }
            _ = sig.cancelled() => {}
            _ = tokio::signal::ctrl_c() => { eprintln!("{}", tr("退出", "Quit")); }
        }
        let _ = manager.save_session();
    });
    0
}

// ----------------------------------------------------------------------
// 远程子命令：原生 WS RPC 客户端
// ----------------------------------------------------------------------

fn cmd_add(args: &[String]) -> i32 {
    let Some(url) = positional(args, 0) else {
        eprintln!("{}", tr("缺少下载地址或 .torrent 文件", "missing download URL or .torrent file"));
        return 2;
    };
    // .torrent 文件 → BT 任务
    let is_torrent_file = url.ends_with(".torrent") && std::path::Path::new(&url).is_file();
    let mut params = if is_torrent_file {
        use base64::Engine;
        let bytes = match std::fs::read(&url) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("{}: {e}", tr("读取种子文件失败", "Failed to read torrent file"));
                return 1;
            }
        };
        json!({
            "torrent": base64::engine::general_purpose::STANDARD.encode(bytes),
        })
    } else {
        json!({"uris": [url]})
    };
    if let Some(d) = flag_value(args, "-d").or_else(|| flag_value(args, "--dir")) {
        params["dir"] = json!(d);
    }
    if !is_torrent_file {
        if let Some(o) = flag_value(args, "-o").or_else(|| flag_value(args, "--out")) {
            params["out"] = json!(o);
        }
        if let Some(c) = flag_value(args, "--checksum") {
            params["checksum"] = json!(c);
        }
    }
    match rpc_call(&connect_url(args), "task.add", with_token(params, args)) {
        Ok(v) => {
            println!("{}", v["gid"].as_str().unwrap_or(""));
            0
        }
        Err(e) => {
            eprintln!("{}: {e}", tr("失败", "Failed"));
            1
        }
    }
}

fn cmd_tell(args: &[String]) -> i32 {
    let Some(gid) = positional(args, 0) else {
        eprintln!("{}", tr("缺少 gid", "missing gid"));
        return 2;
    };
    let params = with_token(json!({"gid": gid}), args);
    match rpc_call(&connect_url(args), "task.tell", params) {
        Ok(v) => {
            println!("{}", serde_json::to_string_pretty(&v).unwrap());
            0
        }
        Err(e) => {
            eprintln!("{}: {e}", tr("失败", "Failed"));
            1
        }
    }
}

fn cmd_list(args: &[String]) -> i32 {
    let scope = flag_value(args, "--scope").unwrap_or_else(|| "all".into());
    let params = with_token(json!({"scope": scope, "num": 1000}), args);
    match rpc_call(&connect_url(args), "task.list", params) {
        Ok(v) => {
            let tasks = v.as_array().cloned().unwrap_or_default();
            if tasks.is_empty() {
                println!("{}", tr("（无任务）", "(no tasks)"));
                return 0;
            }
            println!(
                "{:<16} {:<8} {:>7} {:>12} {:>10}  {}",
                "GID",
                tr("状态", "Status"),
                tr("进度", "Prog"),
                tr("大小", "Size"),
                tr("速度", "Speed"),
                tr("文件", "Name")
            );
            for t in tasks {
                let total = t["totalLength"].as_u64().unwrap_or(0);
                let completed = t["completedLength"].as_u64().unwrap_or(0);
                let pct = if total > 0 {
                    format!("{:.1}%", completed as f64 / total as f64 * 100.0)
                } else {
                    "-".into()
                };
                let name = t["files"][0]["path"]
                    .as_str()
                    .unwrap_or("")
                    .rsplit(['/', '\\'])
                    .next()
                    .unwrap_or("")
                    .to_string();
                println!(
                    "{:<16} {:<8} {:>7} {:>12} {:>10}  {}",
                    t["gid"].as_str().unwrap_or(""),
                    t["status"].as_str().unwrap_or(""),
                    pct,
                    fmt_size(total),
                    format!("{}/s", fmt_size(t["downloadSpeed"].as_u64().unwrap_or(0))),
                    name,
                );
            }
            0
        }
        Err(e) => {
            eprintln!("{}: {e}", tr("失败", "Failed"));
            1
        }
    }
}

fn cmd_task_action(cmd: &str, args: &[String]) -> i32 {
    let Some(gid) = positional(args, 0) else {
        eprintln!("{}", tr("缺少 gid", "missing gid"));
        return 2;
    };
    let method = match cmd {
        "pause" => "task.pause",
        "resume" => "task.resume",
        _ => "task.remove",
    };
    let params = with_token(json!({"gid": gid}), args);
    match rpc_call(&connect_url(args), method, params) {
        Ok(_) => {
            println!("OK");
            0
        }
        Err(e) => {
            eprintln!("{}: {e}", tr("失败", "Failed"));
            1
        }
    }
}

fn cmd_stat(args: &[String]) -> i32 {
    let params = with_token(json!({}), args);
    match rpc_call(&connect_url(args), "engine.globalStat", params) {
        Ok(v) => {
            println!(
                "{} {}/s · {} {} · {} {} · {} {}（{} {}）",
                tr("下载速度", "Down speed"),
                fmt_size(v["downloadSpeed"].as_u64().unwrap_or(0)),
                tr("活动", "active"),
                v["numActive"],
                tr("等待", "waiting"),
                v["numWaiting"],
                tr("停止", "stopped"),
                v["numStopped"],
                tr("累计", "total"),
                v["numStoppedTotal"],
            );
            0
        }
        Err(e) => {
            eprintln!("{}: {e}", tr("失败", "Failed"));
            1
        }
    }
}

// ----------------------------------------------------------------------
// WS 客户端与工具函数
// ----------------------------------------------------------------------

/// 单次原生 RPC 调用：连接 → 请求 → 响应 → 关闭。
fn rpc_call(url: &str, method: &str, params: Value) -> Result<Value, String> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    rt.block_on(async move {
        use futures_util::{SinkExt, StreamExt};
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| {
                format!(
                    "{} {url}: {e}（{} xfer daemon）",
                    tr("连接失败", "Connect failed"),
                    tr("守护进程未启动？先运行", "daemon not running? try")
                )
            })?;
        let (mut tx, mut rx) = ws.split();
        let req = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        tx.send(tokio_tungstenite::tungstenite::Message::Text(
            req.to_string().into(),
        ))
        .await
        .map_err(|e| e.to_string())?;
        while let Some(msg) = rx.next().await {
            let msg = msg.map_err(|e| e.to_string())?;
            if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                let v: Value = serde_json::from_str(&t).map_err(|e| e.to_string())?;
                if v.get("id").and_then(Value::as_i64) == Some(1) {
                    if let Some(err) = v.get("error") {
                        return Err(format!(
                            "{}: {}",
                            err["code"].as_i64().unwrap_or(-1),
                            err["message"]
                                .as_str()
                                .map(String::from)
                                .unwrap_or_else(|| tr("未知错误", "unknown error"))
                        ));
                    }
                    return Ok(v["result"].clone());
                }
            }
        }
        Err(tr("连接被关闭且未收到响应", "connection closed without response").into())
    })
}

fn connect_url(args: &[String]) -> String {
    flag_value(args, "--connect").unwrap_or_else(|| DEFAULT_RPC.into())
}

fn with_token(mut params: Value, args: &[String]) -> Value {
    if let Some(t) = flag_value(args, "--token") {
        params["token"] = json!(t);
    }
    params
}

/// 取第 n 个位置参数（跳过 --flag 及其值）。
fn positional(args: &[String], n: usize) -> Option<String> {
    let mut idx = 0;
    let mut skip_next = false;
    for a in args {
        if skip_next {
            skip_next = false;
            continue;
        }
        if a.starts_with('-') {
            // 带值 flag（= 形式自带值；否则消费下一个）
            if !a.contains('=') {
                skip_next = true;
            }
            continue;
        }
        if idx == n {
            return Some(a.clone());
        }
        idx += 1;
    }
    None
}

/// 取 flag 值：支持 `-d x`、`--dir x`、`--dir=x`。
fn flag_value(args: &[String], name: &str) -> Option<String> {
    let mut it = args.iter().peekable();
    while let Some(a) = it.next() {
        if a == name {
            return it.next().cloned();
        }
        if let Some(v) = a.strip_prefix(&format!("{name}=")) {
            return Some(v.to_string());
        }
    }
    None
}

fn fmt_size(n: u64) -> String {
    const K: u64 = 1024;
    if n >= K * K * K {
        format!("{:.1} GiB", n as f64 / (K * K * K) as f64)
    } else if n >= K * K {
        format!("{:.1} MiB", n as f64 / (K * K) as f64)
    } else if n >= K {
        format!("{:.1} KiB", n as f64 / K as f64)
    } else {
        format!("{n} B")
    }
}

fn fmt_duration(secs: u64) -> String {
    if secs >= 3600 {
        format!(
            "{:02}:{:02}:{:02}",
            secs / 3600,
            (secs % 3600) / 60,
            secs % 60
        )
    } else {
        format!("{:02}:{:02}", secs / 60, secs % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    /// 渲染 draw_in_area 并把缓冲区转成文本行。
    fn render(show_footer: bool) -> Vec<String> {
        let backend = TestBackend::new(80, 24);
        let mut term = Terminal::new(backend).unwrap();
        let st = TuiState {
            last: json!({
                "status": "active",
                "completedLength": 5u64 * 1024 * 1024,
                "totalLength": 10u64 * 1024 * 1024,
                "downloadSpeed": 512u64 * 1024,
                "connections": 3,
                "files": [{"path": "/tmp/test.bin"}],
                "dir": "/tmp",
            }),
            hist: vec![100, 200, 300],
        };
        let gid = Gid::from("abc123def456");
        term.draw(|f| draw_in_area(f, &st, &gid, "FOOTER_MARK", f.area(), show_footer))
            .unwrap();
        let buf = term.backend().buffer().clone();
        (0..24)
            .map(|y| {
                (0..80)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect()
    }

    #[test]
    fn progress_info_line_layout() {
        let _g = LANG_LOCK.lock().unwrap();
        let lines = render(true);
        let text = lines.join("\n");
        // 信息行元素齐全：大小 / 竖线分隔 / 百分比 / 剩余时间
        assert!(text.contains("5.0 MiB / 10.0 MiB"), "应显示已完成/总大小:\n{text}");
        assert!(text.contains("│"), "大小与百分比之间应有竖线分隔:\n{text}");
        assert!(text.contains("50.0%"), "应显示百分比:\n{text}");
        assert!(text.contains("剩 余"), "应显示剩余时间:\n{text}");
        // 进度条行只有块字符，不再居中叠加百分比标签
        let bar_line = lines
            .iter()
            .find(|l| l.contains('█'))
            .expect("应有进度条行");
        assert!(!bar_line.contains('%'), "进度条中间不应再有百分比: {bar_line}");
        assert!(
            bar_line.contains('░'),
            "进度条未填充部分应显示轨道字符，否则看不出终点: {bar_line}"
        );
        // 信息行中百分比位于大小的右侧
        let info_line = lines
            .iter()
            .find(|l| l.contains("MiB"))
            .expect("应有信息行");
        let mib_pos = info_line.find("MiB").unwrap();
        let pct_pos = info_line.find('%').unwrap();
        let div_pos = mib_pos + info_line[mib_pos..].find('│').unwrap();
        assert!(
            mib_pos < div_pos && div_pos < pct_pos,
            "信息行顺序应为 大小 │ 百分比: {info_line}"
        );
    }

    #[test]
    fn footer_shows_once_or_not_at_all() {
        let with = render(true).join("\n");
        assert_eq!(
            with.matches("FOOTER_MARK").count(),
            1,
            "独立视图底栏应恰好出现一次"
        );
        let without = render(false).join("\n");
        assert_eq!(
            without.matches("FOOTER_MARK").count(),
            0,
            "BT 详情分屏内不应再画底栏（外层已有）"
        );
    }

    /// 渲染 BT 任务详情（numPieces > 0），信息行应显示上传速度。
    #[test]
    fn bt_detail_shows_upload_speed() {
        let _g = LANG_LOCK.lock().unwrap();
        let backend = TestBackend::new(80, 24);
        let mut term = Terminal::new(backend).unwrap();
        let st = TuiState {
            last: json!({
                "status": "active",
                "completedLength": 5u64 * 1024 * 1024,
                "totalLength": 10u64 * 1024 * 1024,
                "downloadSpeed": 512u64 * 1024,
                "uploadSpeed": 32u64 * 1024,
                "connections": 3,
                "numPieces": 160,
                "files": [{"path": "/tmp/test.bin"}],
                "dir": "/tmp",
            }),
            hist: vec![100, 200, 300],
        };
        let gid = Gid::from("abc123def456");
        term.draw(|f| draw_in_area(f, &st, &gid, "FOOTER_MARK", f.area(), true))
            .unwrap();
        let buf = term.backend().buffer().clone();
        let text = (0..24)
            .map(|y| {
                (0..80)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n");
        // TestBackend 全角字符间有续行空格（同「剩 余」），按单字断言
        assert!(
            text.contains('上') && text.contains('传'),
            "BT 任务信息行应显示上传速度标签:\n{text}"
        );
        assert!(
            text.contains("32.0 KiB/s"),
            "上传速度值应渲染为 32.0 KiB/s:\n{text}"
        );
    }

    /// 构造只含指定 peer 行的最小 App（空 TaskManager，无副作用）。
    fn app_with_peers(peers: Vec<PeerRow>) -> App {
        App {
            mgr: TaskManager::new(std::env::temp_dir(), 1),
            view: MainView::List,
            selected: 0,
            settings_sel: 0,
            settings_area: 0,
            input: None,
            lang_picker: None,
            confirm_quit: false,
            confirm_remove: None,
            remove_del_files: false,
            tasks: vec![],
            hist: std::collections::HashMap::new(),
            started: std::time::Instant::now(),
            message: None,
            max_concurrent: 1,
            split_connections: 5,
            bt_max_peers: 50,
            dl_limit_kbs: 0,
            ul_limit_kbs: 0,
            download_dir: String::new(),
            session_path: String::new(),
            global_trackers: vec![],
            tracker_sel: 0,
            subscriptions: vec![],
            sub_sel: 0,
            detail_trackers: vec![],
            detail_peers: peers,
            peer_scroll: (0, 0),
            tracker_scroll: (0, 0),
            detail_focus_peers: true,
            category: Category::All,
            sidebar_focus: false,
            bt_encryption: "adaptive".to_string(),
            bt_protocol: "tcp+utp".to_string(),
            bt_adaptive: true,
            max_conn_per_server: 0,
            min_split_size: 0,
            add_task: None,
        }
    }

    fn render_peers(app: &App, w: u16, h: u16) -> String {
        let backend = TestBackend::new(w, h);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_detail_peers(f, app, f.area())).unwrap();
        let buf = term.backend().buffer().clone();
        (0..h)
            .map(|y| {
                (0..w)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn sample_peer() -> PeerRow {
        PeerRow {
            addr: "223.5.5.5:6881".to_string(),
            client: "qBittorrent/5.2.3".to_string(),
            source: "tracker".to_string(),
            seed: false,
            downloaded: 1_500_000,
            connected: true,
            encrypted: true,
            uploaded: 300_000,
            protocol: "tcp".to_string(),
            connected_secs: 125,
            progress: Some(45.2),
            country: "中国".to_string(),
        }
    }

    #[test]
    fn peer_table_wide_keeps_full_client_and_new_columns() {
        let _g = LANG_LOCK.lock().unwrap();
        let app = app_with_peers(vec![sample_peer()]);
        let text = render_peers(&app, 140, 6);
        // TestBackend 全角字符带续行空格（同「剩 余」），去空格后断言
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        // 旧版固定 16 字符会把 "qBittorrent/5.2.3"（17 列）截断；现在必须完整
        assert!(
            compact.contains("qBittorrent/5.2.3"),
            "宽终端下客户端名应完整显示:\n{text}"
        );
        for needle in ["国家/地区", "中国", "加密", "TCP", "02:05", "45.2%", "223.5.5.5:6881"] {
            assert!(compact.contains(needle), "缺少列内容 `{needle}`:\n{text}");
        }
    }

    #[test]
    fn peer_table_header_aligns_with_cells() {
        let _g = LANG_LOCK.lock().unwrap();
        let app = app_with_peers(vec![sample_peer()]);
        let backend = TestBackend::new(140, 6);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_detail_peers(f, &app, f.area())).unwrap();
        let buf = term.backend().buffer().clone();
        // y=1 表头、y=2 数据行（y=0 为边框）：同列字符必须起始于相同 x
        let find_x = |y: u16, ch: char| -> Option<u16> {
            (0..140u16).find(|&x| buf[(x, y)].symbol() == ch.to_string())
        };
        assert_eq!(find_x(1, '国'), find_x(2, '中'), "国家/地区列表头与内容错位");
        assert_eq!(find_x(1, '加'), find_x(2, '加'), "加密列表头与内容错位");
        assert_eq!(find_x(1, '协'), find_x(2, 'T'), "协议列表头与 TCP 错位");
        assert_eq!(find_x(1, '时'), find_x(2, '0'), "时长列表头与 02:05 错位");
        assert_eq!(find_x(1, '进'), find_x(2, '4'), "进度列表头与 45.2% 错位");
    }

    fn sample_task(gid: &str, status: &str, completed: u64, total: u64) -> serde_json::Value {
        json!({
            "gid": gid,
            "status": status,
            "completedLength": completed,
            "totalLength": total,
            "downloadSpeed": 0,
            "files": [{"path": format!("/tmp/{gid}.bin")}],
        })
    }

    #[test]
    fn category_filtering_rules() {
        let mut app = app_with_peers(vec![]);
        app.tasks = vec![
            sample_task("aaa", "active", 10, 100),    // 下载中
            sample_task("bbb", "active", 100, 100),   // 做种
            sample_task("ccc", "complete", 100, 100), // 完成
            sample_task("ddd", "error", 0, 100),      // 错误
            sample_task("eee", "waiting", 0, 100),    // 仅「全部」可见
        ];
        app.category = Category::All;
        assert_eq!(filtered_indices(&app).len(), 5);
        app.category = Category::Downloading;
        assert_eq!(filtered_indices(&app), vec![0]);
        app.category = Category::Seeding;
        assert_eq!(filtered_indices(&app), vec![1]);
        app.category = Category::Complete;
        assert_eq!(filtered_indices(&app), vec![2]);
        app.category = Category::Error;
        assert_eq!(filtered_indices(&app), vec![3]);
        // 选中序号以过滤后视图为准
        app.selected = 0;
        assert_eq!(gid_at(&app, 0).map(|g| g.0), Some("ddd".to_string()));
        // 分类步进循环
        assert_eq!(category_step(Category::All, -1), Category::Error);
        assert_eq!(category_step(Category::Error, 1), Category::All);
    }

    #[test]
    fn list_view_renders_sidebar_and_filters() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);
        app.tasks = vec![
            sample_task("aaa", "active", 10, 100),
            sample_task("ccc", "complete", 100, 100),
        ];
        app.category = Category::Complete;

        let backend = TestBackend::new(100, 20);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_list(f, &app)).unwrap();
        let buf = term.backend().buffer().clone();
        let text: String = (0..20)
            .map(|y| {
                (0..100)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();

        // 侧边栏不再渲染标题（直接从分类行开始）
        for row in text.lines().filter(|l| l.contains("全部")) {
            assert!(!row.contains("分类"), "侧边栏不应渲染标题:\n{text}");
        }
        // 选中分类默认有背景高亮，不再用 ▸ 指针
        // （侧栏区 x<16 内找 DarkGray 背景块；任务表的高亮行在 x>=18，不会混入）
        let hl_cells: Vec<(u16, u16)> = (2..15)
            .flat_map(|x| (1..17).map(move |y| (x, y)))
            .filter(|&(x, y)| buf[(x, y)].bg == ratatui::style::Color::DarkGray)
            .collect();
        assert!(
            !hl_cells.is_empty(),
            "默认选中分类应有背景高亮:\n{text}"
        );
        // 所在行应是选中的「完成」分类
        let hl_y = hl_cells[0].1;
        // 选中条应连续覆盖 x=2..=14（含「完成」等 CJK 宽字符的续格，无断口）
        for x in 2..=14u16 {
            assert_eq!(
                buf[(x, hl_y)].bg,
                ratatui::style::Color::DarkGray,
                "选中条在 x={x} 应连续:\n{text}"
            );
        }
        // 与左右竖线各留 1 格间距（对称）；边框外 x=0 保持默认
        assert_eq!(
            buf[(1, hl_y)].bg,
            ratatui::style::Color::Reset,
            "背景条与左竖线应留 1 格间距:\n{text}"
        );
        assert_eq!(buf[(1, hl_y)].symbol(), "│", "左竖线字符应保留");
        assert_eq!(
            buf[(15, hl_y)].bg,
            ratatui::style::Color::Reset,
            "背景条与分隔竖线应留 1 格间距:\n{text}"
        );
        assert_eq!(
            buf[(0, hl_y)].bg,
            ratatui::style::Color::Reset,
            "背景不应越过边框到 x=0:\n{text}"
        );
        // 未选中项也是整条连续暗色背景；标签右移后从 x=3 起
        let all_y = (1..17)
            .find(|&y| buf[(3, y)].symbol() == "全")
            .expect("全部分类标签应在 x=3");
        for x in 2..=14u16 {
            assert_eq!(
                buf[(x, all_y)].bg,
                ratatui::style::Color::Rgb(38, 38, 38),
                "未选中条在 x={x} 应连续:\n{text}"
            );
        }
        assert_eq!(buf[(1, all_y)].symbol(), "│", "未选中行左竖线应保留");
        assert_eq!(
            buf[(1, all_y)].bg,
            ratatui::style::Color::Reset,
            "未选中条与左竖线应留 1 格间距:\n{text}"
        );
        assert_eq!(
            buf[(0, all_y)].bg,
            ratatui::style::Color::Reset,
            "未选中条不应越过边框:\n{text}"
        );
        let row_compact: String = (0..100)
            .map(|x| buf[(x, hl_y)].symbol().to_string())
            .collect::<String>()
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(
            row_compact.contains("完成(1)"),
            "背景高亮行应是选中的完成分类:\n{text}"
        );
        assert!(
            !compact.contains('▸'),
            "侧边栏不应再用指针指示选中项:\n{text}"
        );
        assert!(compact.contains("全部(2)"), "侧边栏应显示全部分类计数:\n{text}");
        assert!(compact.contains("完成(1)"), "侧边栏应显示完成分类计数:\n{text}");
        assert!(
            !compact.contains("任务（"),
            "任务卡片不应再渲染标题:\n{text}"
        );
        assert!(compact.contains("ccc.bin"), "完成分类应显示已完成任务:\n{text}");
        assert!(!compact.contains("aaa.bin"), "完成分类不应显示下载中任务:\n{text}");
    }

    #[test]
    fn peer_table_scroll_offsets_shift_content() {
        let mut p2 = sample_peer();
        p2.addr = "8.8.8.8:51413".to_string();
        let mut app = app_with_peers(vec![sample_peer(), p2]);

        // 未滚动：表头与第一行可见
        let text0 = render_peers(&app, 140, 6);
        assert!(text0.contains("IP:Port"));
        assert!(text0.contains("223.5.5.5:6881"));

        // 下滚 1 行：表头移出可视区
        app.peer_scroll = (1, 0);
        let text1 = render_peers(&app, 140, 6);
        assert!(!text1.contains("IP:Port"), "下滚后表头应不可见:\n{text1}");

        // 右滚 30 列：IP 列移出，右侧列仍可见
        app.peer_scroll = (0, 30);
        let text2 = render_peers(&app, 140, 6);
        assert!(!text2.contains("223.5.5.5:6881"), "右滚后 IP 列应消失:\n{text2}");
        assert!(
            text2.contains("02:05"),
            "右滚后右侧列仍应可见:\n{text2}"
        );

        // 纵向越界被钳制：最后一行仍可见
        app.peer_scroll = (100, 0);
        let text3 = render_peers(&app, 140, 6);
        assert!(text3.contains("8.8.8.8:51413"), "纵向钳制后末行应可见:\n{text3}");

        // 横向越界被钳制：不 panic，内容不再整体移出
        app.peer_scroll = (0, 5000);
        let _text4 = render_peers(&app, 140, 6);
    }

    #[test]
    fn peer_table_narrow_drops_tail_columns_but_keeps_client() {
        let app = app_with_peers(vec![sample_peer()]);
        let text = render_peers(&app, 60, 6);
        assert!(text.contains("223.5.5.5:6881"), "窄终端仍应显示 IP:\n{text}");
        assert!(
            text.contains("qBittorrent/5.2.3") || text.contains("qBittorrent"),
            "窄终端仍应尽量显示客户端:\n{text}"
        );
    }

    #[test]
    fn width_helpers_are_cjk_aware() {
        use unicode_width::UnicodeWidthStr;
        // 全角补齐：2 个汉字占 4 列，补到 6 列需 2 空格
        let p = pad_str("中国", 6);
        assert_eq!(p.width(), 6, "CJK 补齐后显示宽度应为 6: {p:?}");
        assert!(p.starts_with("中国"));
        // 全角截断：宽度 5 只能放下 2 个汉字（4 列），第 3 个放不下
        let t = truncate_to_width("中国上海市", 5);
        assert_eq!(t, "中国", "宽度 5 应截断为两个汉字: {t:?}");
        // ASCII 常规情形
        assert_eq!(pad_str("abc", 5), "abc  ");
        assert_eq!(truncate_to_width("abcdef", 4), "abcd");
    }

    #[test]
    fn ip_of_addr_strips_port_and_brackets() {
        assert_eq!(ip_of_addr("1.2.3.4:6881"), "1.2.3.4");
        assert_eq!(ip_of_addr("[2001:db8::1]:6881"), "2001:db8::1");
        assert_eq!(ip_of_addr("1.2.3.4"), "1.2.3.4");
    }

    // 语言切换测试与断言中文文案的渲染测试互斥（LANG 为进程级全局）
    static LANG_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// 测试期临时切换语言的守卫：drop 时恢复中文。
    struct LangGuard(Lang);

    impl LangGuard {
        fn new(l: Lang) -> Self {
            set_lang(l);
            LangGuard(Lang::Zh)
        }
    }

    impl Drop for LangGuard {
        fn drop(&mut self) {
            set_lang(self.0);
        }
    }

    #[test]
    fn tr_for_switches_text_by_language() {
        assert_eq!(tr_for(Lang::Zh, "状态", "Status"), "状态");
        assert_eq!(tr_for(Lang::En, "状态", "Status"), "Status");
        assert_eq!(tr_for(Lang::Zh, "全部", "All"), "全部");
        assert_eq!(tr_for(Lang::En, "全部", "All"), "All");
    }

    /// 简→繁转换：覆盖界面用字的常用异形字；繁体三态切换即时生效。
    #[test]
    fn traditional_chinese_converts_ui_strings() {
        // 纯函数转换：界面高频词正确转繁体
        assert_eq!(s2t("下载完成后通知"), "下載完成後通知");
        assert_eq!(s2t("设置"), "設置");
        assert_eq!(s2t("界面语言"), "界面語言");
        assert_eq!(s2t("错误"), "錯誤");
        assert_eq!(s2t("进度"), "進度");
        assert_eq!(s2t("连接数"), "連接數");
        assert_eq!(s2t("暂停"), "暫停");
        // 同形字（中/文/大小）原样保留
        assert_eq!(s2t("中文大小"), "中文大小");

        // 全局语言三态：tr 跟随当前语言
        let _g = LANG_LOCK.lock().unwrap();
        {
            let _guard = LangGuard::new(Lang::ZhTw);
            assert_eq!(tr("下载完成", "Download complete"), "下載完成");
            assert_eq!(tr("下载完成", "Download complete"), "下載完成");
            assert_eq!(tr("下载完成通知", "Notify on done"), "下載完成通知");
        }
        {
            let _guard = LangGuard::new(Lang::En);
            assert_eq!(tr("下载完成", "Download complete"), "Download complete");
        }
    }

    /// 设置页第 12 行：界面语言行显示当前语言（三种名称），
    /// 选中该行按 Enter 打开语言选择弹窗（三个选项全部摆出）。
    #[test]
    fn lang_setting_row_and_picker() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);

        // 设置页渲染出「界面语言」行，值 = 当前语言自称
        let text = render_settings(&app);
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            compact.contains("界面语言简体中文"),
            "设置页应显示界面语言行及当前语言:\n{compact}"
        );

        // Enter 打开语言选择弹窗：三个语言全部出现在弹窗里
        app.settings_sel = 11;
        app.view = MainView::Settings;
        let enter = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        );
        assert!(handle_key(&mut app, &enter));
        let picker = app.lang_picker.expect("Enter 应打开语言选择弹窗");
        assert_eq!(picker, 0, "当前简体应选中第 0 项");

        // 弹窗渲染含全部三个语言名
        let backend = TestBackend::new(50, 12);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_lang_picker(f, &app)).unwrap();
        let buf = term.backend().buffer().clone();
        let txt: String = (0..12)
            .map(|y| {
                (0..50)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let compact2: String = txt.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            compact2.contains("简体中文") && compact2.contains("繁體中文") && compact2.contains("English"),
            "语言选择弹窗应同时列出三种语言:\n{compact2}"
        );

        // ↓ 选择繁体，Enter 确认：界面语言切换并写回引擎设置
        let down = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('j'),
            crossterm::event::KeyModifiers::NONE,
        );
        assert!(handle_key(&mut app, &down));
        assert_eq!(app.lang_picker, Some(1));
        assert!(handle_key(&mut app, &enter));
        assert_eq!(app.lang_picker, None, "确认后应关闭弹窗");
        assert_eq!(lang(), Lang::ZhTw, "确认后界面应切换为繁体中文");
        // 已写回引擎全局选项
        assert_eq!(
            app.mgr.get_global_option()["lang"].as_str(),
            Some("zh_tw"),
            "语言选择应持久化到引擎设置"
        );
        // 恢复简体，避免污染后续测试
        set_lang(Lang::Zh);
    }

    /// 设置页向下导航必须能到第 12 项（界面语言），且不越过末项。
    /// 回归：`min(10)` 曾把上限卡在第 11 项，导致「界面语言」行永远选不中。
    #[test]
    fn settings_down_reaches_language_row() {
        let mut app = app_with_peers(vec![]);
        app.view = MainView::Settings;
        app.settings_area = 0;
        app.settings_sel = 0;

        let down = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Down,
            crossterm::event::KeyModifiers::NONE,
        );

        for _ in 0..11 {
            assert!(handle_key(&mut app, &down));
        }
        assert_eq!(
            app.settings_sel, 11,
            "连续下移 11 次应选中第 12 项（界面语言）"
        );

        // 已在末项，再下移不应越界
        assert!(handle_key(&mut app, &down));
        assert_eq!(
            app.settings_sel, 11,
            "末项之后继续下移应保持在界面语言行"
        );
    }

    /// [临时调试] dump 侧栏区域每格的符号/前景/背景，验证后删除。
    #[test]
    fn debug_dump_sidebar_cells() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);
        app.tasks = vec![
            sample_task("aaa", "active", 10, 100),
            sample_task("ccc", "complete", 100, 100),
        ];
        app.category = Category::Complete;
        let backend = TestBackend::new(100, 20);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_list(f, &app)).unwrap();
        let buf = term.backend().buffer().clone();
        for y in 1..8u16 {
            let mut row = format!("y={y}: ");
            for x in 0..18u16 {
                let c = &buf[(x, y)];
                let sym = c.symbol().chars().next().unwrap_or(' ');
                row.push(match c.bg {
                    ratatui::style::Color::Reset => sym,
                    ratatui::style::Color::DarkGray => '▓',
                    ratatui::style::Color::Rgb(r, _, _) if r == 38 => '░',
                    ratatui::style::Color::Cyan => '█',
                    _ => '?',
                });
            }
            println!("{row}");
        }
    }

    fn render_list(app: &App, w: u16, h: u16) -> Vec<String> {
        let backend = TestBackend::new(w, h);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_list(f, app)).unwrap();
        let buf = term.backend().buffer().clone();
        (0..h)
            .map(|y| {
                (0..w)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect()
    }

    /// 详情页顶部应保留全局信息栏（logo + 速率/计数），且不被任务详情内容覆盖。
    #[test]
    fn detail_view_shows_global_top_bar() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);
        app.tasks = vec![sample_task("aaa", "active", 10, 100)];
        let backend = TestBackend::new(120, 20);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_detail(f, &app, &Gid("aaa".to_string()))).unwrap();
        let buf = term.backend().buffer().clone();
        let top: String = (0..120u16)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect();
        assert!(top.contains('X'), "详情页顶部应显示品牌 logo:\n{top}");
        assert!(top.contains('│'), "详情页顶部应显示速率/计数分隔竖线:\n{top}");
        // 顶栏行不应出现任务详情内容（曾因区域 y 未偏移被详情卡片覆盖）
        assert!(
            !top.contains("/tmp/aaa.bin"),
            "顶栏行不应被任务详情内容占据:\n{top}"
        );
    }

    /// 主区边框：左右两侧均圆角，上下横线横跨全宽（左右各缩 1 列、与 logo 对齐），
    /// 左竖线贴左缘（侧栏包裹在内）、侧栏分隔竖线、右竖线贴右缘。
    #[test]
    fn main_borders_fully_rounded_enclosing_sidebar() {
        let _g = LANG_LOCK.lock().unwrap();
        let app = app_with_peers(vec![]);
        let rows = render_list(&app, 60, 12);
        let side = 16; // 中文侧栏宽度 → 竖线 x=16，任务区行 1..=9
        // 顶栏 1 行（y=0）：logo 左间距 1 格
        assert_eq!(rows[0].chars().nth(0), Some(' '), "logo 左侧应留间距");
        assert_eq!(rows[0].chars().nth(1), Some('X'), "logo 应从 x=1 开始");
        // 顶栏右侧信息右间距 1 格
        assert_eq!(rows[0].chars().nth(58), Some('0'), "计数末字符应在 x=58");
        assert_eq!(rows[0].chars().nth(59), Some(' '), "顶栏右端应留 1 格间距");
        // 速率与任务计数之间的分隔竖线只在内容行
        assert!(rows[0].contains('│'), "速率与计数之间应有分隔竖线");
        // 顶横线：x=1 左圆角、x=2..=57 主体（穿过侧栏区）、x=58 右圆角
        assert_eq!(rows[1].chars().nth(0), Some(' '), "顶横线左端应留缺口");
        assert_eq!(rows[1].chars().nth(1), Some('╭'), "顶横线左端应为圆角");
        assert_eq!(rows[1].chars().nth(2), Some('─'), "圆角后应接横线");
        assert_eq!(rows[1].chars().nth(side), Some('─'), "顶横线应横跨侧栏区");
        assert_eq!(rows[1].chars().nth(58), Some('╮'), "顶横线右端应为圆角");
        assert_eq!(rows[1].chars().nth(59), Some(' '), "圆角右侧应留缺口");
        // 右竖线：缩进 1 列，上下连入圆角；右缘留空
        assert_eq!(rows[2].chars().nth(58), Some('│'), "右竖线应连入顶圆角");
        assert_eq!(rows[8].chars().nth(58), Some('│'), "右竖线应连入底圆角");
        assert_eq!(rows[2].chars().nth(59), Some(' '), "右缘应留缺口");
        // 左竖线：贴左缘缩进 1 列，上下连入圆角，将侧栏包裹在内；最左缘留空
        assert_eq!(rows[2].chars().nth(0), Some(' '), "左缘应留缺口");
        assert_eq!(rows[2].chars().nth(1), Some('│'), "左竖线应连入顶圆角");
        assert_eq!(rows[8].chars().nth(1), Some('│'), "左竖线应连入底圆角");
        // 侧栏分隔竖线：与横线相接
        assert_eq!(rows[2].chars().nth(side), Some('│'), "侧栏分隔竖线");
        assert_eq!(rows[8].chars().nth(side), Some('│'), "侧栏分隔竖线底端");
        // 底横线与顶横线对称，左右两端均为圆角
        assert_eq!(rows[9].chars().nth(1), Some('╰'), "底横线左端应为圆角");
        assert_eq!(rows[9].chars().nth(58), Some('╯'), "底横线右端应为圆角");
        assert_eq!(rows[9].chars().nth(59), Some(' '), "圆角右侧应留缺口");
    }

    #[test]
    fn task_list_header_aligns_with_cjk_name_rows() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);
        app.tasks = vec![sample_task("aaa", "active", 10, 100)];
        // CJK 文件名：历史 bug 是全角字符把后续列顶歪
        app.tasks[0]["files"][0]["path"] = json!("/tmp/文件名对齐测试.bin");

        let backend = TestBackend::new(120, 20);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_list(f, &app)).unwrap();
        let buf = term.backend().buffer().clone();

        // 按缓冲区单元格定位（TestBackend 全角字符带续行空格，不能按字符串 find）
        let find = |ch: &str| -> Vec<(u16, u16)> {
            let mut v = vec![];
            for y in 0..20u16 {
                for x in 0..120u16 {
                    if buf[(x, y)].symbol() == ch {
                        v.push((x, y));
                    }
                }
            }
            v
        };

        // 表头行含「状」（状态列标签）；数据行含「对」（CJK 文件名）
        let status_hdr = find("状");
        assert_eq!(status_hdr.len(), 1, "「状」应只出现在表头");
        let (sx, hy) = status_hdr[0];
        let dui = find("对");
        assert_eq!(dui.len(), 1, "「对」应只出现在数据行");
        let (_, dy) = dui[0];
        assert_ne!(hy, dy, "表头与数据行应为不同行");

        // 状态列：表头「状」与数据行 "active" 首字符同 x
        let (ax, _) = find("a")
            .into_iter()
            .find(|(_, y)| *y == dy)
            .expect("数据行应有 active");
        assert_eq!(sx, ax, "状态列起始 x 应与表头对齐（x={sx} vs {ax}）");

        // 文件名列：表头「文」与数据行「文」同 x
        let wen = find("文");
        let hx = wen
            .iter()
            .find(|(_, y)| *y == hy)
            .expect("表头应有「文」")
            .0;
        let rx = wen
            .iter()
            .find(|(_, y)| *y == dy)
            .expect("数据行应有「文」")
            .0;
        assert_eq!(hx, rx, "文件名列起始 x 应与表头对齐（x={hx} vs {rx}）");

        // 分隔横线紧贴表头且紧贴数据行（无空行），数据行在表头下 2 行
        assert_eq!(dy, hy + 2, "数据行应在表头下方 2 行");
        let sep_y = hy + 1;
        let sep_chars: Vec<u16> = (0..120u16)
            .filter(|&x| buf[(x, sep_y)].symbol() == "─")
            .collect();
        assert!(!sep_chars.is_empty(), "表头下方应有分隔横线");
        assert_eq!(
            sep_chars[0], sx,
            "分隔横线左端应与表头对齐（x={} vs {sx}）",
            sep_chars[0]
        );
        // 分隔横线样式应与边框一致（DarkGray）
        assert_eq!(
            buf[(sep_chars[0], sep_y)].fg,
            ratatui::style::Color::DarkGray,
            "分隔横线样式应与边框一致"
        );

        // 大小列：表头「大」与数据行已完成数值首字符左对齐
        let da = find("大");
        assert_eq!(da.len(), 1, "「大」应只出现在表头");
        let szx = da[0].0;
        let slashes: Vec<u16> = (0..120u16)
            .filter(|&x| buf[(x, dy)].symbol() == "/")
            .collect();
        assert!(!slashes.is_empty(), "数据行大小列应有 / 分隔");
        // 紧凑格式 "10B / 100B" → '/' 距数值起点 5 列
        let size_start = slashes[0] - 5;
        assert_eq!(
            szx, size_start,
            "大小列数值应与表头左对齐（x={szx} vs {size_start}）"
        );
    }

    #[test]
    fn list_view_renders_english_when_lang_en() {
        let _g = LANG_LOCK.lock().unwrap();
        let _lang = LangGuard::new(Lang::En);
        let mut app = app_with_peers(vec![]);
        app.tasks = vec![sample_task("aaa", "active", 10, 100)];

        let lines = render_list(&app, 120, 20);
        let compact: String = lines.join("\n").chars().filter(|c| !c.is_whitespace()).collect();
        for needle in ["Status", "Progress", "Name", "Size", "Speed"] {
            assert!(compact.contains(needle), "英文模式缺少 `{needle}`:\n{compact}");
        }
        assert!(compact.contains("All(1)"), "英文侧边栏应显示 All 分类:\n{compact}");
        // 侧栏标题与任务卡片标题已删除
        assert!(!compact.contains("Filter"), "英文侧边栏不应渲染标题:\n{compact}");
        assert!(!compact.contains("Tasks（"), "英文任务卡片不应渲染标题:\n{compact}");
        assert!(!compact.contains("状态"), "英文模式不应出现中文表头:\n{compact}");
    }

    /// 顶栏全局速率：上传速度必须始终可见（无上传时显示 0，而非隐藏列）。
    #[test]
    fn list_top_bar_always_shows_upload_speed() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);
        app.tasks = vec![sample_task("aaa", "active", 10, 100)];

        let lines = render_list(&app, 120, 20);
        let compact: String = lines
            .join("\n")
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(
            compact.contains("↓0B/s"),
            "顶栏应始终显示下载速度:\n{compact}"
        );
        assert!(
            compact.contains("↑0B/s"),
            "顶栏应始终显示上传速度（即使为 0）:\n{compact}"
        );
    }

    fn render_settings(app: &App) -> String {
        let backend = TestBackend::new(100, 36);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_settings(f, app)).unwrap();
        let buf = term.backend().buffer().clone();
        (0..36)
            .map(|y| {
                (0..100)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// 单服务器连接数 / 最小分片大小未设置时：设置页直接显示引擎默认值，
    /// 而不是「默认」文字；显式填 0（= 默认）也归一到默认值。
    #[test]
    fn unset_conn_and_split_options_show_engine_defaults() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);
        // TaskManager::new 无任何全局选项 → 未设置 → 回填引擎默认值
        refresh_app(&mut app);
        assert_eq!(
            app.max_conn_per_server,
            xfer_engine::DEFAULT_SPLIT_CONNECTIONS as u64,
            "单服务器连接数未设置时应回填默认值"
        );
        assert_eq!(
            app.min_split_size,
            xfer_engine::DEFAULT_MIN_SPLIT_SIZE,
            "最小分片大小未设置时应回填默认值"
        );
        // 设置页渲染：显示数值，不再出现「默认」文字
        let text = render_settings(&app);
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            !compact.contains("默认"),
            "未设置的两项不应显示「默认」文字:\n{compact}"
        );
        assert!(
            compact.contains(&format!(
                "单服务器连接数{}",
                xfer_engine::DEFAULT_SPLIT_CONNECTIONS
            )),
            "应直接显示单服务器连接数默认值:\n{compact}"
        );
        assert!(
            compact.contains("最小分片大小4.2M"),
            "应直接显示最小分片大小默认值（4 MiB = 4.2M）:\n{compact}"
        );
        // 用户显式填 0（= 默认）：写入后刷新仍归一默认值
        submit_setting(&mut app, SettingKey::MinSplitSize, "0");
        refresh_app(&mut app);
        assert_eq!(
            app.min_split_size,
            xfer_engine::DEFAULT_MIN_SPLIT_SIZE,
            "显式 0 应继续显示默认值"
        );
    }

    /// 构造磁力文件选择弹窗用的任务状态快照（多文件）。
    fn magnet_status_fixture() -> Value {
        json!({
            "gid": "gid-magnet-1",
            "status": "paused",
            "awaitingSelection": true,
            "files": [
                {"index": 1, "path": "UbuntuSuite/ubuntu-24.04.iso", "length": 5u64 * 1024 * 1024 * 1024},
                {"index": 2, "path": "UbuntuSuite/README.txt", "length": 2048},
                {"index": 3, "path": "UbuntuSuite/checksums.sha256", "length": 4096},
            ],
        })
    }

    #[test]
    fn magnet_selection_from_status_strips_root() {
        let _g = LANG_LOCK.lock().unwrap();
        let t = magnet_status_fixture();
        let d = magnet_selection_from_status(&t, Gid::from("gid-magnet-1"));
        let AddStage::Selecting {
            name, files, checked, cursor, ..
        } = d
        else {
            panic!("应为 Selecting 状态");
        };
        assert_eq!(name, "UbuntuSuite");
        assert_eq!(files.len(), 3);
        // 公共根目录段被剥离
        assert_eq!(files[0].path, "ubuntu-24.04.iso");
        assert_eq!(files[1].path, "README.txt");
        assert_eq!(files[2].path, "checksums.sha256");
        // index 转为 0 起算
        assert_eq!(files[0].index, 0);
        assert_eq!(files[2].index, 2);
        // 默认全选、光标在首行
        assert!(checked.iter().all(|c| *c));
        assert_eq!(checked.len(), files.len());
        assert_eq!(cursor, 0);
    }

    #[test]
    fn magnet_selection_single_file_uses_full_path() {
        let _g = LANG_LOCK.lock().unwrap();
        let t = json!({
            "gid": "gid-magnet-2",
            "status": "paused",
            "awaitingSelection": true,
            "files": [{"index": 1, "path": "movie.mkv", "length": 1024}],
        });
        let d = magnet_selection_from_status(&t, Gid::from("gid-magnet-2"));
        let AddStage::Selecting { name, files, .. } = d else {
            panic!("应为 Selecting 状态");
        };
        assert_eq!(name, "movie.mkv");
        assert_eq!(files[0].path, "movie.mkv");
        assert_eq!(files[0].index, 0);
    }

    #[test]
    fn magnet_selected_summary_counts() {
        let _g = LANG_LOCK.lock().unwrap();
        let files = vec![
            MagnetFileRow { index: 0, path: "a".into(), length: 100 },
            MagnetFileRow { index: 1, path: "b".into(), length: 200 },
            MagnetFileRow { index: 2, path: "c".into(), length: 400 },
        ];
        let (n, total, bytes, total_bytes) =
            magnet_selected_summary(&files, &[true, false, true]);
        assert_eq!((n, total, bytes, total_bytes), (2, 3, 500, 700));
    }

    /// 新建任务弹窗（输入态）应渲染地址 + 目录两字段与占位说明。
    #[test]
    fn add_task_dialog_renders_url_and_dir_fields() {
        let _g = LANG_LOCK.lock().unwrap();
        let app = app_with_peers(vec![]);
        let app = App {
            add_task: Some(AddTaskDialog {
                url: "magnet:?xt=urn:btih:abcdef".into(),
                dir: String::new(),
                field: AddField::Url,
                stage: AddStage::Input,
            }),
            ..app
        };
        let backend = TestBackend::new(80, 24);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw_app(f, &app)).unwrap();
        let buf = term.backend().buffer().clone();
        let text: String = (0..24)
            .map(|y| {
                (0..80)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        for needle in ["新建任务", "地址:", "目录:", "magnet:?xt=urn:btih:abcdef"] {
            assert!(compact.contains(needle), "弹窗缺少 `{needle}`:\n{text}");
        }
        // 空目录应显示占位说明
        assert!(
            compact.contains("目录:（空=使用默认下载目录）"),
            "空目录应有占位说明:\n{text}"
        );
    }

    /// 磁力链接提交后直接解析：弹窗保持打开进入解析态，任务带目录。
    #[test]
    fn submit_magnet_starts_parsing_and_keeps_dialog() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);
        let tmp = std::env::temp_dir().join(format!("xfer-tui-test-{}", std::process::id()));
        let dir = tmp.to_string_lossy().to_string();
        app.add_task = Some(AddTaskDialog {
            url: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567".into(),
            dir: dir.clone(),
            field: AddField::Url,
            stage: AddStage::Input,
        });
        let d = app.add_task.clone().unwrap();
        submit_add_task(&mut app, &d);
        // 弹窗保持打开，进入解析态（用户输入后直接解析）
        match &app.add_task {
            Some(AddTaskDialog {
                stage: AddStage::Parsing { .. },
                dir: d2,
                ..
            }) => assert_eq!(d2, &dir, "解析态应保留目录"),
            other => panic!("提交磁力后应为 Parsing 态，实际 {other:?}"),
        }
        // 任务已创建且目录为设置的目录
        let tasks = app.mgr.list_native("all", 0, -1, None);
        let t = tasks
            .as_array()
            .and_then(|a| a.first())
            .expect("磁力任务应已创建");
        assert_eq!(t["dir"].as_str(), Some(dir.as_str()));
        assert_eq!(
            t["awaitingSelection"].as_bool(),
            Some(true),
            "磁力任务应等待文件选择"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// 普通 URL 提交后弹窗关闭，任务使用设置的目录而非全局目录。
    #[test]
    fn submit_url_adds_with_dir_and_closes_dialog() {
        let _g = LANG_LOCK.lock().unwrap();
        let mut app = app_with_peers(vec![]);
        let tmp = std::env::temp_dir().join(format!("xfer-tui-test-url-{}", std::process::id()));
        let dir = tmp.to_string_lossy().to_string();
        app.add_task = Some(AddTaskDialog {
            url: "http://example.com/file.bin".into(),
            dir: dir.clone(),
            field: AddField::Dir,
            stage: AddStage::Input,
        });
        let d = app.add_task.clone().unwrap();
        submit_add_task(&mut app, &d);
        assert!(app.add_task.is_none(), "URL 添加成功后弹窗应关闭");
        let tasks = app.mgr.list_native("all", 0, -1, None);
        let t = tasks
            .as_array()
            .and_then(|a| a.first())
            .expect("任务应已创建");
        assert_eq!(t["dir"].as_str(), Some(dir.as_str()));
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
