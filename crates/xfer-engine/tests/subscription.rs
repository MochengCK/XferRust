//! Tracker 订阅源集成测试。
//!
//! 覆盖：
//! - 新增订阅源 → 引擎侧 kick 后台循环立即拉取（所有客户端一致的
//!   "首次添加自动更新"，不再依赖 TUI 手动补刷新）；
//! - 刷新的**同步语义**：远程列表变化（新增/移除）如实反映到全局
//!   Tracker 列表（此前 merge-only，移除永不生效，表现为"列表不动"）；
//! - 手动添加的 tracker 不被订阅同步移除；
//! - 远程空列表防误清空；
//! - 禁用/移除订阅源回收其贡献的 tracker；
//! - TTL 过滤（24h）与手动全量刷新的区别；
//! - 来源标记（trackerSources）的会话持久化。

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use xfer_engine::TaskManager;

/// 可变内容的纯文本 tracker 列表服务（每行一个 URL，支持 # 注释）。
struct TrackerListServer {
    base: String,
    list: Arc<Mutex<Vec<String>>>,
}

impl TrackerListServer {
    fn set(&self, urls: &[&str]) {
        *self.list.lock().unwrap() = urls.iter().map(|s| s.to_string()).collect();
    }

    fn url(&self) -> String {
        format!("{}/list.txt", self.base)
    }
}

async fn start_list_server() -> TrackerListServer {
    let list: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let l = list.clone();
    let app = axum::Router::new().route(
        "/list.txt",
        axum::routing::get(move || {
            let l = l.clone();
            async move {
                let body = l.lock().unwrap().join("\n");
                axum::response::Response::new(axum::body::Body::from(body))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    TrackerListServer {
        base: format!("http://{addr}"),
        list,
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("xfer-sub-{tag}-{}", std::process::id()))
}

/// 轮询等待条件成立（最多 10s）。
async fn wait_for<F: Fn() -> bool>(cond: F) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

/// 新增订阅源后，引擎后台循环立即拉取并更新全局 Tracker 列表
/// （此前仅 TUI 添加路径手动刷新一次，RPC 等其他客户端要等后台
/// 周期——即用户反馈的"只有首次添加会自动更新"的根因之一）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_subscription_kicks_immediate_refresh() {
    let dir = temp_dir("kick");
    let srv = start_list_server().await;
    srv.set(&[
        "udp://tracker.example.com:1337/announce",
        "http://tracker.example.com/announce",
    ]);
    // start() 会 spawn_scheduler（含订阅刷新循环）
    let mgr = TaskManager::start(dir.clone(), 1);

    let sub = mgr.add_subscription("测试订阅", &srv.url(), true).unwrap();
    assert!(sub.enabled);

    // kick → 后台循环唤醒 → 拉取 → 合并（本地服务 <1s）
    let ok = wait_for(|| mgr.get_global_trackers().len() == 2).await;
    assert!(
        ok,
        "添加订阅源后应立即获取 tracker，实际: {:?}",
        mgr.get_global_trackers()
    );
    let trackers = mgr.get_global_trackers();
    assert!(trackers.contains(&"udp://tracker.example.com:1337/announce".to_string()));
    assert!(trackers.contains(&"http://tracker.example.com/announce".to_string()));

    // 订阅状态已更新
    let subs = mgr.get_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].last_count, 2);
    assert!(subs[0].last_error.is_empty());
    assert!(subs[0].last_updated > 0);

    let _ = std::fs::remove_dir_all(&dir);
}

/// 订阅源更新的同步语义：远程移除的 tracker 从全局列表消失、
/// 新增的出现（此前 merge-only 只增不减，用户表现为"列表从不更新"）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_syncs_remote_add_and_removal() {
    let dir = temp_dir("sync");
    let srv = start_list_server().await;
    let mgr = TaskManager::new(dir.clone(), 1);

    srv.set(&["udp://a.example/ann", "udp://b.example/ann", "udp://c.example/ann"]);
    let sub = mgr.add_subscription("", &srv.url(), true).unwrap();
    mgr.refresh_subscription(&sub.id).await.unwrap();
    assert_eq!(mgr.get_global_trackers().len(), 3);

    // 远程列表变化：c 移除、d 新增，a/b 保留
    srv.set(&["udp://a.example/ann", "udp://b.example/ann", "udp://d.example/ann"]);
    mgr.refresh_subscription(&sub.id).await.unwrap();

    let trackers = mgr.get_global_trackers();
    assert_eq!(trackers.len(), 3, "总数不变（一增一减）");
    assert!(!trackers.contains(&"udp://c.example/ann".to_string()), "远程已移除的应剔除");
    assert!(trackers.contains(&"udp://d.example/ann".to_string()), "远程新增的应加入");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 手动添加的 tracker 与订阅源重叠时，订阅同步不得移除它。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_tracker_survives_subscription_sync() {
    let dir = temp_dir("manual");
    let srv = start_list_server().await;
    let mgr = TaskManager::new(dir.clone(), 1);

    mgr.add_global_tracker("udp://manual.example/ann").unwrap();
    srv.set(&["udp://manual.example/ann", "udp://sub.example/ann"]);
    let sub = mgr.add_subscription("", &srv.url(), true).unwrap();
    mgr.refresh_subscription(&sub.id).await.unwrap();
    assert_eq!(mgr.get_global_trackers().len(), 2);

    // 订阅源不再提供 manual.example —— 但它是手动添加的，必须保留
    srv.set(&["udp://sub.example/ann"]);
    mgr.refresh_subscription(&sub.id).await.unwrap();

    let trackers = mgr.get_global_trackers();
    assert!(
        trackers.contains(&"udp://manual.example/ann".to_string()),
        "手动添加的 tracker 不应被订阅同步移除: {trackers:?}"
    );
    assert!(trackers.contains(&"udp://sub.example/ann".to_string()));

    let _ = std::fs::remove_dir_all(&dir);
}

/// 多订阅源重叠：A 移除后 B 仍提供的 tracker 保留。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_tracker_survives_when_one_sub_drops() {
    let dir = temp_dir("shared");
    let srv_a = start_list_server().await;
    let srv_b = start_list_server().await;
    let mgr = TaskManager::new(dir.clone(), 1);

    let shared = "udp://shared.example/ann";
    srv_a.set(&[shared, "udp://only-a.example/ann"]);
    srv_b.set(&[shared, "udp://only-b.example/ann"]);
    let a = mgr.add_subscription("A", &srv_a.url(), true).unwrap();
    let b = mgr.add_subscription("B", &srv_b.url(), true).unwrap();
    mgr.refresh_subscription(&a.id).await.unwrap();
    mgr.refresh_subscription(&b.id).await.unwrap();
    assert_eq!(mgr.get_global_trackers().len(), 3);

    // A 不再提供 only-a（shared 仍提供）→ only-a 应剔除
    srv_a.set(&[shared]);
    mgr.refresh_subscription(&a.id).await.unwrap();
    let trackers = mgr.get_global_trackers();
    assert_eq!(trackers.len(), 2, "A 独有且远程已移除的应剔除: {trackers:?}");
    assert!(trackers.contains(&shared.to_string()));
    assert!(trackers.contains(&"udp://only-b.example/ann".to_string()));

    // A 连 shared 也不再提供（换成 only-a2）→ shared 因 B 仍提供而保留
    srv_a.set(&["udp://only-a2.example/ann"]);
    mgr.refresh_subscription(&a.id).await.unwrap();
    let trackers = mgr.get_global_trackers();
    assert!(
        trackers.contains(&shared.to_string()),
        "B 仍提供的 shared 不应被 A 的刷新移除: {trackers:?}"
    );
    assert!(trackers.contains(&"udp://only-b.example/ann".to_string()));
    assert!(trackers.contains(&"udp://only-a2.example/ann".to_string()));
    assert_eq!(trackers.len(), 3);

    let _ = std::fs::remove_dir_all(&dir);
}

/// 远程返回空列表：视为异常，保留现有 tracker（防误清空）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_remote_list_preserves_trackers() {
    let dir = temp_dir("empty");
    let srv = start_list_server().await;
    let mgr = TaskManager::new(dir.clone(), 1);

    srv.set(&["udp://keep-1.example/ann", "udp://keep-2.example/ann"]);
    let sub = mgr.add_subscription("", &srv.url(), true).unwrap();
    mgr.refresh_subscription(&sub.id).await.unwrap();
    assert_eq!(mgr.get_global_trackers().len(), 2);

    srv.set(&[]); // 远程列表变空（配置错误/网关劫持）
    let r = mgr.refresh_subscription(&sub.id).await;
    assert!(r.is_err(), "空列表应报错");
    assert_eq!(mgr.get_global_trackers().len(), 2, "现有 tracker 不应被清空");
    let subs = mgr.get_subscriptions();
    assert!(!subs[0].last_error.is_empty(), "应记录错误信息");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 禁用订阅源：其贡献的 tracker 回收（手动/其他订阅源的保留），
/// 重新启用后立即恢复。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disable_and_reenable_subscription() {
    let dir = temp_dir("toggle");
    let srv = start_list_server().await;
    let mgr = TaskManager::new(dir.clone(), 1);

    mgr.add_global_tracker("udp://manual.example/ann").unwrap();
    srv.set(&["udp://s1.example/ann", "udp://s2.example/ann"]);
    let sub = mgr.add_subscription("", &srv.url(), true).unwrap();
    mgr.refresh_subscription(&sub.id).await.unwrap();
    assert_eq!(mgr.get_global_trackers().len(), 3);

    // 禁用：贡献的 s1/s2 回收，手动 manual 保留
    mgr.toggle_subscription(&sub.id).unwrap();
    let trackers = mgr.get_global_trackers();
    assert_eq!(trackers.len(), 1, "禁用后仅剩手动 tracker: {trackers:?}");
    assert!(trackers.contains(&"udp://manual.example/ann".to_string()));

    // 重新启用：引擎 kick 立即拉取（无调度器环境下手动刷新验证恢复路径）
    mgr.toggle_subscription(&sub.id).unwrap();
    mgr.refresh_subscription(&sub.id).await.unwrap();
    assert_eq!(mgr.get_global_trackers().len(), 3, "重新启用后恢复");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 移除订阅源：其贡献的 tracker 一并回收。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_subscription_reclaims_trackers() {
    let dir = temp_dir("remove");
    let srv = start_list_server().await;
    let mgr = TaskManager::new(dir.clone(), 1);

    srv.set(&["udp://x.example/ann", "udp://y.example/ann"]);
    let sub = mgr.add_subscription("", &srv.url(), true).unwrap();
    mgr.refresh_subscription(&sub.id).await.unwrap();
    assert_eq!(mgr.get_global_trackers().len(), 2);

    mgr.remove_subscription(&sub.id).unwrap();
    assert!(
        mgr.get_global_trackers().is_empty(),
        "移除订阅源应回收其贡献的 tracker: {:?}",
        mgr.get_global_trackers()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// TTL 过滤：刚刷新过的订阅源跳过（24h 内不重复拉取），从未
/// 更新过的会刷新。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_refresh_skips_fresh_subscriptions() {
    let dir = temp_dir("ttl");
    let srv = start_list_server().await;
    let mgr = TaskManager::new(dir.clone(), 1);

    srv.set(&["udp://fresh.example/ann"]);
    let fresh = mgr.add_subscription("fresh", &srv.url(), true).unwrap();
    mgr.refresh_subscription(&fresh.id).await.unwrap();
    assert_eq!(mgr.get_global_trackers().len(), 1);

    // 未更新的订阅源（模拟另一个 URL）
    let srv2 = start_list_server().await;
    srv2.set(&["udp://stale.example/ann"]);
    let _stale = mgr.add_subscription("stale", &srv2.url(), true).unwrap();

    // TTL 刷新：stale（last_updated=0）会被拉取；fresh 刚更新过被跳过。
    // 注意：fresh 的 tracker 已在全局列表，即使被错误重拉也观察不到
    // 区别——这里通过"结果列表恰好包含两者"验证整体行为正确。
    let n = mgr.refresh_expired_subscriptions().await;
    assert_eq!(n, 1, "仅未更新过的订阅源应被刷新");
    let trackers = mgr.get_global_trackers();
    assert!(trackers.contains(&"udp://fresh.example/ann".to_string()));
    assert!(trackers.contains(&"udp://stale.example/ann".to_string()));
    let subs = mgr.get_subscriptions();
    assert!(subs.iter().all(|s| s.last_updated > 0), "两个订阅源都应有更新时间");

    // 再跑一次 TTL：全部刚更新过 → 无一刷新
    let n = mgr.refresh_expired_subscriptions().await;
    assert_eq!(n, 0, "24h 内的订阅源不应重复刷新");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 来源标记的会话持久化：重启后订阅同步语义保持（手动 tracker
/// 与订阅 tracker 的区分不丢失）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tracker_sources_persist_across_restart() {
    let dir = temp_dir("persist");
    let session = dir.join("session.json");
    let srv = start_list_server().await;

    {
        let mgr = TaskManager::start_with_session(Some(dir.clone()), Some(1), session.clone());
        mgr.add_global_tracker("udp://manual.example/ann").unwrap();
        srv.set(&["udp://sub-a.example/ann", "udp://sub-b.example/ann"]);
        let sub = mgr.add_subscription("持久化", &srv.url(), true).unwrap();
        mgr.refresh_subscription(&sub.id).await.unwrap();
        assert_eq!(mgr.get_global_trackers().len(), 3);
    }

    // 重启恢复
    let mgr = TaskManager::start_with_session(Some(dir.clone()), Some(1), session.clone());
    assert_eq!(mgr.get_global_trackers().len(), 3, "tracker 列表应恢复");
    assert_eq!(mgr.get_subscriptions().len(), 1, "订阅源应恢复");

    // 恢复后同步语义仍然生效：sub-b 被远程移除 → 剔除；manual 保留
    srv.set(&["udp://sub-a.example/ann"]);
    let subs = mgr.get_subscriptions();
    mgr.refresh_subscription(&subs[0].id).await.unwrap();
    let trackers = mgr.get_global_trackers();
    assert_eq!(trackers.len(), 2, "恢复后同步语义应保持: {trackers:?}");
    assert!(trackers.contains(&"udp://manual.example/ann".to_string()));
    assert!(trackers.contains(&"udp://sub-a.example/ann".to_string()));
    assert!(
        !trackers.contains(&"udp://sub-b.example/ann".to_string()),
        "远程已移除的 tracker 恢复后仍应剔除"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
