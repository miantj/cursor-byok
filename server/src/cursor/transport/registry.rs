//! Maps request IDs to active transport handles.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::sync::{mpsc, Mutex, Notify};

use crate::{
    cursor::{
        conversation::ConversationRegistry, prompting::PromptCompiler,
        services::observability::CursorTraceService,
    },
    plugin::PluginRegistry,
    provider::Provider,
    search::WebCache,
    store::Store,
    Result,
};

use super::{BugbotReplay, OutputHub, SharedBugbotReplay, TransportHandle};

/// 关闭后的 request 仍可能收到客户端尾包。保留这一小段记录，避免把尾包当成缺少模型的首包。
const CLOSED_ROUTE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct TransportRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    local: Mutex<HashMap<String, LocalTransport>>,
    next_local_generation: AtomicU64,
    upstream: parking_lot::Mutex<HashMap<String, UpstreamTransport>>,
    next_upstream_generation: AtomicU64,
    closed: parking_lot::Mutex<HashMap<String, Instant>>,
    route_changed: Notify,
    store: Store,
    traces: CursorTraceService,
    web_cache: WebCache,
    plugins: Option<PluginRegistry>,
    conversations: ConversationRegistry,
}

#[derive(Clone)]
struct LocalTransport {
    generation: u64,
    handle: TransportHandle,
}

struct UpstreamTransport {
    generation: u64,
    subscribers: usize,
    bugbot_replay: Option<SharedBugbotReplay>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportRoute {
    Local,
    Upstream(u64),
}

impl TransportRegistry {
    pub fn new(store: Store, provider: Arc<dyn Provider>, compiler: PromptCompiler) -> Self {
        Self::with_web_cache(store, provider, compiler, WebCache::default())
    }

    pub fn with_web_cache(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        web_cache: WebCache,
    ) -> Self {
        Self::build(store, provider, compiler, web_cache, None, None)
    }

    /// 附带本地 rules 目录的构造;编译请求上下文时会合并该目录下的 md 规则。
    pub fn with_local_rules(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        local_rules_dir: std::path::PathBuf,
    ) -> Self {
        Self::build(
            store,
            provider,
            compiler,
            WebCache::default(),
            None,
            Some(local_rules_dir),
        )
    }

    pub fn with_plugins(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        web_cache: WebCache,
        plugins: PluginRegistry,
        local_rules_dir: std::path::PathBuf,
    ) -> Self {
        Self::build(
            store,
            provider,
            compiler,
            web_cache,
            Some(plugins),
            Some(local_rules_dir),
        )
    }

    fn build(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        web_cache: WebCache,
        plugins: Option<PluginRegistry>,
        local_rules_dir: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                local: Mutex::new(HashMap::new()),
                next_local_generation: AtomicU64::new(1),
                upstream: parking_lot::Mutex::new(HashMap::new()),
                next_upstream_generation: AtomicU64::new(1),
                closed: parking_lot::Mutex::new(HashMap::new()),
                route_changed: Notify::new(),
                traces: CursorTraceService::new(store.clone()),
                conversations: ConversationRegistry::new(
                    store.clone(),
                    provider,
                    compiler,
                    web_cache.clone(),
                    local_rules_dir,
                ),
                store,
                web_cache,
                plugins,
            }),
        }
    }

    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    pub fn trace(
        &self,
        request_id: &str,
    ) -> crate::cursor::services::observability::CursorTraceRecorder {
        self.inner.traces.recorder(request_id)
    }

    pub fn web_cache(&self) -> &WebCache {
        &self.inner.web_cache
    }

    pub fn plugins(&self) -> Option<&PluginRegistry> {
        self.inner.plugins.as_ref()
    }

    pub fn conversations(&self) -> &ConversationRegistry {
        &self.inner.conversations
    }

    pub async fn get_or_create(&self, request_id: &str) -> Result<TransportHandle> {
        self.get_or_create_for_append(request_id, false).await
    }

    pub(crate) async fn get_or_create_for_append(
        &self,
        request_id: &str,
        replace_closing: bool,
    ) -> Result<TransportHandle> {
        let mut local = self.inner.local.lock().await;
        if let Some(transport) = local.get(request_id) {
            if transport.handle.accepting_appends() || !replace_closing {
                return Ok(transport.handle.clone());
            }
        }
        local.remove(request_id);
        let (commands, receiver) = mpsc::channel(128);
        let output = Arc::new(OutputHub::default());
        let trace = self.inner.traces.recorder(request_id);
        trace.resume();
        let handle = TransportHandle::new(request_id.into(), commands, output, trace);
        let generation = self
            .inner
            .next_local_generation
            .fetch_add(1, Ordering::Relaxed);
        local.insert(
            request_id.into(),
            LocalTransport {
                generation,
                handle: handle.clone(),
            },
        );
        self.inner.closed.lock().remove(request_id);
        drop(local);
        self.inner.route_changed.notify_waiters();
        self.inner
            .conversations
            .bind_transport(handle.clone(), receiver);

        let registry = Arc::downgrade(&self.inner);
        let request_id = request_id.to_string();
        let lifecycle = handle.clone();
        tokio::spawn(async move {
            lifecycle.wait_transport_closed().await;
            if let Some(registry) = registry.upgrade() {
                let mut local = registry.local.lock().await;
                if local
                    .get(&request_id)
                    .is_some_and(|transport| transport.generation == generation)
                {
                    local.remove(&request_id);
                    registry.record_closed(&request_id);
                }
            }
        });
        Ok(handle)
    }

    pub async fn local(&self, request_id: &str) -> Option<TransportHandle> {
        self.inner
            .local
            .lock()
            .await
            .get(request_id)
            .map(|transport| transport.handle.clone())
    }

    pub(crate) async fn mark_upstream(&self, request_id: &str, replay: Option<BugbotReplay>) {
        let mut upstream = self.inner.upstream.lock();
        let generation = self
            .inner
            .next_upstream_generation
            .fetch_add(1, Ordering::Relaxed);
        upstream.insert(
            request_id.into(),
            UpstreamTransport {
                generation,
                subscribers: 0,
                bugbot_replay: replay.map(|replay| Arc::new(Mutex::new(replay))),
            },
        );
        self.inner.closed.lock().remove(request_id);
        drop(upstream);
        self.inner.route_changed.notify_waiters();
    }

    pub async fn upstream(&self, request_id: &str) -> bool {
        self.inner.upstream.lock().contains_key(request_id)
    }

    pub(crate) async fn bugbot_replay(
        &self,
        request_id: &str,
    ) -> Option<(u64, SharedBugbotReplay)> {
        self.inner
            .upstream
            .lock()
            .get(request_id)
            .and_then(|transport| {
                transport
                    .bugbot_replay
                    .clone()
                    .map(|replay| (transport.generation, replay))
            })
    }

    pub async fn wait_route_for(
        &self,
        request_id: &str,
        timeout: std::time::Duration,
    ) -> Option<TransportRoute> {
        tokio::time::timeout(timeout, self.wait_for_route(request_id, true))
            .await
            .ok()
            .flatten()
    }

    pub async fn wait_route(&self, request_id: &str) -> TransportRoute {
        self.wait_for_route(request_id, false)
            .await
            .expect("stream waits for an active route")
    }

    async fn wait_for_route(
        &self,
        request_id: &str,
        stop_when_closed: bool,
    ) -> Option<TransportRoute> {
        loop {
            let changed = self.inner.route_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.inner.local.lock().await.contains_key(request_id) {
                return Some(TransportRoute::Local);
            }
            if let Some(transport) = self.inner.upstream.lock().get(request_id) {
                return Some(TransportRoute::Upstream(transport.generation));
            }
            if stop_when_closed && self.recently_closed(request_id) {
                return None;
            }
            changed.await;
        }
    }

    pub(crate) fn subscribe_upstream(&self, request_id: &str, generation: u64) -> Result<()> {
        let mut upstream = self.inner.upstream.lock();
        let transport = upstream
            .get_mut(request_id)
            .filter(|transport| transport.generation == generation)
            .ok_or_else(|| crate::Error::RunNotFound(request_id.into()))?;
        transport.subscribers += 1;
        Ok(())
    }

    pub(crate) fn unsubscribe_upstream(&self, request_id: &str, generation: u64) {
        let mut upstream = self.inner.upstream.lock();
        let Some(transport) = upstream
            .get_mut(request_id)
            .filter(|transport| transport.generation == generation)
        else {
            return;
        };
        transport.subscribers -= 1;
        if transport.subscribers == 0 {
            upstream.remove(request_id);
            self.inner.record_closed(request_id);
        }
    }

    pub fn finish_upstream(&self, request_id: String, generation: u64) {
        let mut upstream = self.inner.upstream.lock();
        if upstream
            .get(&request_id)
            .is_some_and(|transport| transport.generation == generation)
        {
            upstream.remove(&request_id);
            self.inner.record_closed(&request_id);
        }
    }

    pub(crate) fn recently_closed(&self, request_id: &str) -> bool {
        let mut closed = self.inner.closed.lock();
        let Some(closed_at) = closed.get(request_id).copied() else {
            return false;
        };
        if closed_at.elapsed() < CLOSED_ROUTE_TTL {
            return true;
        }
        closed.remove(request_id);
        false
    }

    pub async fn shutdown(&self) {
        self.inner.conversations.shutdown().await;
        let handles = std::mem::take(&mut *self.inner.local.lock().await);
        self.inner.upstream.lock().clear();
        for transport in handles.into_values() {
            transport.handle.disconnect().await;
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                transport.handle.wait_transport_closed(),
            )
            .await;
        }
    }
}

impl RegistryInner {
    fn record_closed(&self, request_id: &str) {
        let mut closed = self.closed.lock();
        let now = Instant::now();
        closed.retain(|_, closed_at| now.duration_since(*closed_at) < CLOSED_ROUTE_TTL);
        closed.insert(request_id.to_owned(), now);
        self.route_changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;
    use std::{sync::Arc, time::Duration};

    use tokio_util::sync::CancellationToken;

    use crate::{
        cursor::prompting::{PromptAssets, PromptCompiler},
        model::ModelInvocation,
        provider::{Provider, ProviderStream},
        store::Store,
    };

    use super::{TransportRegistry, TransportRoute};

    struct UnusedProvider;

    impl Provider for UnusedProvider {
        fn stream(&self, _: ModelInvocation, _: CancellationToken) -> ProviderStream {
            panic!("route wait must not start a provider")
        }
    }

    async fn registry() -> (tempfile::TempDir, TransportRegistry) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::connect(&format!(
            "sqlite://{}",
            directory.path().join("test.db").display()
        ))
        .await
        .unwrap();
        (
            directory,
            TransportRegistry::new(
                store,
                Arc::new(UnusedProvider),
                PromptCompiler::new(PromptAssets::embedded().unwrap()),
            ),
        )
    }

    #[tokio::test]
    async fn route_wait_returns_an_existing_route() {
        let (_directory, registry) = registry().await;
        registry.mark_upstream("ready", None).await;
        assert_eq!(
            registry
                .wait_route_for("ready", Duration::from_millis(50))
                .await,
            Some(TransportRoute::Upstream(1))
        );
    }

    #[tokio::test]
    async fn route_wait_observes_a_route_registered_while_waiting() {
        let (_directory, registry) = registry().await;
        let pending = registry.wait_route_for("late", Duration::from_secs(2));
        tokio::pin!(pending);
        tokio::select! {
            _ = &mut pending => panic!("route appeared before it was registered"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        registry.mark_upstream("late", None).await;
        assert_eq!(pending.await, Some(TransportRoute::Upstream(1)));
    }

    #[tokio::test]
    async fn route_wait_returns_none_when_nothing_registers() {
        let (_directory, registry) = registry().await;
        assert_eq!(
            registry
                .wait_route_for("missing", Duration::from_millis(50))
                .await,
            None
        );
    }

    #[tokio::test]
    async fn a_closed_local_route_is_remembered_until_it_is_reused() {
        let (_directory, registry) = registry().await;
        let handle = registry.get_or_create("done").await.unwrap();
        handle.close_transport();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !registry.recently_closed("done") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closed route was not recorded");
        assert!(!registry.recently_closed("never"));
        assert!(registry
            .wait_route_for("done", Duration::from_secs(60))
            .now_or_never()
            .unwrap()
            .is_none());

        crate::api::cursor::bidi::append(
            &registry,
            crate::api::cursor::bidi::DecodedAppend {
                request_id: "done".into(),
                seqno: 1,
                message: Default::default(),
            },
            None,
        )
        .await
        .unwrap();
        assert!(registry.local("done").await.is_none());

        registry.mark_upstream("done", None).await;
        assert!(!registry.recently_closed("done"));
    }

    #[tokio::test]
    async fn finishing_an_upstream_route_remembers_it() {
        let (_directory, registry) = registry().await;
        registry.mark_upstream("upstream", None).await;
        registry.finish_upstream("upstream".into(), 1);
        assert!(registry.recently_closed("upstream"));
        assert!(!registry.upstream("upstream").await);
    }
}
