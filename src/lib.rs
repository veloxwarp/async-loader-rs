use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{body::Body, http::Response, response::IntoResponse, Json};
use parking_lot::{Mutex, RwLock};
#[cfg(test)]
use reqwest::header::CACHE_CONTROL;
use reqwest::StatusCode;
use tokio::sync::broadcast;

/// Provides access to a cached value with automatic reload.
pub struct AsyncLoader<Loader: CacheLoader> {
    pub inner: Arc<Inner<Loader>>,
}

// Manual impl to avoid a Clone bound on Loader
impl<Loader: CacheLoader> Clone for AsyncLoader<Loader> {
    fn clone(&self) -> Self {
        AsyncLoader {
            inner: self.inner.clone(),
        }
    }
}

type ArcInnerMut<Loader> = Arc<Mutex<InnerMut<Loader>>>;

pub struct Inner<Loader: CacheLoader> {
    /// Mutable pieces of the inner storage, per key
    per_key: RwLock<HashMap<Loader::Key, ArcInnerMut<Loader>>>,
    pub config: AsyncLoaderConfig,
    /// Actual value used for loading new values
    loader: Loader,
}

#[derive(Debug, serde::Serialize)]
pub struct LoaderMetric {
    length: usize,
    capacity: usize,
}

impl<Loader: CacheLoader> Inner<Loader> {
    pub fn metrics(&self) -> LoaderMetric {
        let key = self.per_key.read();
        let length = key.len();
        let capacity = key.capacity();
        LoaderMetric { length, capacity }
    }
}

pub struct AsyncLoaderBuilder {
    /// How long after we load a value should we trigger a reload?
    ///
    /// Default: 1 minute
    pub reload_delay: Option<Duration>,
    /// How long after the reload delay should we consider a value too old and refuse to return it?
    ///
    /// Default: same as reload_delay
    pub stale_delay: StaleDelay,
    /// Should we store cache ?
    ///
    /// Default: true
    pub store_cache: bool,
    /// Should we disable cache loader ?
    ///
    /// Default: false
    pub disable_loader: bool,
}

#[derive(Default)]
pub enum StaleDelay {
    /// Default will be twice the [AsyncLoaderBuilder::reload_delay]
    #[default]
    Default,
    Duration(Duration),
    /// Allows last computed stale value in case of an error during
    /// the load step.
    AllowStale,
}

pub struct AsyncLoaderConfig {
    reload_delay: Duration,
    /// None if we never go stale
    reload_and_stale_delay: Option<Duration>,
    /// Store cache
    store_cache: bool,
    /// Disable cache
    disable_loader: bool,
}

impl From<&AsyncLoaderBuilder> for AsyncLoaderConfig {
    fn from(value: &AsyncLoaderBuilder) -> Self {
        let reload_delay = value
            .reload_delay
            .unwrap_or_else(|| Duration::from_secs(60));
        AsyncLoaderConfig {
            reload_delay,
            reload_and_stale_delay: match value.stale_delay {
                StaleDelay::Default => Some(reload_delay + reload_delay),
                StaleDelay::Duration(stale_delay) => Some(reload_delay + stale_delay),
                StaleDelay::AllowStale => None,
            },
            store_cache: value.store_cache,
            disable_loader: value.disable_loader,
        }
    }
}

struct InnerMut<Loader: CacheLoader> {
    /// The latest value loaded, if available.
    latest: Option<Latest<Loader>>,
    /// A receiver to subscribe to the newest update.
    ///
    /// Only set if there is currently an update occurring.
    broadcast: Option<broadcast::Sender<ValueForRender<Loader>>>,
}

pub struct Latest<Loader: CacheLoader> {
    /// When was this value loaded? Used for cache calculations
    pub loaded: Instant,
    pub value: Arc<Loader::Output>,
}

impl<Loader: CacheLoader> Clone for Latest<Loader> {
    fn clone(&self) -> Self {
        Latest {
            loaded: self.loaded,
            value: self.value.clone(),
        }
    }
}

/// Can we use the latest value?
enum UseLatest<Loader: CacheLoader> {
    /// No value found, or the value found was too old
    NoValue,
    /// Value found, but it's stale and shouldn't be used unless explicitly looking for stale values
    StaleValue(Latest<Loader>),
    /// We can use a value, but we should start reloading for a fresher cache
    UseAndReload(ValueForRender<Loader>),
    /// Value is recent enough, no reloading required
    UseNoReload(ValueForRender<Loader>),
}

impl<Loader: CacheLoader> Debug for UseLatest<Loader> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UseLatest::NoValue => write!(f, "NoValue"),
            UseLatest::StaleValue(_) => write!(f, "StaleValue"),
            UseLatest::UseAndReload(_) => write!(f, "UseAndReload"),
            UseLatest::UseNoReload(_) => write!(f, "UseNoReload"),
        }
    }
}

pub enum ValueForRender<Loader: CacheLoader> {
    /// Value was loaded successfully
    Success {
        value: Arc<Loader::Output>,
        loaded: Instant,
    },
    /// Error while loading the value
    Error {
        message: Arc<str>,
        status: StatusCode,
        /// If there's a cached value available that's considered stale, provide it here.
        stale_cache: Option<Latest<Loader>>,
    },
}

/// An error type that wraps up a [axum::Response].
///
/// If you return this as an error value from [CacheLoader::load],
/// it will be used as the error response.
#[derive(Clone, Debug)]
pub struct ErrorResponse {
    pub status: StatusCode,
    pub message: String,
}

impl std::error::Error for ErrorResponse {}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl<Loader: CacheLoader> Clone for ValueForRender<Loader> {
    fn clone(&self) -> Self {
        match self {
            ValueForRender::Success { value, loaded } => ValueForRender::Success {
                value: value.clone(),
                loaded: *loaded,
            },
            ValueForRender::Error {
                message,
                status,
                stale_cache,
            } => ValueForRender::Error {
                message: message.clone(),
                status: *status,
                stale_cache: stale_cache.clone(),
            },
        }
    }
}

impl<Loader: CacheLoader> ValueForRender<Loader> {
    /// Convert into an axum Response
    ///
    /// Preserves loader-provided headers, status codes, and content-type.
    /// HTTP cache policy belongs to the caller; no Cache-Control is added.
    pub fn into_response(self, _config: &AsyncLoaderConfig) -> axum::response::Response {
        match self {
            ValueForRender::Success { value, .. } => Loader::into_response(&*value),
            ValueForRender::Error {
                message,
                status,
                stale_cache: _,
            } => (status, message.as_ref().to_owned()).into_response(),
        }
    }
}

impl<Loader: CacheLoader> Latest<Loader> {
    /// Check whether we're allowed to use this value given how much time has passed.
    fn can_use(&self, config: &AsyncLoaderConfig) -> UseLatest<Loader> {
        let age = self.loaded.elapsed();

        if let Some(delay) = config.reload_and_stale_delay {
            if age > delay {
                // Too old, return no value or the stale value
                return UseLatest::StaleValue(self.clone());
            }
        }

        // We can use the value, but determine whether we need to refresh the cache
        if age > config.reload_delay {
            UseLatest::UseAndReload(ValueForRender::Success {
                value: self.value.clone(),
                loaded: self.loaded,
            })
        } else {
            UseLatest::UseNoReload(ValueForRender::Success {
                value: self.value.clone(),
                loaded: self.loaded,
            })
        }
    }
}

/// Something that can load a value for the cacher.
pub trait CacheLoader: Send + Sync + 'static {
    type Key: std::hash::Hash + Eq + Clone + Send + Sync + 'static;
    type Output: serde::Serialize + Send + Sync + 'static;

    fn load(&self, key: Self::Key) -> impl Future<Output = anyhow::Result<Self::Output>> + Send;

    /// Convert the [Self::Output] value into an axum [Response].
    ///
    /// By default, this will convert to a JSON representation.
    fn into_response(output: &Self::Output) -> Response<Body> {
        Json(output).into_response()
    }

    /// Convert the error output into an error message and status code.
    ///
    /// By default, uses the [Display] trait and an internal server error (500 status code).
    fn display_error(err: anyhow::Error) -> (String, StatusCode) {
        (err.to_string(), StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl AsyncLoaderBuilder {
    pub fn build<Loader: CacheLoader>(&self, loader: Loader) -> AsyncLoader<Loader> {
        AsyncLoader {
            inner: Arc::new(Inner {
                per_key: RwLock::new(HashMap::new()),
                config: self.into(),
                loader,
            }),
        }
    }
}

enum BroadcastOrValue<Loader: CacheLoader> {
    Broadcast {
        broadcast: broadcast::Receiver<ValueForRender<Loader>>,
        stale_cache: Option<Latest<Loader>>,
    },
    Value(ValueForRender<Loader>),
}

impl<Loader: CacheLoader> Debug for BroadcastOrValue<Loader> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Broadcast {
                broadcast: _,
                stale_cache: _,
            } => write!(f, "Broadcast"),
            Self::Value(_) => write!(f, "value"),
        }
    }
}

impl<Loader: CacheLoader> AsyncLoader<Loader> {
    /// Get a new response from this cacher.
    ///
    /// Response may be an error response.
    pub async fn get_response(&self, key: Loader::Key) -> axum::response::Response {
        self.get_value(key).await.into_response(&self.inner.config)
    }

    /// Get a new response from this cacher, but allows stale value.
    ///
    /// Response may be an error response.
    pub async fn get_response_allow_stale(&self, key: Loader::Key) -> axum::response::Response {
        self.get_value_allow_stale(key)
            .await
            .into_response(&self.inner.config)
    }

    /// Get the underlying value and how old it is.
    pub async fn get_value(&self, key: Loader::Key) -> ValueForRender<Loader> {
        let config = self.get_cacher_config();

        // Get either the current value or a channel to wait for values on.
        let broadcast_or_value = if config.disable_loader {
            self.new_broadcast(key)
        } else {
            self.get_broadcast_or_value(key, false)
        };
        match broadcast_or_value {
            // Need to wait for a new value to be broadcast
            BroadcastOrValue::Broadcast {
                mut broadcast,
                stale_cache,
            } => match broadcast.recv().await {
                // Success!
                Ok(value) => value,
                // This should never happen, it indicates a bug in the code. It
                // may occur if the loader itself panics.
                Err(err) => {
                    tracing::error!("AsyncLoader::get_response: impossible Err received: {err:?}");
                    ValueForRender::Error {
                        message: "Internal error detected: broadcast channel is gone".into(),
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        stale_cache,
                    }
                }
            },
            // Value was already available
            BroadcastOrValue::Value(value) => value,
        }
    }

    /// Get the underlying value, but fetch it newly this time
    pub async fn get_latest_value(&self, key: Loader::Key) -> ValueForRender<Loader> {
        let config = self.get_cacher_config();

        // Get either the current value or a channel to wait for values on.
        let broadcast_or_value = if config.disable_loader {
            self.new_broadcast(key)
        } else {
            self.get_broadcast_or_value(key, true)
        };
        match broadcast_or_value {
            // Need to wait for a new value to be broadcast
            BroadcastOrValue::Broadcast {
                mut broadcast,
                stale_cache,
            } => match broadcast.recv().await {
                // Success!
                Ok(value) => value,
                // This should never happen, it indicates a bug in the code. It
                // may occur if the loader itself panics.
                Err(err) => {
                    tracing::error!("AsyncLoader::get_response: impossible Err received: {err:?}");
                    ValueForRender::Error {
                        message: "Internal error detected: broadcast channel is gone".into(),
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        stale_cache,
                    }
                }
            },
            // Value was already available
            BroadcastOrValue::Value(value) => value,
        }
    }

    /// Get cacher config
    pub fn get_cacher_config(&self) -> &AsyncLoaderConfig {
        &self.inner.config
    }

    /// Get a value without paying attention to whether it's stale
    ///
    /// Returns an error if there is no cached value and loading fails.
    pub async fn get_value_allow_stale(&self, key: Loader::Key) -> ValueForRender<Loader> {
        let result = self.get_value(key).await;
        match result {
            ValueForRender::Success { .. } => result,
            ValueForRender::Error {
                stale_cache: Some(stale_cache),
                message,
                status,
            } => {
                tracing::error!("AsyncLoader::get_value_allow_stale: using stale cache, status=={status}, message: {message}");
                ValueForRender::Success {
                    value: stale_cache.value,
                    loaded: stale_cache.loaded,
                }
            }
            ValueForRender::Error { .. } => result,
        }
    }

    /// Builds on [Self::get_value_allow_stale] by returning a Result.
    pub async fn get_value_or_error(
        &self,
        key: Loader::Key,
    ) -> anyhow::Result<Arc<Loader::Output>> {
        match self.get_value_allow_stale(key).await {
            ValueForRender::Success { value, loaded: _ } => Ok(value),
            ValueForRender::Error {
                message,
                status: _,
                stale_cache,
            } => match stale_cache {
                Some(latest) => Ok(latest.value),
                None => Err(anyhow::anyhow!("{message}")),
            },
        }
    }

    fn get_mutex_by_key(&self, key: &Loader::Key) -> Arc<Mutex<InnerMut<Loader>>> {
        {
            // First: optimistically try to get the existing value with just a read guard
            let read_guard = self.inner.per_key.read();

            if let Some(value) = read_guard.get(key) {
                return value.clone();
            }
        }

        // That failed, we've dropped the read guard, now try again with a write guard
        let mut write_guard = self.inner.per_key.write();

        if let Some(value) = write_guard.get(key) {
            return value.clone();
        }

        // Write guard also didn't have it, so now we fill in the structure.
        let value = Arc::new(Mutex::new(InnerMut {
            latest: None,
            broadcast: None,
        }));

        write_guard.insert(key.clone(), value.clone());
        value
    }

    fn new_broadcast(&self, key: Loader::Key) -> BroadcastOrValue<Loader> {
        let (sender, receiver) = broadcast::channel(1);
        tokio::task::spawn(self.clone().do_update(sender, key, None));
        BroadcastOrValue::Broadcast {
            broadcast: receiver,
            stale_cache: None,
        }
    }

    fn get_broadcast_or_value(
        &self,
        key: Loader::Key,
        force_reload: bool,
    ) -> BroadcastOrValue<Loader> {
        // Keep the map entry alive until the per-key decision is complete.
        // A no-store completion can remove an entry after lookup; retry rather
        // than start an update on detached state in that case.
        loop {
            let mutex_by_key = self.get_mutex_by_key(&key);
            let map = self.inner.per_key.read();
            if map
                .get(&key)
                .is_some_and(|state| Arc::ptr_eq(state, &mutex_by_key))
            {
                return self.get_broadcast_or_value_locked(
                    key.clone(),
                    force_reload,
                    &mutex_by_key,
                );
            }
        }
    }

    fn get_broadcast_or_value_locked(
        &self,
        key: Loader::Key,
        force_reload: bool,
        mutex_by_key: &Mutex<InnerMut<Loader>>,
    ) -> BroadcastOrValue<Loader> {
        // Map-before-key lock ordering is shared with no-store completion.
        let mut guard = mutex_by_key.lock();

        // Check if the latest value can be used or not
        let use_latest = guard.latest.as_ref().map_or(UseLatest::NoValue, |latest| {
            latest.can_use(&self.inner.config)
        });
        let use_latest = if force_reload {
            UseLatest::NoValue
        } else {
            use_latest
        };

        let (value, stale_cache) = match use_latest {
            // Need to start a new loader task, _and_ need to wait for it
            UseLatest::NoValue => (None, None),
            // Need to start a new loader task, and can only provide a stale value if loading fails
            UseLatest::StaleValue(value) => (None, Some(value)),
            // Need a loader task, but use this value
            UseLatest::UseAndReload(value) => {
                // Future updates might fail, so it is essential that
                // we propagate the proper stale_cache value here
                let stale_cache = match &value {
                    ValueForRender::Success { value, loaded } => Some(Latest {
                        value: value.clone(),
                        loaded: *loaded,
                    }),
                    ValueForRender::Error {
                        message: _,
                        status: _,
                        stale_cache,
                    } => stale_cache.clone(),
                };
                (Some(value), stale_cache)
            }
            // Need a loader task, but afterwards just return the value
            UseLatest::UseNoReload(value) => return BroadcastOrValue::Value(value),
        };

        // Since we got this far, we need a loader task. Check if one is already
        // running. Note the interplay here with locking the mutex and then checking
        // values, versus the do_update method, and comments in that part of the code.
        match &guard.broadcast {
            // Already have a task running, either return the value or subscribe
            // for an update.
            Some(broadcast) => match value {
                Some(value) => BroadcastOrValue::Value(value),
                None => BroadcastOrValue::Broadcast {
                    broadcast: broadcast.subscribe(),
                    stale_cache,
                },
            },
            // No task running, so spawn it and track it inside our mutex.
            None => {
                // We're only ever going to broadcast a single message, so 1 is fine.
                let (sender, receiver) = broadcast::channel(1);
                guard.broadcast = Some(sender.clone());
                tokio::task::spawn(self.clone().do_update(sender, key, stale_cache.clone()));
                match value {
                    Some(value) => BroadcastOrValue::Value(value),
                    None => BroadcastOrValue::Broadcast {
                        broadcast: receiver,
                        stale_cache,
                    },
                }
            }
        }
    }

    /// Background update
    async fn do_update(
        self,
        sender: broadcast::Sender<ValueForRender<Loader>>,
        key: Loader::Key,
        stale_cache: Option<Latest<Loader>>,
    ) {
        // Run the loader and then render to JSON.
        // Isolate both invocation and polling panics in a task. The supervisor
        // still completes the update, releases waiters, and permits retries.
        let loader = self.clone();
        let load_key = key.clone();
        let res = match tokio::task::spawn(async move { loader.inner.loader.load(load_key).await })
            .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("Loader task panicked or was cancelled")),
        }
        .map(Arc::new);
        let config = self.get_cacher_config();

        if !config.store_cache {
            // Remove the in-flight entry atomically with respect to lookups.
            // Do not temporarily publish a cached value or clear the slot while
            // another caller can start a task that this completion would remove.
            let mut map = self.inner.per_key.write();
            if let Some(state) = map.remove(&key) {
                state.lock().broadcast = None;
            }
        } else {
            let mutex_by_key = self.get_mutex_by_key(&key);
            let mut guard = mutex_by_key.lock();
            guard.broadcast = None;
            if let Ok(value) = &res {
                guard.latest = Some(Latest {
                    loaded: Instant::now(),
                    value: value.clone(),
                });
            }
        }

        // Convert into a ValueForRender form.
        let value = match res {
            Ok(value) => ValueForRender::Success {
                value,
                loaded: Instant::now(),
            },
            Err(err) => {
                tracing::error!("AsyncLoader::do_update: {err:?}");
                let (message, status) = match err.downcast() {
                    Ok(ErrorResponse { status, message }) => (message, status),
                    Err(err) => Loader::display_error(err),
                };
                ValueForRender::Error {
                    message: message.into(),
                    status,
                    stale_cache,
                }
            }
        };

        // Update any tasks that are waiting for this broadcaster.
        // We ignore any errors from the send, since it's fine if there's no one
        // waiting for our response.
        let _ = sender.send(value);
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    use anyhow::anyhow;
    use tokio::task::JoinSet;

    use super::{AsyncLoader, AsyncLoaderBuilder, CacheLoader, StaleDelay, ValueForRender};

    struct MyData {
        num: u8,
        failures: u8,
    }

    pub struct TestLoader {
        num: Mutex<MyData>,
    }

    impl TestLoader {
        pub fn build() -> AsyncLoader<Self> {
            AsyncLoaderBuilder {
                reload_delay: Some(Duration::from_secs(2)),
                stale_delay: StaleDelay::Duration(Duration::from_secs(1)),
                store_cache: true,
                disable_loader: false,
            }
            .build(TestLoader {
                num: Mutex::new(MyData {
                    num: 0,
                    failures: 0,
                }),
            })
        }
        pub fn build_stale() -> AsyncLoader<Self> {
            AsyncLoaderBuilder {
                reload_delay: Some(Duration::from_secs(1)),
                stale_delay: StaleDelay::AllowStale,
                store_cache: true,
                disable_loader: false,
            }
            .build(TestLoader {
                num: Mutex::new(MyData {
                    num: 0,
                    failures: 0,
                }),
            })
        }
        pub fn build_without_cache() -> AsyncLoader<Self> {
            AsyncLoaderBuilder {
                reload_delay: Some(Duration::from_secs(1)),
                stale_delay: StaleDelay::AllowStale,
                store_cache: false,
                disable_loader: false,
            }
            .build(TestLoader {
                num: Mutex::new(MyData {
                    num: 0,
                    failures: 0,
                }),
            })
        }
        pub fn build_with_no_loader() -> AsyncLoader<Self> {
            AsyncLoaderBuilder {
                reload_delay: Some(Duration::from_secs(2)),
                stale_delay: StaleDelay::Duration(Duration::from_secs(1)),
                store_cache: true,
                disable_loader: true,
            }
            .build(TestLoader {
                num: Mutex::new(MyData {
                    num: 0,
                    failures: 0,
                }),
            })
        }
    }

    impl<Loader: CacheLoader> ValueForRender<Loader> {
        fn is_success(&self) -> bool {
            match self {
                ValueForRender::Success { .. } => true,
                ValueForRender::Error { .. } => false,
            }
        }

        fn get_success_value(&self) -> &Loader::Output {
            match self {
                ValueForRender::Success { value, loaded: _ } => value,
                ValueForRender::Error { .. } => panic!("Cannot get a value from error"),
            }
        }
    }

    impl CacheLoader for TestLoader {
        type Key = ();
        type Output = u8;

        async fn load(&self, (): ()) -> anyhow::Result<u8> {
            let mut guard = self.num.lock().unwrap();
            if guard.num == 0 || guard.failures >= 1 {
                guard.num += 1;
                Ok(guard.num)
            } else {
                guard.failures += 1;
                Err(anyhow!("Falied executing load"))
            }
        }
    }

    #[tokio::test]
    async fn no_store_completion_leaves_no_cached_value() {
        let cacher = gated_cache(false, false);
        let mut first = std::pin::pin!(cacher.get_value(()));
        let mut second = std::pin::pin!(cacher.get_value(()));
        assert!(poll_once(first.as_mut()).await.is_pending());
        assert!(poll_once(second.as_mut()).await.is_pending());
        cacher.inner.loader.started.notified().await;
        let state = cacher.get_mutex_by_key(&());
        assert_eq!(
            cacher
                .inner
                .loader
                .calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        cacher.inner.loader.release.notify_one();
        assert_eq!(*first.await.get_success_value(), 20);
        assert_eq!(*second.await.get_success_value(), 20);
        assert!(
            state.lock().latest.is_none(),
            "no-store must never publish a cached value"
        );
        assert_eq!(cacher.inner.metrics().length, 0);
        let mut retry = std::pin::pin!(cacher.get_value(()));
        assert!(poll_once(retry.as_mut()).await.is_pending());
        cacher.inner.loader.started.notified().await;
        cacher.inner.loader.release.notify_one();
        assert_eq!(*retry.await.get_success_value(), 21);
    }

    struct HeaderLoader;

    impl CacheLoader for HeaderLoader {
        type Key = String;
        type Output = String;

        async fn load(&self, key: String) -> anyhow::Result<String> {
            Ok(key)
        }

        fn into_response(output: &String) -> super::Response<super::Body> {
            let mut response = super::IntoResponse::into_response(super::Json(output));
            if !output.is_empty() {
                response
                    .headers_mut()
                    .insert(super::CACHE_CONTROL, output.parse().unwrap());
            }
            response
        }
    }

    #[test]
    fn response_cache_policy_is_owned_by_loader() {
        let config = TestLoader::build();
        for policy in ["no-store", "private, max-age=5", "public, max-age=60", ""] {
            let value = ValueForRender::<HeaderLoader>::Success {
                value: super::Arc::new(policy.to_owned()),
                loaded: super::Instant::now(),
            };
            let response = value.into_response(config.get_cacher_config());
            if policy.is_empty() {
                assert!(!response.headers().contains_key(super::CACHE_CONTROL));
            } else {
                assert_eq!(response.headers()[super::CACHE_CONTROL], policy);
            }
        }
    }

    struct GateLoader {
        calls: std::sync::atomic::AtomicUsize,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
        fail: bool,
    }

    impl CacheLoader for GateLoader {
        type Key = ();
        type Output = usize;

        async fn load(&self, (): ()) -> anyhow::Result<usize> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            if self.fail {
                Err(super::ErrorResponse {
                    status: super::StatusCode::BAD_GATEWAY,
                    message: "unavailable".into(),
                }
                .into())
            } else {
                Ok(call + 20)
            }
        }
    }

    fn gated_cache(store_cache: bool, fail: bool) -> AsyncLoader<GateLoader> {
        AsyncLoaderBuilder {
            reload_delay: Some(Duration::from_secs(10)),
            stale_delay: StaleDelay::Duration(Duration::from_secs(10)),
            store_cache,
            disable_loader: false,
        }
        .build(GateLoader {
            calls: std::sync::atomic::AtomicUsize::new(0),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            fail,
        })
    }

    fn seed_cache(cacher: &AsyncLoader<GateLoader>, age: u64) {
        cacher.get_mutex_by_key(&()).lock().latest = Some(super::Latest {
            loaded: super::Instant::now() - Duration::from_secs(age),
            value: super::Arc::new(10),
        });
    }

    async fn poll_once<F: std::future::Future>(
        future: std::pin::Pin<&mut F>,
    ) -> std::task::Poll<F::Output> {
        let mut future = future;
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await
    }

    #[tokio::test]
    async fn first_refresh_returns_cache_and_coalesces_forced_waiter() {
        let cacher = gated_cache(true, false);
        seed_cache(&cacher, 15);
        let mut first = std::pin::pin!(cacher.get_value(()));
        let std::task::Poll::Ready(value) = poll_once(first.as_mut()).await else {
            panic!("first refresh caller must receive cache without waiting");
        };
        assert_eq!(*value.get_success_value(), 10);
        cacher.inner.loader.started.notified().await;
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 10);
        let mut fresh = std::pin::pin!(cacher.get_latest_value(()));
        assert!(poll_once(fresh.as_mut()).await.is_pending());
        assert_eq!(
            cacher
                .inner
                .loader
                .calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        cacher.inner.loader.release.notify_one();
        assert_eq!(*fresh.await.get_success_value(), 20);
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 20);
    }

    struct PanicLoader {
        calls: std::sync::atomic::AtomicUsize,
        invocation: bool,
    }

    impl CacheLoader for PanicLoader {
        type Key = ();
        type Output = usize;

        fn load(&self, (): ()) -> impl std::future::Future<Output = anyhow::Result<usize>> + Send {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(!(call == 0 && self.invocation), "invocation panic");
            async move {
                assert!(call != 0, "poll panic");
                Ok(call)
            }
        }
    }

    async fn check_panic_retry(invocation: bool) {
        let cacher = AsyncLoaderBuilder {
            reload_delay: None,
            stale_delay: StaleDelay::Default,
            store_cache: true,
            disable_loader: false,
        }
        .build(PanicLoader {
            calls: std::sync::atomic::AtomicUsize::new(0),
            invocation,
        });
        let first = cacher.get_broadcast_or_value((), false);
        let second = cacher.get_broadcast_or_value((), false);
        for waiter in [first, second] {
            let super::BroadcastOrValue::Broadcast { mut broadcast, .. } = waiter else {
                panic!("expected waiting caller");
            };
            let result = tokio::time::timeout(Duration::from_secs(1), broadcast.recv())
                .await
                .expect("panic must release callers")
                .expect("error must be broadcast");
            assert!(matches!(
                result,
                ValueForRender::Error {
                    status: super::StatusCode::INTERNAL_SERVER_ERROR,
                    ..
                }
            ));
        }
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 1);
    }

    #[tokio::test]
    async fn invocation_panic_releases_waiters_and_retries() {
        check_panic_retry(true).await;
    }

    #[tokio::test]
    async fn polling_panic_releases_waiters_and_retries() {
        check_panic_retry(false).await;
    }

    #[tokio::test]
    async fn refresh_failure_preserves_usable_cache() {
        let cacher = gated_cache(true, true);
        seed_cache(&cacher, 15);
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 10);
        cacher.inner.loader.started.notified().await;
        let mut fresh = std::pin::pin!(cacher.get_latest_value(()));
        assert!(poll_once(fresh.as_mut()).await.is_pending());
        cacher.inner.loader.release.notify_one();
        assert!(matches!(
            fresh.await,
            ValueForRender::Error {
                status: super::StatusCode::BAD_GATEWAY,
                ..
            }
        ));
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 10);
        cacher.inner.loader.started.notified().await;
        assert_eq!(
            cacher
                .inner
                .loader
                .calls
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
        let mut retry = std::pin::pin!(cacher.get_latest_value(()));
        assert!(poll_once(retry.as_mut()).await.is_pending());
        cacher.inner.loader.release.notify_one();
        assert!(matches!(
            retry.await,
            ValueForRender::Error {
                status: super::StatusCode::BAD_GATEWAY,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn expired_cache_waits_and_failure_can_fall_back() {
        let cacher = gated_cache(true, true);
        seed_cache(&cacher, 25);
        let mut strict = std::pin::pin!(cacher.get_value(()));
        let mut fallback = std::pin::pin!(cacher.get_value_allow_stale(()));
        assert!(poll_once(strict.as_mut()).await.is_pending());
        assert!(poll_once(fallback.as_mut()).await.is_pending());
        cacher.inner.loader.started.notified().await;
        cacher.inner.loader.release.notify_one();
        assert!(matches!(
            strict.await,
            ValueForRender::Error {
                status: super::StatusCode::BAD_GATEWAY,
                stale_cache: Some(_),
                ..
            }
        ));
        assert_eq!(*fallback.await.get_success_value(), 10);
        assert_eq!(
            cacher
                .inner
                .loader
                .calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn allow_stale_never_expires_but_refreshes() {
        let cacher = TestLoader::build_stale();
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 1);
        cacher
            .get_mutex_by_key(&())
            .lock()
            .latest
            .as_mut()
            .unwrap()
            .loaded = super::Instant::now() - Duration::from_secs(100);
        let mut cached = std::pin::pin!(cacher.get_value(()));
        let std::task::Poll::Ready(value) = poll_once(cached.as_mut()).await else {
            panic!("AllowStale cache must be immediately usable");
        };
        assert_eq!(*value.get_success_value(), 1);
        assert!(!cacher.get_latest_value(()).await.is_success());
        assert_eq!(*cacher.get_latest_value(()).await.get_success_value(), 2);
    }

    #[tokio::test]
    async fn test_force_reload() {
        let cacher = TestLoader::build();
        let item = cacher.get_value(()).await;
        assert_eq!(item.get_success_value(), &1);

        let item = cacher.get_latest_value(()).await;
        assert!(!item.is_success());

        let item = cacher.get_latest_value(()).await;
        assert_eq!(*item.get_success_value(), 2);

        let item = cacher.get_latest_value(()).await;
        assert_eq!(*item.get_success_value(), 3);
    }

    #[tokio::test]
    async fn test_without_store() {
        let cacher = TestLoader::build_without_cache();
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 1);
        assert!(!cacher.get_value(()).await.is_success());
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 2);
        assert_eq!(*cacher.get_value(()).await.get_success_value(), 3);
        assert_eq!(cacher.inner.metrics().length, 0);
    }

    #[tokio::test]
    async fn test_with_store_disable_loader() {
        async fn test_task(cacher: AsyncLoader<TestLoader>) -> String {
            let result = cacher.get_value(()).await;
            match result {
                ValueForRender::Success { value, .. } => value.to_string(),
                ValueForRender::Error { .. } => "error".to_owned(),
            }
        }

        let cacher = TestLoader::build_with_no_loader();

        let item = cacher.get_value(()).await;
        assert_eq!(item.get_success_value(), &1);

        let item = cacher.get_value(()).await;
        assert!(!item.is_success());

        let mut set = JoinSet::new();
        for _ in 0..5 {
            let cacher = cacher.clone();
            set.spawn(test_task(cacher));
        }

        let mut index = 0;
        let possible_result = vec![
            "2".to_owned(),
            "3".to_owned(),
            "4".to_owned(),
            "5".to_owned(),
            "6".to_owned(),
        ];
        let mut actual_result = vec![];
        while let Some(res) = set.join_next().await {
            actual_result.push(res.unwrap());
            index += 1;
        }
        actual_result.sort();
        assert_eq!(actual_result, possible_result, "All values were uncached");

        assert_eq!(index, 5, "5 tokio tasks executed");
    }
}
