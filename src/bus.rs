use crate::config::BusConfig;
use crate::error::{Error, Result};
use crate::producer_lock::acquire_producer_lock;
use crate::publisher::Publisher;
use crate::stats::TopicStats;
use crate::subscriber::{StartPos, Subscriber};
use crate::subscription::Subscription;
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

/// Named pub-sub namespace. Topics are independent channels within the
/// namespace; each topic gets its own ring-buffer file on disk.
///
/// # Example
///
/// ```rust,no_run
/// use mmbus::Bus;
///
/// // Publisher process
/// let mut bus = Bus::new("my-app");
/// bus.publish("sensors", b"hello").unwrap();
///
/// // Subscriber process
/// let bus = Bus::new("my-app");
/// for msg in bus.subscribe("sensors").unwrap() {
///     println!("{:?}", msg.unwrap());
/// }
/// ```
pub struct Bus {
    name: String,
    config: BusConfig,
    publishers: HashMap<String, Publisher>,
}

impl Bus {
    /// Create or connect to a named bus with default config.
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_config(name, BusConfig::default())
    }

    /// Create or connect to a named bus with custom config.
    pub fn with_config(name: impl Into<String>, config: BusConfig) -> Self {
        Self { name: name.into(), config, publishers: HashMap::new() }
    }

    /// Publish `data` to `topic`. Publisher is created on the first call and
    /// cached. Returns `Err(Error::Full)` when the ring is saturated.
    pub fn publish(&mut self, topic: &str, data: &[u8]) -> Result<()> {
        self.ensure_publisher(topic)?;
        self.publishers.get_mut(topic).unwrap().publish(data)
    }

    /// Subscribe to `topic`, waiting up to 30 seconds for the publisher.
    pub fn subscribe(&self, topic: &str) -> Result<Subscription> {
        self.subscribe_timeout(topic, Duration::from_secs(30))
    }

    /// Subscribe to `topic` with a custom connection timeout.
    pub fn subscribe_timeout(&self, topic: &str, timeout: Duration) -> Result<Subscription> {
        let sub = Subscriber::connect(topic, &self.topic_config(topic), timeout)?;
        Ok(Subscription::new(sub))
    }

    /// Subscribe starting `n_messages_back` behind the current tail.  Replays
    /// recent in-ring history; capped at the ring capacity (older messages
    /// have been overwritten).  Uses a 30-second connection timeout.
    ///
    /// Note: this is a *best-effort* replay.  If `n_messages_back` exceeds
    /// what's still in the ring, the seqlock in [`Subscription::recv`] will
    /// skip forward to the oldest available slot on the first read — you
    /// won't observe an error, just fewer messages than requested.
    pub fn subscribe_with_history(
        &self,
        topic: &str,
        n_messages_back: u64,
    ) -> Result<Subscription> {
        self.subscribe_with_history_timeout(
            topic,
            n_messages_back,
            Duration::from_secs(30),
        )
    }

    /// `subscribe_with_history` with a custom connection timeout.
    pub fn subscribe_with_history_timeout(
        &self,
        topic: &str,
        n_messages_back: u64,
        timeout: Duration,
    ) -> Result<Subscription> {
        let sub = Subscriber::connect_with(
            topic,
            &self.topic_config(topic),
            timeout,
            StartPos::HistoryBack(n_messages_back),
        )?;
        Ok(Subscription::new(sub))
    }

    /// Subscribe starting at an explicit cursor value.  Returns
    /// [`Error::CursorTooOld`] if `cursor` is older than the oldest slot
    /// still in the ring.  Uses a 30-second connection timeout.
    ///
    /// Cursor stability is per-publisher-generation: a cursor obtained
    /// before a publisher restart is invalid after restart.
    pub fn subscribe_from(&self, topic: &str, cursor: u64) -> Result<Subscription> {
        self.subscribe_from_timeout(topic, cursor, Duration::from_secs(30))
    }

    /// `subscribe_from` with a custom connection timeout.
    pub fn subscribe_from_timeout(
        &self,
        topic: &str,
        cursor: u64,
        timeout: Duration,
    ) -> Result<Subscription> {
        let sub = Subscriber::connect_with(
            topic,
            &self.topic_config(topic),
            timeout,
            StartPos::Explicit(cursor),
        )?;
        Ok(Subscription::new(sub))
    }

    /// Ensure the publisher for `topic` exists and block until at least `n`
    /// subscribers have connected, or until `timeout` expires.
    pub fn wait_for_subscribers(
        &mut self,
        topic: &str,
        n: usize,
        timeout: Duration,
    ) -> Result<()> {
        self.ensure_publisher(topic)?;
        self.publishers.get_mut(topic).unwrap().wait_for_subscribers(n, timeout)
    }

    /// Snapshot of ring and socket stats for `topic`.
    /// Returns `None` if no publisher has been created for `topic` in this Bus.
    pub fn stats(&self, topic: &str) -> Option<TopicStats> {
        self.publishers.get(topic).map(|p| p.stats())
    }

    /// `(cursor_idx, lag)` pairs for subscribers on `topic` whose lag is
    /// `>= threshold` messages.  Returns an empty Vec if no publisher
    /// exists for `topic` in this `Bus`, or if every subscriber is
    /// caught up to within `threshold`.
    ///
    /// Intended for periodic monitoring — call from a background thread
    /// and emit metrics / warnings when the returned Vec is non-empty.
    /// `cursor_idx` is stable across calls for the same subscriber, so
    /// it can be used to track which one is consistently slow.
    pub fn slow_subscribers(
        &self,
        topic: &str,
        threshold: u64,
    ) -> Vec<(usize, u64)> {
        self.publishers
            .get(topic)
            .map(|p| p.slow_subscribers(threshold))
            .unwrap_or_default()
    }

    /// Remove all on-disk state for `topic` (ring file, signal socket,
    /// producer lock).  Refuses with `Error::AlreadyPublishing` if any
    /// process — including this one — is currently publishing.
    ///
    /// Intended for test setup / dev tooling.  Existing subscribers keep
    /// working from their already-mmap'd pages until they drop; new
    /// subscribers will see "no such topic" until something publishes
    /// again.  Never call this against a topic in active production use.
    pub fn clean_topic(&mut self, topic: &str) -> Result<()> {
        // Drop our own cached publisher first so the in-process lock is
        // released — otherwise `acquire_producer_lock` below would refuse.
        self.publishers.remove(topic);

        let dir = self.config.base_dir.join(&self.name).join(topic);
        if !dir.exists() {
            return Ok(());
        }
        // Acquire the producer lock as a "no-one is publishing" gate.  We
        // hold it across `remove_dir_all`; the lock file gets unlinked too,
        // but our fd keeps `flock` semantics until `_guard` drops.
        let _guard = acquire_producer_lock(topic, &dir)?;
        fs::remove_dir_all(&dir).map_err(Error::Io)?;
        Ok(())
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    fn ensure_publisher(&mut self, topic: &str) -> Result<()> {
        if !self.publishers.contains_key(topic) {
            let pub_ = Publisher::create(topic, self.topic_config(topic))?;
            self.publishers.insert(topic.to_owned(), pub_);
        }
        Ok(())
    }

    fn topic_config(&self, _topic: &str) -> BusConfig {
        BusConfig { base_dir: self.config.base_dir.join(&self.name), ..self.config.clone() }
    }
}
