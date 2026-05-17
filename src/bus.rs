use crate::config::BusConfig;
use crate::error::Result;
use crate::publisher::Publisher;
use crate::stats::TopicStats;
use crate::subscriber::Subscriber;
use crate::subscription::Subscription;
use std::collections::HashMap;
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
