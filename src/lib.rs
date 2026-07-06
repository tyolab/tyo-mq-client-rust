//! A Rust client for [tyo-mq](https://github.com/tyolab/tyo-mq) — the
//! distributed pub/sub messaging service with durable delivery (ACK / retry /
//! dead-letter queue), MQTT-style topic wildcards, consumer groups, and
//! multi-tenant auth realms.
//!
//! ```no_run
//! use tyo_mq_client::{Client, Options, SubscribeRequest};
//! use serde_json::json;
//!
//! let consumer = Client::connect(Options::new().port(17352))?;
//! consumer.register_consumer("email-service")?;
//! consumer.subscribe(
//!     SubscribeRequest::new("order-service", "order-placed", "email-service")
//!         .durable()
//!         .ack(),
//!     |msg, _ack| println!("order event: {}", msg.message),
//! )?;
//!
//! let producer = Client::connect(Options::new().port(17352))?;
//! producer.register_producer("order-service")?;
//! producer.produce("order-service", "order-placed", json!({"orderId": 1001}))?;
//! # Ok::<(), tyo_mq_client::Error>(())
//! ```

use rust_socketio::{ClientBuilder, Payload, RawClient};
use serde_json::{json, Value};

use std::collections::HashMap;
use std::sync::atomic::{AtomicI8, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

/// The default tyo-mq server port.
pub const DEFAULT_PORT: u16 = 17352;

/// Subscribe to an event (or topic pattern) from any producer.
pub const ALL_PRODUCERS: &str = "TYO-MQ-ALL";

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// One delivered message.
#[derive(Debug, Clone)]
pub struct ConsumedMessage {
    pub event: String,
    pub message: Value,
    pub from: String,
    /// Present only on ACK-enabled deliveries.
    pub msg_id: Option<String>,
}

/// The delivery-acknowledgement callback handed to subscription handlers.
/// A no-op when the delivery carries no msgId. With auto-ACK (`.ack()`
/// without `.manual_ack()`) it is invoked for you after the handler returns.
pub type Ack<'a> = &'a dyn Fn();

type SubscriptionHandler = Box<dyn Fn(ConsumedMessage, Ack) + Send + Sync>;

struct Subscription {
    handler: SubscriptionHandler,
    auto_ack: bool,
}

/// Connection options.
#[derive(Debug, Clone)]
pub struct Options {
    pub host: String,
    pub port: u16,
    pub protocol: String,
    /// Sent as AUTHENTICATION right after connecting — required when the
    /// server runs with auth enabled.
    pub auth_token: Option<String>,
}

impl Options {
    pub fn new() -> Self {
        Options {
            host: "localhost".into(),
            port: DEFAULT_PORT,
            protocol: "http".into(),
            auth_token: None,
        }
    }

    pub fn host(mut self, host: &str) -> Self { self.host = host.into(); self }
    pub fn port(mut self, port: u16) -> Self { self.port = port; self }
    pub fn protocol(mut self, protocol: &str) -> Self { self.protocol = protocol.into(); self }
    pub fn auth_token(mut self, token: &str) -> Self { self.auth_token = Some(token.into()); self }

    fn url(&self) -> String {
        format!("{}://{}:{}", self.protocol, self.host, self.port)
    }
}

impl Default for Options {
    fn default() -> Self { Self::new() }
}

/// Retry schedule for ACK-enabled durable subscriptions.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    /// Duration string, e.g. "5s" or "200ms".
    pub delay: String,
    /// "" or "exponential".
    pub backoff: String,
}

/// A subscription request. The default is plain fire-and-forget delivery;
/// the builder methods opt in to guaranteed delivery and routing features.
#[derive(Debug, Clone)]
pub struct SubscribeRequest {
    pub producer: String,
    pub event: String,
    pub consumer: String,
    pub durable: bool,
    pub ack: bool,
    pub manual_ack: bool,
    pub ack_timeout: Option<String>,
    pub retry: Option<RetryPolicy>,
    pub mode: Option<String>,
    pub group: Option<String>,
}

impl SubscribeRequest {
    pub fn new(producer: &str, event: &str, consumer: &str) -> Self {
        SubscribeRequest {
            producer: producer.into(),
            event: event.into(),
            consumer: consumer.into(),
            durable: false,
            ack: false,
            manual_ack: false,
            ack_timeout: None,
            retry: None,
            mode: None,
            group: None,
        }
    }

    /// An MQTT-style topic pattern (`+` one level, `#` the rest) matched
    /// against events from any producer.
    pub fn topic(pattern: &str, consumer: &str) -> Self {
        let mut req = Self::new(ALL_PRODUCERS, pattern, consumer);
        req.mode = Some("topic".into());
        req
    }

    pub fn durable(mut self) -> Self { self.durable = true; self }
    /// Auto-ACK after the handler returns.
    pub fn ack(mut self) -> Self { self.ack = true; self }
    /// The handler must call its ack argument itself.
    pub fn manual_ack(mut self) -> Self { self.ack = true; self.manual_ack = true; self }
    pub fn ack_timeout(mut self, timeout: &str) -> Self { self.ack_timeout = Some(timeout.into()); self }
    pub fn retry(mut self, max_attempts: u32, delay: &str, backoff: &str) -> Self {
        self.retry = Some(RetryPolicy { max_attempts, delay: delay.into(), backoff: backoff.into() });
        self
    }
    pub fn group(mut self, group: &str) -> Self { self.group = Some(group.into()); self }
}

/// The Socket.IO event on which deliveries for a subscription arrive.
pub fn consume_event_name(producer: &str, event: &str, scope: &str) -> String {
    if scope == "all" {
        format!("CONSUME-{}-TM-ALL", producer.to_lowercase())
    } else {
        format!("CONSUME-{}", format!("{}-{}", producer, event).to_lowercase())
    }
}

type Registry = RwLock<HashMap<String, Arc<Subscription>>>;

/// A tyo-mq connection. One `Client` can act as a producer, a consumer, or
/// both; create one per logical service identity.
pub struct Client {
    socket: rust_socketio::client::Client,
    registry: Arc<Registry>,
}

impl Client {
    /// Connects (and authenticates, when `auth_token` is set).
    pub fn connect(options: Options) -> Result<Self, Error> {
        let registry: Arc<Registry> = Arc::new(RwLock::new(HashMap::new()));
        let dispatch_registry = Arc::clone(&registry);

        // AUTH handshake state: 0 pending, 1 ok, -1 failed.
        let auth_state = Arc::new((Mutex::new(()), Condvar::new(), AtomicI8::new(0)));
        let auth_dispatch = Arc::clone(&auth_state);

        // Connection gate: connect() returns before the transport is open,
        // so wait for the Connect event before the first emit.
        let open_state = Arc::new((Mutex::new(()), Condvar::new(), AtomicI8::new(0)));
        let open_signal = Arc::clone(&open_state);

        let socket = ClientBuilder::new(options.url())
            .reconnect(true)
            .on(rust_socketio::Event::Connect, move |_, _| {
                open_signal.2.store(1, Ordering::SeqCst);
                open_signal.1.notify_all();
            })
            .on_any(move |event, payload, raw| {
                let name = String::from(event);
                match name.as_str() {
                    "AUTH_OK" => {
                        auth_dispatch.2.store(1, Ordering::SeqCst);
                        auth_dispatch.1.notify_all();
                    }
                    "AUTH_FAIL" => {
                        auth_dispatch.2.store(-1, Ordering::SeqCst);
                        auth_dispatch.1.notify_all();
                    }
                    _ => dispatch(&dispatch_registry, &name, payload, &raw),
                }
            })
            .connect()?;

        {
            let (lock, cvar, state) = &*open_state;
            let guard = lock.lock().unwrap();
            let (_guard, timeout) = cvar
                .wait_timeout_while(guard, Duration::from_secs(10), |_| {
                    state.load(Ordering::SeqCst) == 0
                })
                .unwrap();
            if timeout.timed_out() {
                return Err("timed out waiting for the connection to open".into());
            }
        }

        if let Some(token) = &options.auth_token {
            socket.emit("AUTHENTICATION", json!({ "token": token }))?;
            let (lock, cvar, state) = &*auth_state;
            let guard = lock.lock().unwrap();
            let (_guard, timeout) = cvar
                .wait_timeout_while(guard, Duration::from_secs(5), |_| {
                    state.load(Ordering::SeqCst) == 0
                })
                .unwrap();
            if timeout.timed_out() {
                return Err("authentication timed out".into());
            }
            if state.load(Ordering::SeqCst) != 1 {
                return Err("authentication failed".into());
            }
        }

        Ok(Client { socket, registry })
    }

    /// Announces this connection as a producer.
    pub fn register_producer(&self, name: &str) -> Result<(), Error> {
        self.socket.emit("PRODUCER", json!({ "name": name }))?;
        Ok(())
    }

    /// Announces this connection as a consumer. The name doubles as the
    /// durable consumer identity: reconnect with the same name to replay
    /// queued messages of a durable subscription.
    pub fn register_consumer(&self, name: &str) -> Result<(), Error> {
        self.socket
            .emit("CONSUMER", json!({ "name": name, "id": name, "consumer_id": name }))?;
        Ok(())
    }

    /// Publishes one fire-and-forget message.
    pub fn produce(&self, from: &str, event: &str, message: Value) -> Result<(), Error> {
        self.socket.emit(
            "PRODUCE",
            json!({ "event": event, "message": message, "from": from }),
        )?;
        Ok(())
    }

    /// Broadcasts one copy to every realm member (`kind` = "realm") or every
    /// member of a consumer group (`kind` = "group").
    pub fn broadcast(
        &self,
        from: &str,
        event: &str,
        message: Value,
        kind: &str,
        group: Option<&str>,
    ) -> Result<(), Error> {
        let mut payload = json!({
            "event": event, "message": message, "from": from,
            "method": "broadcast",
            "broadcast": if kind == "group" { "group" } else { "realm" },
        });
        if let Some(g) = group {
            payload["group"] = json!(g);
        }
        self.socket.emit("PRODUCE", payload)?;
        Ok(())
    }

    /// Acknowledges one ACK-enabled delivery.
    pub fn ack(&self, msg_id: &str) -> Result<(), Error> {
        self.socket.emit("ACK", json!({ "msgId": msg_id }))?;
        Ok(())
    }

    /// Sends a SUBSCRIBE request and dispatches matching deliveries to
    /// `handler`. With `req.ack` and not `req.manual_ack`, deliveries are
    /// acknowledged automatically after the handler returns.
    pub fn subscribe<F>(&self, req: SubscribeRequest, handler: F) -> Result<(), Error>
    where
        F: Fn(ConsumedMessage, Ack) + Send + Sync + 'static,
    {
        let mut payload = json!({
            "event": req.event,
            "producer": req.producer,
            "consumer": req.consumer,
            "scope": "default",
            "consumer_id": req.consumer,
        });
        if req.durable { payload["durable"] = json!(true); }
        if req.ack { payload["ack"] = json!(true); }
        if req.manual_ack { payload["manual_ack"] = json!(true); }
        if let Some(t) = &req.ack_timeout { payload["ack_timeout"] = json!(t); }
        if let Some(r) = &req.retry {
            payload["retry"] = json!({
                "max_attempts": r.max_attempts,
                "delay": r.delay,
                "backoff": r.backoff,
            });
        }
        if let Some(m) = &req.mode { payload["mode"] = json!(m); }
        if let Some(g) = &req.group { payload["group"] = json!(g); }

        let key = consume_event_name(&req.producer, &req.event, "");
        self.registry.write().unwrap().insert(
            key,
            Arc::new(Subscription {
                handler: Box::new(handler),
                auto_ack: req.ack && !req.manual_ack,
            }),
        );

        self.socket.emit("SUBSCRIBE", payload)?;
        Ok(())
    }

    /// Disconnects the client.
    pub fn disconnect(&self) -> Result<(), Error> {
        self.socket.disconnect()?;
        Ok(())
    }
}

fn dispatch(registry: &Registry, name: &str, payload: Payload, raw: &RawClient) {
    let subscription = match registry.read().unwrap().get(name) {
        Some(s) => Arc::clone(s),
        None => return,
    };

    let value = match payload {
        Payload::Text(values) => values.into_iter().next().unwrap_or(Value::Null),
        #[allow(deprecated)]
        Payload::String(s) => serde_json::from_str(&s).unwrap_or(Value::Null),
        _ => return,
    };
    let obj = match value.as_object() {
        Some(o) => o,
        None => return,
    };

    let msg = ConsumedMessage {
        event: obj.get("event").and_then(Value::as_str).unwrap_or("").to_string(),
        message: obj.get("message").cloned().unwrap_or(Value::Null),
        from: obj.get("from").and_then(Value::as_str).unwrap_or("").to_string(),
        msg_id: obj
            .get("msgId")
            .or_else(|| obj.get("msg_id"))
            .and_then(Value::as_str)
            .map(String::from),
    };

    let acked = std::sync::atomic::AtomicBool::new(false);
    let ack_fn = || {
        if let Some(id) = &msg.msg_id {
            if !acked.swap(true, Ordering::SeqCst) {
                let _ = raw.emit("ACK", json!({ "msgId": id }));
            }
        }
    };

    (subscription.handler)(msg.clone(), &ack_fn);
    if subscription.auto_ack {
        ack_fn();
    }
}
