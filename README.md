# tyo-mq-client-rust

A Rust client for **[tyo-mq](https://github.com/tyolab/tyo-mq)** — the
distributed pub/sub messaging service with durable delivery (ACK / retry /
dead-letter queue), MQTT-style topic wildcards, consumer groups, and
multi-tenant auth realms.

Built on [rust_socketio](https://crates.io/crates/rust_socketio)
(Socket.IO v4), synchronous API.

## Install

```toml
[dependencies]
tyo-mq-client = { git = "https://github.com/tyolab/tyo-mq-client-rust" }
```

You'll need a running tyo-mq server — see the
[server repo](https://github.com/tyolab/tyo-mq).

## Quick start

```rust
use tyo_mq_client::{Client, Options, SubscribeRequest};
use serde_json::json;

// consume (durable + auto-ACK)
let consumer = Client::connect(Options::new().host("localhost").port(17352))?;
// with auth enabled on the server: Options::new().auth_token("my-token")
consumer.register_consumer("email-service")?;
consumer.subscribe(
    SubscribeRequest::new("order-service", "order-placed", "email-service")
        .durable()
        .ack()
        .retry(3, "5s", "exponential"),
    |msg, _ack| println!("order event: {} (msgId {:?})", msg.message, msg.msg_id),
)?;

// produce
let producer = Client::connect(Options::new())?;
producer.register_producer("order-service")?;
producer.produce("order-service", "order-placed", json!({"orderId": 1001}))?;
```

With `.manual_ack()` (plus `.ack_timeout("30s")`) the handler receives an
`ack` callable and acknowledges only when the work truly succeeded;
unacknowledged deliveries are retried on the schedule and dead-lettered when
attempts run out.

## Topics, groups, broadcast

```rust
// MQTT-style wildcards: + is one level, # is the rest
consumer.subscribe(
    SubscribeRequest::topic("orders/+/status", "dashboard"),
    |msg, _| println!("{}: {}", msg.event, msg.message),
)?;

// consumer groups load-balance across workers
consumer.subscribe(
    SubscribeRequest::new("dispatcher", "jobs", "worker-1").group("workers"),
    handle_job,
)?;

// broadcast one copy to every realm member, or every group member
producer.broadcast("control", "announcement", json!({"notice": "maintenance"}), "realm", None)?;
producer.broadcast("control", "reload", json!({}), "group", Some("workers"))?;
```

## Example

```bash
cargo run --example pubsub -- localhost 17352
```

`examples/pubsub.rs` is a complete round trip (durable + auto-ACK), verified
against a live tyo-mq server.

## Other clients

Node.js (and browsers) ships with the [server package](https://github.com/tyolab/tyo-mq);
see also [Python](https://github.com/tyolab/tyo-mq-client-python),
[Go](https://github.com/tyolab/tyo-mq-client-go),
[C/C++](https://github.com/tyolab/tyo-mq-client-cpp),
[Ruby](https://github.com/tyolab/tyo-mq-client-ruby),
[Java](https://github.com/tyolab/tyo-mq-client-java), and
[C#](https://github.com/tyolab/tyo-mq-client-csharp).

All clients are exercised together by the cross-language
[conformance suite](https://github.com/tyolab/tyo-mq-conformance), which runs
the same pub/sub, durable-delivery, topic, group, and auth scenarios against
every client (and every producer/consumer language pair) and publishes the
resulting matrix.

## License

Apache-2.0. Built by [TYO Lab](https://tyo.com.au).
