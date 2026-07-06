//! A minimal tyo-mq round trip: produce on one connection, consume (durable,
//! auto-ACK) on another.
//!
//! Start a server first (see https://github.com/tyolab/tyo-mq), then:
//!
//!     cargo run --example pubsub -- 127.0.0.1 17352

use serde_json::json;
use std::sync::mpsc;
use std::time::Duration;
use tyo_mq_client::{Client, Error, Options, SubscribeRequest};

fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "localhost".into());
    let port: u16 = args.next().and_then(|p| p.parse().ok()).unwrap_or(17352);

    let options = Options::new().host(&host).port(port);
    // with auth enabled on the server: .auth_token("my-token")

    let consumer = Client::connect(options.clone())?;
    consumer.register_consumer("rust-listener")?;

    let (tx, rx) = mpsc::channel();
    consumer.subscribe(
        SubscribeRequest::new("rust-example", "order-placed", "rust-listener")
            .durable()
            .ack() // auto-ACKed after the handler returns
            .retry(3, "5s", "exponential"),
        move |msg, _ack| {
            println!(
                "received from {}: {} (msgId: {})",
                msg.from,
                msg.message,
                msg.msg_id.as_deref().unwrap_or("-")
            );
            let _ = tx.send(());
        },
    )?;

    let producer = Client::connect(options)?;
    producer.register_producer("rust-example")?;

    std::thread::sleep(Duration::from_millis(400)); // let the subscription register
    producer.produce("rust-example", "order-placed", json!({"orderId": 1001, "total": 129.0}))?;

    let ok = rx.recv_timeout(Duration::from_secs(10)).is_ok();
    let _ = producer.disconnect();
    let _ = consumer.disconnect();

    println!("{}", if ok { "round trip OK" } else { "no message received before timeout" });
    std::process::exit(if ok { 0 } else { 1 });
}
