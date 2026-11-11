use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use zookeeper_async::{WatchedEvent, ZooKeeper};

#[tokio::test]
async fn backs_off_when_server_closes_every_connection() {
    let _ = tracing_subscriber::fmt::try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let server_connections = connections.clone();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            server_connections.fetch_add(1, Ordering::Relaxed);
            drop(sock);
        }
    });

    let _zk = ZooKeeper::connect(
        &addr.to_string(),
        Duration::from_secs(15),
        |_: WatchedEvent| {},
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;
    let count = connections.load(Ordering::Relaxed);
    assert!(
        (2..=10).contains(&count),
        "expected 2 to 10 connections in 3 s, got {}",
        count
    );
}
