use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use zookeeper_async::{WatchedEvent, ZkState, ZooKeeper};

/// Protocol version ClickHouse Keeper sends when it refuses a handshake.
const CONNECTION_REJECT: i32 = 42;
const SESSION_ID: i64 = 0x1234;

#[derive(Debug, Clone, Copy, PartialEq)]
struct Handshake {
    protocol_version: i32,
    session_id: i64,
}

async fn read_handshake(sock: &mut TcpStream) -> Option<Handshake> {
    let len = sock.read_i32().await.ok()?;
    let mut body = vec![0; len as usize];
    sock.read_exact(&mut body).await.ok()?;
    Some(Handshake {
        protocol_version: i32::from_be_bytes(body[0..4].try_into().unwrap()),
        session_id: i64::from_be_bytes(body[16..24].try_into().unwrap()),
    })
}

async fn write_connect_response(sock: &mut TcpStream, protocol_version: i32, session_id: i64) {
    let mut body = Vec::new();
    body.extend_from_slice(&protocol_version.to_be_bytes());
    body.extend_from_slice(&15000i32.to_be_bytes());
    body.extend_from_slice(&session_id.to_be_bytes());
    body.extend_from_slice(&16i32.to_be_bytes());
    body.extend_from_slice(&[0; 16]);
    body.push(0);
    sock.write_i32(body.len() as i32).await.unwrap();
    sock.write_all(&body).await.unwrap();
}

/// Answers the first handshake the way ClickHouse Keeper does without a quorum: protocol version
/// 42, session id -1 or 0 depending on the Keeper version, then close. Closes any later handshake
/// with a protocol version other than 0, which Keeper can't parse, and serves a session to a
/// handshake that asks for a new session or an existing one.
async fn rejecting_server(
    listener: TcpListener,
    reject_session_id: i64,
    handshakes: Arc<Mutex<Vec<Handshake>>>,
    served: mpsc::Sender<()>,
) {
    let mut first = true;
    loop {
        let (mut sock, _) = listener.accept().await.unwrap();
        let handshake = match read_handshake(&mut sock).await {
            Some(handshake) => handshake,
            None => continue,
        };
        handshakes.lock().unwrap().push(handshake);
        if first {
            first = false;
            write_connect_response(&mut sock, CONNECTION_REJECT, reject_session_id).await;
        } else if handshake.protocol_version == 0 && handshake.session_id >= 0 {
            write_connect_response(&mut sock, 0, SESSION_ID).await;
            let _ = served.send(()).await;
            tokio::spawn(async move {
                let mut buf = [0; 1024];
                while matches!(sock.read(&mut buf).await, Ok(n) if n > 0) {}
            });
        }
    }
}

async fn reconnects_after_connection_reject(reject_session_id: i64) {
    let _ = tracing_subscriber::fmt::try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handshakes = Arc::new(Mutex::new(Vec::new()));
    let (served_tx, mut served_rx) = mpsc::channel(1);
    tokio::spawn(rejecting_server(
        listener,
        reject_session_id,
        handshakes.clone(),
        served_tx,
    ));

    let zk = ZooKeeper::connect(
        &addr.to_string(),
        Duration::from_secs(15),
        |_: WatchedEvent| {},
    )
    .await
    .unwrap();
    let state = Arc::new(Mutex::new(ZkState::Connecting));
    let state_listener = state.clone();
    zk.add_listener(move |s| *state_listener.lock().unwrap() = s);

    let served = tokio::time::timeout(Duration::from_secs(5), served_rx.recv()).await;
    let handshakes = handshakes.lock().unwrap().clone();
    assert!(
        served.is_ok(),
        "no valid handshake after the reject; got {} handshakes, last {:?}",
        handshakes.len(),
        handshakes.last()
    );
    for handshake in &handshakes[1..] {
        assert_eq!(handshake.protocol_version, 0, "{:?}", handshake);
        assert!(handshake.session_id >= 0, "{:?}", handshake);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(*state.lock().unwrap(), ZkState::Connected);
}

#[tokio::test]
async fn reconnects_after_connection_reject_with_session_id_minus_one() {
    reconnects_after_connection_reject(-1).await;
}

#[tokio::test]
async fn reconnects_after_connection_reject_with_session_id_zero() {
    reconnects_after_connection_reject(0).await;
}
