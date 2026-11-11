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
const PASSWD: [u8; 16] = [0xab; 16];

#[derive(Debug, Clone, PartialEq)]
struct Handshake {
    protocol_version: i32,
    session_id: i64,
    passwd: Vec<u8>,
}

async fn read_handshake(sock: &mut TcpStream) -> Option<Handshake> {
    let len = sock.read_i32().await.ok()?;
    let mut body = vec![0; len as usize];
    sock.read_exact(&mut body).await.ok()?;
    let passwd_len = i32::from_be_bytes(body[24..28].try_into().unwrap()) as usize;
    Some(Handshake {
        protocol_version: i32::from_be_bytes(body[0..4].try_into().unwrap()),
        session_id: i64::from_be_bytes(body[16..24].try_into().unwrap()),
        passwd: body[28..28 + passwd_len].to_vec(),
    })
}

async fn write_connect_response(
    sock: &mut TcpStream,
    protocol_version: i32,
    session_id: i64,
    passwd: &[u8],
) {
    let mut body = Vec::new();
    body.extend_from_slice(&protocol_version.to_be_bytes());
    body.extend_from_slice(&15000i32.to_be_bytes());
    body.extend_from_slice(&session_id.to_be_bytes());
    body.extend_from_slice(&(passwd.len() as i32).to_be_bytes());
    body.extend_from_slice(passwd);
    body.push(0);
    sock.write_i32(body.len() as i32).await.unwrap();
    sock.write_all(&body).await.unwrap();
}

fn serve(mut sock: TcpStream) {
    tokio::spawn(async move {
        let mut buf = [0; 1024];
        while matches!(sock.read(&mut buf).await, Ok(n) if n > 0) {}
    });
}

/// Answers the first handshake with a refusal, then closes. ClickHouse Keeper without a quorum
/// sends protocol version 42 and session id -1 or 0 depending on its version. Closes any later
/// handshake with a protocol version other than 0, which Keeper can't parse, and serves a session
/// to a handshake that asks for a new session or an existing one.
async fn rejecting_server(
    listener: TcpListener,
    reject_protocol_version: i32,
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
        handshakes.lock().unwrap().push(handshake.clone());
        if first {
            first = false;
            write_connect_response(
                &mut sock,
                reject_protocol_version,
                reject_session_id,
                &[0; 16],
            )
            .await;
        } else if handshake.protocol_version == 0 && handshake.session_id >= 0 {
            write_connect_response(&mut sock, 0, SESSION_ID, &PASSWD).await;
            let _ = served.send(()).await;
            serve(sock);
        }
    }
}

fn track_connected(zk: &ZooKeeper) -> Arc<Mutex<Vec<ZkState>>> {
    let states = Arc::new(Mutex::new(Vec::new()));
    let states_listener = states.clone();
    zk.add_listener(move |s| states_listener.lock().unwrap().push(s));
    states
}

fn connected_count(states: &Mutex<Vec<ZkState>>) -> usize {
    states
        .lock()
        .unwrap()
        .iter()
        .filter(|s| **s == ZkState::Connected)
        .count()
}

async fn reconnects_after_connection_reject(reject_protocol_version: i32, reject_session_id: i64) {
    let _ = tracing_subscriber::fmt::try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handshakes = Arc::new(Mutex::new(Vec::new()));
    let (served_tx, mut served_rx) = mpsc::channel(1);
    tokio::spawn(rejecting_server(
        listener,
        reject_protocol_version,
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
    let states = track_connected(&zk);

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
    assert_eq!(states.lock().unwrap().last(), Some(&ZkState::Connected));
    assert_eq!(
        connected_count(&states),
        1,
        "the refusal was taken for a session: {:?}",
        states.lock().unwrap()
    );
}

#[tokio::test]
async fn reconnects_after_connection_reject_with_session_id_minus_one() {
    reconnects_after_connection_reject(CONNECTION_REJECT, -1).await;
}

#[tokio::test]
async fn reconnects_after_connection_reject_with_session_id_zero() {
    reconnects_after_connection_reject(CONNECTION_REJECT, 0).await;
}

#[tokio::test]
async fn reply_without_session_id_is_a_refusal() {
    reconnects_after_connection_reject(0, 0).await;
}

/// Serves a session, drops the connection, refuses the reconnect, and records the handshake that
/// follows the refusal.
async fn refusing_after_session_server(
    listener: TcpListener,
    after_refusal: mpsc::Sender<Handshake>,
) {
    let (mut sock, _) = listener.accept().await.unwrap();
    read_handshake(&mut sock).await.unwrap();
    write_connect_response(&mut sock, 0, SESSION_ID, &PASSWD).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(sock);

    let (mut sock, _) = listener.accept().await.unwrap();
    read_handshake(&mut sock).await.unwrap();
    write_connect_response(&mut sock, CONNECTION_REJECT, -1, &[0; 16]).await;
    drop(sock);

    loop {
        let (mut sock, _) = listener.accept().await.unwrap();
        let handshake = match read_handshake(&mut sock).await {
            Some(handshake) => handshake,
            None => continue,
        };
        let _ = after_refusal.send(handshake.clone()).await;
        if handshake.protocol_version == 0 && handshake.session_id == SESSION_ID {
            write_connect_response(&mut sock, 0, SESSION_ID, &PASSWD).await;
            serve(sock);
            return;
        }
    }
}

#[tokio::test]
async fn refusal_keeps_the_established_session() {
    let _ = tracing_subscriber::fmt::try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    tokio::spawn(refusing_after_session_server(listener, tx));

    let zk = ZooKeeper::connect(
        &addr.to_string(),
        Duration::from_secs(15),
        |_: WatchedEvent| {},
    )
    .await
    .unwrap();
    let states = track_connected(&zk);

    let handshake = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("no handshake after the refusal")
        .unwrap();
    assert_eq!(handshake.protocol_version, 0, "{:?}", handshake);
    assert_eq!(handshake.session_id, SESSION_ID, "{:?}", handshake);
    assert_eq!(handshake.passwd, PASSWD.to_vec(), "{:?}", handshake);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(states.lock().unwrap().last(), Some(&ZkState::Connected));
}
