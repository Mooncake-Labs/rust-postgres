use futures_util::{future, StreamExt, TryStreamExt};
use tokio_postgres::{error::SqlState, Client, SimpleQueryMessage, SimpleQueryRow};

use crate::Cancellable;

async fn q(client: &Client, query: &str) -> Vec<SimpleQueryRow> {
    let msgs = client.simple_query(query).await.unwrap();

    msgs.into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn copy_both_error() {
    let client = crate::connect("user=postgres replication=database").await;

    let err = client
        .copy_both_simple::<bytes::Bytes>("START_REPLICATION SLOT undefined LOGICAL 0000/0000")
        .await
        .err()
        .unwrap();

    assert_eq!(err.code(), Some(&SqlState::UNDEFINED_OBJECT));

    // Ensure we can continue issuing queries
    assert_eq!(q(&client, "SELECT 1").await[0].get(0), Some("1"));
}

#[tokio::test]
async fn copy_both_stream_error() {
    let client = crate::connect("user=postgres replication=true").await;

    q(&client, "CREATE_REPLICATION_SLOT err2 PHYSICAL").await;

    // This will immediately error out after entering CopyBoth mode
    let duplex_stream = client
        .copy_both_simple::<bytes::Bytes>("START_REPLICATION SLOT err2 PHYSICAL FFFF/FFFF")
        .await
        .unwrap();

    let mut msgs: Vec<_> = duplex_stream.collect().await;
    let result = msgs.pop().unwrap();
    assert_eq!(msgs.len(), 0);
    assert!(result.unwrap_err().as_db_error().is_some());

    // Ensure we can continue issuing queries
    assert_eq!(q(&client, "DROP_REPLICATION_SLOT err2").await.len(), 0);
}

#[tokio::test]
async fn copy_both_stream_error_sync() {
    let client = crate::connect("user=postgres replication=database").await;

    q(&client, "CREATE_REPLICATION_SLOT err1 TEMPORARY PHYSICAL").await;

    // This will immediately error out after entering CopyBoth mode
    let duplex_stream = client
        .copy_both_simple::<bytes::Bytes>("START_REPLICATION SLOT err1 PHYSICAL FFFF/FFFF")
        .await
        .unwrap();

    // Immediately close our sink to send a CopyDone before receiving the ErrorResponse
    drop(duplex_stream);

    // Ensure we can continue issuing queries
    assert_eq!(q(&client, "SELECT 1").await[0].get(0), Some("1"));
}

#[tokio::test]
async fn copy_both() {
    let client = crate::connect("user=postgres replication=database").await;

    q(&client, "DROP TABLE IF EXISTS replication").await;
    q(&client, "CREATE TABLE replication (i text)").await;

    let slot_query = "CREATE_REPLICATION_SLOT slot TEMPORARY LOGICAL \"test_decoding\"";
    let lsn = q(&client, slot_query).await[0]
        .get("consistent_point")
        .unwrap()
        .to_owned();

    // We will attempt to read this from the other end
    q(&client, "BEGIN").await;
    let xid = q(&client, "SELECT txid_current()").await[0]
        .get("txid_current")
        .unwrap()
        .to_owned();
    q(&client, "INSERT INTO replication VALUES ('processed')").await;
    q(&client, "COMMIT").await;

    // Insert a second row to generate unprocessed messages in the stream
    q(&client, "INSERT INTO replication VALUES ('ignored')").await;

    let query = format!("START_REPLICATION SLOT slot LOGICAL {}", lsn);
    let duplex_stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();

    let expected = vec![
        format!("BEGIN {}", xid),
        "table public.replication: INSERT: i[text]:'processed'".to_string(),
        format!("COMMIT {}", xid),
    ];

    let actual: Vec<_> = duplex_stream
        // Process only XLogData messages
        .try_filter(|buf| future::ready(buf[0] == b'w'))
        // Playback the stream until the first expected message
        .try_skip_while(|buf| future::ready(Ok(!buf.ends_with(expected[0].as_ref()))))
        // Take only the expected number of messsage, the rest will be discarded by tokio_postgres
        .take(expected.len())
        .try_collect()
        .await
        .unwrap();

    for (msg, ending) in actual.into_iter().zip(expected.into_iter()) {
        assert!(msg.ends_with(ending.as_ref()));
    }

    // Ensure we can continue issuing queries
    assert_eq!(q(&client, "SELECT 1").await[0].get(0), Some("1"));
}

#[tokio::test]
async fn copy_both_future_cancellation() {
    let client = crate::connect("user=postgres replication=database").await;

    let slot_query =
        "CREATE_REPLICATION_SLOT future_cancellation TEMPORARY LOGICAL \"test_decoding\"";
    let lsn = q(&client, slot_query).await[0]
        .get("consistent_point")
        .unwrap()
        .to_owned();

    let query = format!("START_REPLICATION SLOT future_cancellation LOGICAL {}", lsn);
    for i in 0.. {
        let done = {
            let duplex_stream = client.copy_both_simple::<bytes::Bytes>(&query);
            let fut = Cancellable {
                fut: duplex_stream,
                polls_left: i,
            };
            fut.await
                .map(|res| res.expect("copy_both failed"))
                .is_some()
        };

        // Ensure we can continue issuing queries
        assert_eq!(q(&client, "SELECT 1").await[0].get(0), Some("1"));

        if done {
            break;
        }
    }
}

// New tests for tokio-based batching and closure semantics
use bytes::Bytes as RawBytes;
use postgres_protocol::message::backend::Message;

#[tokio::test]
async fn copy_both_recv_many_raw_batches() {
    let client = crate::connect("user=postgres replication=database").await;

    // Prepare data
    q(&client, "DROP TABLE IF EXISTS replication_b").await;
    q(&client, "CREATE TABLE replication_b (i text)").await;
    let slot_query = "CREATE_REPLICATION_SLOT slot_b TEMPORARY LOGICAL \"test_decoding\"";
    let lsn = q(&client, slot_query).await[0]
        .get("consistent_point")
        .unwrap()
        .to_owned();
    q(&client, "BEGIN").await;
    q(&client, "INSERT INTO replication_b VALUES ('a')").await;
    q(&client, "INSERT INTO replication_b VALUES ('b')").await;
    q(&client, "COMMIT").await;

    let query = format!("START_REPLICATION SLOT slot_b LOGICAL {}", lsn);
    let mut duplex = client
        .copy_both_simple::<RawBytes>(&query)
        .await
        .expect("copy_both_simple");

    // Batch fetch with different max sizes
    let mut out = Vec::<Result<Message, tokio_postgres::Error>>::new();

    let n1 = duplex.recv_many_raw(&mut out, 1).await;
    assert!(n1 <= 1);
    assert!(out
        .iter()
        .take(n1)
        .all(|r| matches!(r, Ok(Message::CopyData(_)) | Ok(_))));

    out.clear();
    let n2 = duplex.recv_many_raw(&mut out, 16).await;
    assert!(n2 >= 1);
    // At least one XLogData (starts with 'w') should be present
    let have_w = out
        .iter()
        .take(n2)
        .any(|r| matches!(r, Ok(Message::CopyData(cd)) if cd.data()[0] == b'w'));
    assert!(have_w);
}

#[tokio::test]
async fn copy_both_recv_many_raw_zero_max() {
    let client = crate::connect("user=postgres replication=database").await;

    let slot_query = "CREATE_REPLICATION_SLOT slot_zero TEMPORARY LOGICAL \"test_decoding\"";
    let lsn = q(&client, slot_query).await[0]
        .get("consistent_point")
        .unwrap()
        .to_owned();

    let query = format!("START_REPLICATION SLOT slot_zero LOGICAL {}", lsn);
    let mut duplex = client
        .copy_both_simple::<RawBytes>(&query)
        .await
        .expect("copy_both_simple");

    let mut out = Vec::<Result<Message, tokio_postgres::Error>>::new();
    let n = duplex.recv_many_raw(&mut out, 0).await;
    assert_eq!(n, 0);
    assert!(out.is_empty());
}

#[tokio::test]
async fn copy_both_error_with_recv_many_raw() {
    let client = crate::connect("user=postgres replication=database").await;

    q(
        &client,
        "CREATE_REPLICATION_SLOT err_many TEMPORARY PHYSICAL",
    )
    .await;

    // Enter CopyBoth and get an error
    let mut duplex = client
        .copy_both_simple::<RawBytes>("START_REPLICATION SLOT err_many PHYSICAL FFFF/FFFF")
        .await
        .unwrap();

    let mut out = Vec::<Result<Message, tokio_postgres::Error>>::new();
    let n = duplex.recv_many_raw(&mut out, 8).await;
    assert!(n >= 1);
    assert!(out
        .iter()
        .take(n)
        .any(|r| matches!(r, Err(e) if e.as_db_error().is_some())));

    // Ensure connection remains usable
    assert_eq!(q(&client, "SELECT 1").await[0].get(0), Some("1"));
}
