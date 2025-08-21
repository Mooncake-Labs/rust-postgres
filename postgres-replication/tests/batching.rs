use std::cell::Cell;

use futures_util::pin_mut;
use postgres_replication::protocol::LogicalReplicationMessage;
use postgres_replication::protocol::ReplicationMessage::{PrimaryKeepAlive, XLogData};
use postgres_replication::{LogicalReplicationStream, ReplicationStream};
use tokio_postgres::NoTls;
use tokio_postgres::SimpleQueryMessage::Row;

#[tokio::test]
async fn replication_stream_next_batch_msgs_raw() {
    // form SQL connection
    let conninfo = "host=127.0.0.1 port=5433 user=postgres replication=database";
    let (client, connection) = tokio_postgres::connect(conninfo, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Prepare schema and publication
    client
        .simple_query("DROP TABLE IF EXISTS test_logical_replication_batch_raw")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE test_logical_replication_batch_raw(i int)")
        .await
        .unwrap();
    client
        .simple_query("DROP PUBLICATION IF EXISTS test_pub_batch_raw")
        .await
        .unwrap();
    client
        .simple_query("CREATE PUBLICATION test_pub_batch_raw FOR ALL TABLES")
        .await
        .unwrap();

    // Create temporary logical replication slot
    let slot = "test_logical_slot_batch_raw";
    let query = format!(
        r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL "pgoutput""#,
        slot
    );
    let slot_query = client.simple_query(&query).await.unwrap();
    let lsn = if let Row(row) = &slot_query[1] {
        row.get("consistent_point").unwrap()
    } else {
        panic!("unexpected query message");
    };

    // Generate a transaction that will appear in the slot's stream
    client
        .simple_query("INSERT INTO test_logical_replication_batch_raw VALUES (1)")
        .await
        .unwrap();

    // Start logical replication
    let options = r#"("proto_version" '1', "publication_names" 'test_pub_batch_raw')"#;
    let start_query = format!(
        r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
        slot, lsn, options
    );
    let copy_stream = client
        .copy_both_simple::<bytes::Bytes>(&start_query)
        .await
        .unwrap();

    // Wrap in ReplicationStream and batch-recv
    let stream = ReplicationStream::new(copy_stream);
    pin_mut!(stream);

    let mut out = Vec::new();
    let _n = stream.as_mut().next_batch_msgs(&mut out, 16).await;

    // Expect at least one XLogData frame; attempt to parse first logical message inside
    let mut saw_xlog = false;
    for item in &out {
        match item {
            Ok(XLogData(body)) => {
                saw_xlog = true;
                // Validate inner logical message parses
                let buf = body.data();
                let _ = LogicalReplicationMessage::parse(buf, 1, &Cell::new(false)).unwrap();
                break;
            }
            Ok(PrimaryKeepAlive(_)) => {}
            Ok(_) => {}
            Err(_) => {}
        }
    }
    assert!(saw_xlog, "expected at least one XLogData frame in batch");
}

#[tokio::test]
async fn logical_replication_stream_next_batch_msgs_logical() {
    // form SQL connection
    let conninfo = "host=127.0.0.1 port=5433 user=postgres replication=database";
    let (client, connection) = tokio_postgres::connect(conninfo, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Prepare schema and publication
    client
        .simple_query("DROP TABLE IF EXISTS test_logical_replication_batch_logical")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE test_logical_replication_batch_logical(i int)")
        .await
        .unwrap();
    client
        .simple_query("DROP PUBLICATION IF EXISTS test_pub_batch_logical")
        .await
        .unwrap();
    client
        .simple_query("CREATE PUBLICATION test_pub_batch_logical FOR ALL TABLES")
        .await
        .unwrap();

    // Create temporary logical replication slot
    let slot = "test_logical_slot_batch_logical";
    let query = format!(
        r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL "pgoutput""#,
        slot
    );
    let slot_query = client.simple_query(&query).await.unwrap();
    let lsn = if let Row(row) = &slot_query[1] {
        row.get("consistent_point").unwrap()
    } else {
        panic!("unexpected query message");
    };

    // Generate a transaction that will appear in the slot's stream
    client
        .simple_query(
            "BEGIN; INSERT INTO test_logical_replication_batch_logical VALUES (2); COMMIT;",
        )
        .await
        .unwrap();

    // Start logical replication
    let options = r#"("proto_version" '1', "publication_names" 'test_pub_batch_logical')"#;
    let start_query = format!(
        r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
        slot, lsn, options
    );
    let copy_stream = client
        .copy_both_simple::<bytes::Bytes>(&start_query)
        .await
        .unwrap();

    let stream = LogicalReplicationStream::new(copy_stream, Some(1));
    tokio::pin!(stream);

    let mut out = Vec::new();

    // Pull a batch and expect at least one logical XLogData message
    let _n = stream.as_mut().next_batch_msgs(&mut out, 16).await;

    let mut have_any = false;
    for item in &out {
        match item {
            Ok(XLogData(_)) => {
                have_any = true;
                break;
            }
            Ok(PrimaryKeepAlive(_)) => {}
            Ok(_) => {}
            Err(_) => {}
        }
    }
    assert!(
        have_any,
        "expected at least one logical XLogData frame in batch"
    );
}
