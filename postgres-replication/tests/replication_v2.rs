use futures_util::StreamExt;
use postgres_replication::protocol::LogicalReplicationMessage::{
    Insert, StreamAbort, StreamCommit, StreamStart, StreamStop,
};
use postgres_replication::protocol::ReplicationMessage::*;
use postgres_replication::protocol::TupleData;
use postgres_replication::LogicalReplicationStream;
use tokio_postgres::NoTls;
use tokio_postgres::SimpleQueryMessage::Row;

#[tokio::test]
async fn test_replication_v2() {
    // Skip this test if PostgreSQL version < 14
    // You could add a helper function to check the version and skip if necessary

    // form SQL connection
    let conninfo = "host=127.0.0.1 port=5433 user=postgres replication=database";
    let (client, connection) = tokio_postgres::connect(conninfo, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    println!("Connecting to PostgreSQL with: {}", conninfo);
    println!("Successfully connected to PostgreSQL");

    // Check PostgreSQL version
    println!("Checking PostgreSQL version...");
    let version_res = client
        .simple_query("SHOW server_version_num")
        .await
        .unwrap();
    let version: i32 = if let Row(row) = &version_res[1] {
        row.get(0).unwrap().parse().unwrap()
    } else {
        panic!("unexpected query message");
    };

    println!("Server version string: {}", version);
    println!("PostgreSQL version: {}", version);

    if version < 140000 {
        println!("Skipping test_replication_v2: PostgreSQL version < 14");
        return;
    }

    println!("Checking WAL level configuration...");
    let wal_res = client.simple_query("SHOW wal_level").await.unwrap();
    let wal_level = if let Row(row) = &wal_res[1] {
        row.get(0).unwrap()
    } else {
        panic!("unexpected query message");
    };
    println!("WAL level: {}", wal_level);

    // Set up a test table
    client
        .simple_query("DROP TABLE IF EXISTS test_logical_replication_v2")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE test_logical_replication_v2(i int)")
        .await
        .unwrap();

    // Get the relation ID
    let res = client
        .simple_query("SELECT 'test_logical_replication_v2'::regclass::oid")
        .await
        .unwrap();
    let rel_id: u32 = if let Row(row) = &res[1] {
        row.get("oid").unwrap().parse().unwrap()
    } else {
        panic!("unexpected query message");
    };

    // Set up a publication
    client
        .simple_query("DROP PUBLICATION IF EXISTS test_pub_v2")
        .await
        .unwrap();
    client
        .simple_query("CREATE PUBLICATION test_pub_v2 FOR ALL TABLES")
        .await
        .unwrap();

    // Create a replication slot
    let slot = "test_logical_slot_v2";
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

    // Issue a query to create some replication data
    client
        .simple_query(
            "INSERT INTO test_logical_replication_v2 VALUES (generate_series(1, 1000000))",
        )
        .await
        .unwrap();

    // Start replication with protocol version 2
    let options = r#"("proto_version" '2', "publication_names" 'test_pub_v2', "streaming" 'true')"#;
    let query = format!(
        r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
        slot, lsn, options
    );
    let copy_stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();

    // Create replication stream with protocol version 2
    let stream = LogicalReplicationStream::new(copy_stream, Some(2));
    tokio::pin!(stream);

    // verify that we can observe the transaction in the replication stream
    let stream_start = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let StreamStart(stream_start) = body.into_data() {
                    break stream_start;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
        }
    };

    let insert = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let Insert(insert) = body.into_data() {
                    break insert;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
        }
    };

    let _ = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let StreamStop(stream_end) = body.into_data() {
                    break stream_end;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
        }
    };

    let stream_commit = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let StreamCommit(stream_commit) = body.into_data() {
                    break stream_commit;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
        }
    };

    assert!(stream_start.xid() > 0, "Expected non-zero XID");
    assert!(
        stream_start.is_first_segment() == 0 || stream_start.is_first_segment() == 1,
        "is_first_segment should be 0 or 1"
    );

    // Assert Insert properties
    assert_eq!(insert.rel_id(), rel_id);
    let tuple_data = insert.tuple().tuple_data();
    assert_eq!(tuple_data.len(), 1);
    assert!(matches!(tuple_data[0], TupleData::Text(_)));

    // Assert StreamCommit properties
    assert!(
        stream_commit.xid() > 0,
        "Expected StreamCommit xid to be non-zero"
    );

    assert_eq!(
        stream_commit.flags(),
        0,
        "Expected StreamCommit flags to be 0 (reserved for future use)"
    );

    assert!(
        stream_commit.commit_lsn() <= stream_commit.end_lsn(),
        "commit_lsn ({}) should be <= end_lsn ({})",
        stream_commit.commit_lsn(),
        stream_commit.end_lsn()
    );

    assert!(
        stream_commit.timestamp() > 0,
        "Expected StreamCommit timestamp to be non-zero"
    );
}

#[tokio::test]
async fn test_replication_v2_abort() {
    // Skip this test if PostgreSQL version < 14
    let conninfo = "host=127.0.0.1 port=5433 user=postgres replication=database";
    let (client, connection) = tokio_postgres::connect(conninfo, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    println!("Connecting to PostgreSQL with: {}", conninfo);
    println!("Successfully connected to PostgreSQL");

    // Check PostgreSQL version
    println!("Checking PostgreSQL version...");
    let version_res = client
        .simple_query("SHOW server_version_num")
        .await
        .unwrap();
    let version: i32 = if let Row(row) = &version_res[1] {
        row.get(0).unwrap().parse().unwrap()
    } else {
        panic!("unexpected query message");
    };

    println!("PostgreSQL version: {}", version);

    if version < 140000 {
        println!("Skipping test_replication_v2_abort: PostgreSQL version < 14");
        return;
    }

    println!("Checking WAL level configuration...");
    let wal_res = client.simple_query("SHOW wal_level").await.unwrap();
    let wal_level = if let Row(row) = &wal_res[1] {
        row.get(0).unwrap()
    } else {
        panic!("unexpected query message");
    };
    println!("WAL level: {}", wal_level);

    // Set up a test table
    client
        .simple_query("DROP TABLE IF EXISTS test_logical_replication_v2_abort")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE test_logical_replication_v2_abort(i int)")
        .await
        .unwrap();

    // Set up a publication
    client
        .simple_query("DROP PUBLICATION IF EXISTS test_pub_v2_abort")
        .await
        .unwrap();
    client
        .simple_query("CREATE PUBLICATION test_pub_v2_abort FOR ALL TABLES")
        .await
        .unwrap();

    // Create a replication slot
    let slot = "test_logical_slot_v2_abort";
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

    // Start a transaction and then abort it
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query(
            "INSERT INTO test_logical_replication_v2_abort VALUES (generate_series(1, 1000000))",
        )
        .await
        .unwrap();
    client.simple_query("ROLLBACK").await.unwrap();

    // Start replication with protocol version 2
    let options =
        r#"("proto_version" '2', "publication_names" 'test_pub_v2_abort', "streaming" 'true')"#;
    let query = format!(
        r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
        slot, lsn, options
    );
    let copy_stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();

    // Create replication stream with protocol version 2
    let stream = LogicalReplicationStream::new(copy_stream, Some(2));
    tokio::pin!(stream);

    // First we should see a StreamStart message for the transaction
    let stream_start = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let StreamStart(stream_start) = body.into_data() {
                    break stream_start;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(e)) => panic!("unexpected replication stream error: {}", e),
            None => panic!("unexpected replication stream end"),
        }
    };

    // Finally, we should see a StreamAbort message for the rolled back transaction
    let stream_abort = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let StreamAbort(stream_abort) = body.into_data() {
                    break stream_abort;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(e)) => panic!("unexpected replication stream error: {}", e),
            None => panic!("unexpected replication stream end"),
        }
    };

    // Assert that we got the correct abort message
    assert!(
        stream_start.xid() > 0,
        "Expected non-zero XID for stream start"
    );
    assert_eq!(
        stream_abort.xid(),
        stream_start.xid(),
        "Expected abort XID to match the stream start XID"
    );
    assert_eq!(
        stream_abort.subxid(),
        stream_abort.xid(),
        "For top-level transaction abort, subxid should match xid"
    );
}
