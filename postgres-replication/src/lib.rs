//! Utilities for working with the PostgreSQL replication copy both format.

use std::cell::Cell;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{ready, SinkExt, Stream};
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use postgres_types::PgLsn;
use tokio_postgres::CopyBothDuplex;
use tokio_postgres::Error;

pub mod protocol;

use crate::protocol::{LogicalReplicationMessage, ReplicationMessage};

const STANDBY_STATUS_UPDATE_TAG: u8 = b'r';
const HOT_STANDBY_FEEDBACK_TAG: u8 = b'h';

pin_project! {
    /// A type which deserializes the postgres replication protocol. This type can be used with
    /// both physical and logical replication to get access to the byte content of each replication
    /// message.
    ///
    /// The replication *must* be explicitly completed via the `finish` method.
    pub struct ReplicationStream {
        #[pin]
        stream: CopyBothDuplex<Bytes>,
        raw_scratch: Vec<Result<Message, Error>>,
        frames_scratch: Vec<Result<ReplicationMessage<LogicalReplicationMessage>, Error>>,
    }
}

impl ReplicationStream {
    /// Creates a new ReplicationStream that will wrap the underlying CopyBoth stream
    pub fn new(stream: CopyBothDuplex<Bytes>) -> Self {
        Self {
            stream,
            raw_scratch: Vec::new(),
            frames_scratch: Vec::new(),
        }
    }

    /// Send standby update to server.
    pub async fn standby_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), Error> {
        let mut this = self.project();

        let mut buf = BytesMut::new();
        buf.put_u8(STANDBY_STATUS_UPDATE_TAG);
        buf.put_u64(write_lsn.into());
        buf.put_u64(flush_lsn.into());
        buf.put_u64(apply_lsn.into());
        buf.put_i64(ts);
        buf.put_u8(reply);

        this.stream.send(buf.freeze()).await
    }

    /// Send hot standby feedback message to server.
    pub async fn hot_standby_feedback(
        self: Pin<&mut Self>,
        timestamp: i64,
        global_xmin: u32,
        global_xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<(), Error> {
        let mut this = self.project();

        let mut buf = BytesMut::new();
        buf.put_u8(HOT_STANDBY_FEEDBACK_TAG);
        buf.put_i64(timestamp);
        buf.put_u32(global_xmin);
        buf.put_u32(global_xmin_epoch);
        buf.put_u32(catalog_xmin);
        buf.put_u32(catalog_xmin_epoch);

        this.stream.send(buf.freeze()).await
    }

    pub async fn next_batch_msgs(
        self: core::pin::Pin<&mut Self>,
        out: &mut Vec<Result<ReplicationMessage<Bytes>, Error>>,
        max: usize,
    ) -> usize {
        let this = self.project();
        let raw = &mut *this.raw_scratch;
        raw.clear();
        raw.reserve(max);

        let n = this.stream.recv_many_raw(raw, max).await;

        out.clear();
        out.reserve(n);
        for r in raw.drain(..n) {
            out.push(match r {
                Ok(Message::CopyData(body)) => {
                    ReplicationMessage::parse(&body.into_bytes()).map_err(Error::parse)
                }
                Ok(_) => Err(Error::unexpected_message()),
                Err(e) => Err(e),
            });
        }
        n
    }

    /// Convert a raw replication message into a logical one.
    fn convert_raw_msg(
        msg: ReplicationMessage<bytes::Bytes>,
        protocol_version: u8,
        in_txn: &std::cell::Cell<bool>,
    ) -> Result<ReplicationMessage<LogicalReplicationMessage>, Error> {
        match msg {
            ReplicationMessage::XLogData(body) => body
                .map_data(|buf| LogicalReplicationMessage::parse(&buf, protocol_version, in_txn))
                .map_err(Error::parse)
                .map(ReplicationMessage::XLogData),
            ReplicationMessage::PrimaryKeepAlive(k) => Ok(ReplicationMessage::PrimaryKeepAlive(k)),
        }
    }
}

impl Stream for ReplicationStream {
    type Item = Result<ReplicationMessage<Bytes>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(buf)) => {
                Poll::Ready(Some(ReplicationMessage::parse(&buf).map_err(Error::parse)))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}

pin_project! {
    /// A type which deserializes the postgres logical replication protocol. This type gives access
    /// to a high level representation of the changes in transaction commit order.
    ///
    /// The replication *must* be explicitly completed via the `finish` method.
    pub struct LogicalReplicationStream {
        #[pin]
        stream: ReplicationStream,
        protocol_version: u8,
        in_streamed_transaction: Cell<bool>,
        frames_scratch: Vec<Result<ReplicationMessage<bytes::Bytes>, Error>>,
    }
}

impl LogicalReplicationStream {
    /// Creates a new LogicalReplicationStream that will wrap the underlying CopyBoth stream
    pub fn new(stream: CopyBothDuplex<Bytes>, protocol_version: Option<u8>) -> Self {
        Self {
            stream: ReplicationStream::new(stream),
            protocol_version: protocol_version.unwrap_or(1),
            in_streamed_transaction: Cell::new(false),
            frames_scratch: Vec::new(),
        }
    }

    /// Send standby update to server.
    pub async fn standby_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), Error> {
        let this = self.project();
        this.stream
            .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, reply)
            .await
    }

    /// Send hot standby feedback message to server.
    pub async fn hot_standby_feedback(
        self: Pin<&mut Self>,
        timestamp: i64,
        global_xmin: u32,
        global_xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<(), Error> {
        let this = self.project();
        this.stream
            .hot_standby_feedback(
                timestamp,
                global_xmin,
                global_xmin_epoch,
                catalog_xmin,
                catalog_xmin_epoch,
            )
            .await
    }
    /// Batches parsed replication messages (driven by CopyBothDuplex::recv_many_* below).
    pub async fn next_batch_msgs(
        self: core::pin::Pin<&mut Self>,
        out: &mut Vec<Result<ReplicationMessage<LogicalReplicationMessage>, Error>>,
        max: usize,
    ) -> usize {
        let mut this = self.project();

        let frames = &mut *this.frames_scratch;
        frames.clear();
        frames.reserve(max);

        let n = this.stream.as_mut().next_batch_msgs(frames, max).await;

        let protocol_version: u8 = *this.protocol_version;
        let in_txn = &this.in_streamed_transaction;

        out.clear();
        out.reserve(n);
        for f in frames.drain(..n) {
            out.push(match f {
                Ok(raw) => ReplicationStream::convert_raw_msg(raw, protocol_version, in_txn),
                Err(e) => Err(e),
            });
        }
        n
    }
}

impl Stream for LogicalReplicationStream {
    type Item = Result<ReplicationMessage<LogicalReplicationMessage>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let protocol_version: u8 = *this.protocol_version;
        let stream = this.stream.as_mut();
        let in_txn = &this.in_streamed_transaction;

        match ready!(stream.poll_next(cx)) {
            Some(Ok(raw)) => Poll::Ready(Some(ReplicationStream::convert_raw_msg(
                raw,
                protocol_version,
                in_txn,
            ))),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}
