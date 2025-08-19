use std::cell::Cell;
use std::io;
use std::str;

use bytes::{Buf, Bytes};
use memchr::memchr;
use postgres_protocol::{Lsn, Oid};

// replication message tags
pub const XLOG_DATA_TAG: u8 = b'w';
pub const PRIMARY_KEEPALIVE_TAG: u8 = b'k';

// logical replication message tags
const BEGIN_TAG: u8 = b'B';
const COMMIT_TAG: u8 = b'C';
const ORIGIN_TAG: u8 = b'O';
const RELATION_TAG: u8 = b'R';
const TYPE_TAG: u8 = b'Y';
const INSERT_TAG: u8 = b'I';
const UPDATE_TAG: u8 = b'U';
const DELETE_TAG: u8 = b'D';
const TRUNCATE_TAG: u8 = b'T';
const TUPLE_NEW_TAG: u8 = b'N';
const TUPLE_KEY_TAG: u8 = b'K';
const TUPLE_OLD_TAG: u8 = b'O';
const TUPLE_DATA_NULL_TAG: u8 = b'n';
const TUPLE_DATA_TOAST_TAG: u8 = b'u';
const TUPLE_DATA_TEXT_TAG: u8 = b't';
// logical replication version 2.0 message tags
const STREAM_START_TAG: u8 = b'S';
const STREAM_STOP_TAG: u8 = b'E';
const STREAM_COMMIT_TAG: u8 = b'c';
const STREAM_ABORT_TAG: u8 = b'A';

// replica identity tags
const REPLICA_IDENTITY_DEFAULT_TAG: u8 = b'd';
const REPLICA_IDENTITY_NOTHING_TAG: u8 = b'n';
const REPLICA_IDENTITY_FULL_TAG: u8 = b'f';
const REPLICA_IDENTITY_INDEX_TAG: u8 = b'i';

/// An enum representing Postgres backend replication messages.
#[non_exhaustive]
#[derive(Debug)]
pub enum ReplicationMessage<D> {
    XLogData(XLogDataBody<D>),
    PrimaryKeepAlive(PrimaryKeepAliveBody),
}

impl ReplicationMessage<Bytes> {
    #[inline]
    pub fn parse(buf: &Bytes) -> io::Result<Self> {
        let mut buf = Buffer { bytes: buf.clone() };

        let tag = buf.read_u8()?;

        let replication_message = match tag {
            XLOG_DATA_TAG => {
                let wal_start = buf.read_u64_be()?;
                let wal_end = buf.read_u64_be()?;
                let timestamp = buf.read_i64_be()?;
                let data = buf.read_all();
                ReplicationMessage::XLogData(XLogDataBody {
                    wal_start,
                    wal_end,
                    timestamp,
                    data,
                })
            }
            PRIMARY_KEEPALIVE_TAG => {
                let wal_end = buf.read_u64_be()?;
                let timestamp = buf.read_i64_be()?;
                let reply = buf.read_u8()?;
                ReplicationMessage::PrimaryKeepAlive(PrimaryKeepAliveBody {
                    wal_end,
                    timestamp,
                    reply,
                })
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(replication_message)
    }
}

#[derive(Debug)]
pub struct XLogDataBody<D> {
    wal_start: u64,
    wal_end: u64,
    timestamp: i64,
    data: D,
}

impl<D> XLogDataBody<D> {
    #[inline]
    pub fn wal_start(&self) -> u64 {
        self.wal_start
    }

    #[inline]
    pub fn wal_end(&self) -> u64 {
        self.wal_end
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    pub fn data(&self) -> &D {
        &self.data
    }

    #[inline]
    pub fn into_data(self) -> D {
        self.data
    }

    pub fn map_data<F, D2, E>(self, f: F) -> Result<XLogDataBody<D2>, E>
    where
        F: Fn(D) -> Result<D2, E>,
    {
        let data = f(self.data)?;
        Ok(XLogDataBody {
            wal_start: self.wal_start,
            wal_end: self.wal_end,
            timestamp: self.timestamp,
            data,
        })
    }
}

#[derive(Debug)]
pub struct PrimaryKeepAliveBody {
    wal_end: u64,
    timestamp: i64,
    reply: u8,
}

impl PrimaryKeepAliveBody {
    #[inline]
    pub fn wal_end(&self) -> u64 {
        self.wal_end
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    pub fn reply(&self) -> u8 {
        self.reply
    }
}

#[non_exhaustive]
/// A message of the logical replication stream
#[derive(Debug)]
pub enum LogicalReplicationMessage {
    /// A BEGIN statement
    Begin(BeginBody),
    /// A BEGIN statement
    Commit(CommitBody),
    /// An Origin replication message
    /// Note that there can be multiple Origin messages inside a single transaction.
    Origin(OriginBody),
    /// A Relation replication message
    Relation(RelationBody),
    /// A Type replication message
    Type(TypeBody),
    /// An INSERT statement
    Insert(InsertBody),
    /// An UPDATE statement
    Update(UpdateBody),
    /// A DELETE statement
    Delete(DeleteBody),
    /// A TRUNCATE statement
    Truncate(TruncateBody),
    /// (V2)
    /// A STREAM START statement
    StreamStart(StreamStartBody),
    /// A STREAM STOP statement
    StreamStop(StreamStopBody),
    /// A STREAM COMMIT statement
    StreamCommit(StreamCommitBody),
    /// A STREAM ABORT statement
    StreamAbort(StreamAbortBody),
}

impl LogicalReplicationMessage {
    pub fn parse(
        buf: &Bytes,
        protocol_version: u8,
        in_streamed_transaction: &Cell<bool>,
    ) -> io::Result<Self> {
        let mut buf = Buffer { bytes: buf.clone() };

        let tag = buf.read_u8()?;

        let logical_replication_message = match tag {
            BEGIN_TAG => Self::Begin(BeginBody {
                final_lsn: buf.read_u64_be()?,
                timestamp: buf.read_i64_be()?,
                xid: buf.read_u32_be()?,
            }),
            COMMIT_TAG => Self::Commit(CommitBody {
                flags: buf.read_i8()?,
                commit_lsn: buf.read_u64_be()?,
                end_lsn: buf.read_u64_be()?,
                timestamp: buf.read_i64_be()?,
            }),
            ORIGIN_TAG => Self::Origin(OriginBody {
                commit_lsn: buf.read_u64_be()?,
                name: buf.read_cstr()?,
            }),
            RELATION_TAG => {
                let xid = if in_streamed_transaction.get() {
                    Some(buf.read_u32_be()?)
                } else {
                    None
                };
                let rel_id = buf.read_u32_be()?;
                let namespace = buf.read_cstr()?;
                let name = buf.read_cstr()?;
                let replica_identity = match buf.read_u8()? {
                    REPLICA_IDENTITY_DEFAULT_TAG => ReplicaIdentity::Default,
                    REPLICA_IDENTITY_NOTHING_TAG => ReplicaIdentity::Nothing,
                    REPLICA_IDENTITY_FULL_TAG => ReplicaIdentity::Full,
                    REPLICA_IDENTITY_INDEX_TAG => ReplicaIdentity::Index,
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown replica identity tag `{}`", tag),
                        ));
                    }
                };
                let column_len = buf.read_i16_be()?;

                let mut columns = Vec::with_capacity(column_len as usize);
                for _ in 0..column_len {
                    columns.push(Column::parse(&mut buf)?);
                }

                Self::Relation(RelationBody {
                    xid,
                    rel_id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                })
            }
            TYPE_TAG => Self::Type(TypeBody {
                xid: if in_streamed_transaction.get() {
                    Some(buf.read_u32_be()?)
                } else {
                    None
                },
                id: buf.read_u32_be()?,
                namespace: buf.read_cstr()?,
                name: buf.read_cstr()?,
            }),
            INSERT_TAG => {
                let xid = if in_streamed_transaction.get() {
                    Some(buf.read_u32_be()?)
                } else {
                    None
                };
                let rel_id = buf.read_u32_be()?;
                let tag = buf.read_u8()?;

                let tuple = match tag {
                    TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unexpected tuple tag `{}`", tag),
                        ));
                    }
                };

                Self::Insert(InsertBody { xid, rel_id, tuple })
            }
            UPDATE_TAG => {
                let xid = if in_streamed_transaction.get() {
                    Some(buf.read_u32_be()?)
                } else {
                    None
                };
                let rel_id = buf.read_u32_be()?;
                let tag = buf.read_u8()?;

                let mut key_tuple = None;
                let mut old_tuple = None;

                let new_tuple = match tag {
                    TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                    TUPLE_OLD_TAG | TUPLE_KEY_TAG => {
                        if tag == TUPLE_OLD_TAG {
                            old_tuple = Some(Tuple::parse(&mut buf)?);
                        } else {
                            key_tuple = Some(Tuple::parse(&mut buf)?);
                        }

                        match buf.read_u8()? {
                            TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                            tag => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("unexpected tuple tag `{}`", tag),
                                ));
                            }
                        }
                    }
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown tuple tag `{}`", tag),
                        ));
                    }
                };

                Self::Update(UpdateBody {
                    xid,
                    rel_id,
                    key_tuple,
                    old_tuple,
                    new_tuple,
                })
            }
            DELETE_TAG => {
                let xid = if in_streamed_transaction.get() {
                    Some(buf.read_u32_be()?)
                } else {
                    None
                };
                let rel_id = buf.read_u32_be()?;
                let tag = buf.read_u8()?;

                let mut key_tuple = None;
                let mut old_tuple = None;

                match tag {
                    TUPLE_OLD_TAG => old_tuple = Some(Tuple::parse(&mut buf)?),
                    TUPLE_KEY_TAG => key_tuple = Some(Tuple::parse(&mut buf)?),
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown tuple tag `{}`", tag),
                        ));
                    }
                }

                Self::Delete(DeleteBody {
                    xid,
                    rel_id,
                    key_tuple,
                    old_tuple,
                })
            }
            TRUNCATE_TAG => {
                let xid = if in_streamed_transaction.get() {
                    Some(buf.read_u32_be()?)
                } else {
                    None
                };
                let relation_len = buf.read_i32_be()?;
                let options = buf.read_i8()?;

                let mut rel_ids = Vec::with_capacity(relation_len as usize);
                for _ in 0..relation_len {
                    rel_ids.push(buf.read_u32_be()?);
                }

                Self::Truncate(TruncateBody {
                    xid,
                    options,
                    rel_ids,
                })
            }
            // Protocol v2 messages
            STREAM_START_TAG if protocol_version >= 2 => {
                in_streamed_transaction.set(true);
                Self::StreamStart(StreamStartBody {
                    xid: buf.read_u32_be()?,
                    is_first_segment: buf.read_u8()?,
                })
            }
            STREAM_STOP_TAG if protocol_version >= 2 => {
                in_streamed_transaction.set(false);
                Self::StreamStop(StreamStopBody {})
            }
            STREAM_COMMIT_TAG if protocol_version >= 2 => {
                in_streamed_transaction.set(false);
                Self::StreamCommit(StreamCommitBody {
                    xid: buf.read_u32_be()?,
                    flags: buf.read_i8()?,
                    commit_lsn: buf.read_u64_be()?,
                    end_lsn: buf.read_u64_be()?,
                    timestamp: buf.read_i64_be()?,
                })
            }
            STREAM_ABORT_TAG if protocol_version >= 2 => {
                in_streamed_transaction.set(false);
                Self::StreamAbort(StreamAbortBody {
                    xid: buf.read_u32_be()?,
                    subxid: buf.read_u32_be()?,
                })
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(logical_replication_message)
    }
}

/// A row as it appears in the replication stream
#[derive(Debug)]
pub struct Tuple(Vec<TupleData>);

impl Tuple {
    #[inline]
    /// The tuple data of this tuple
    pub fn tuple_data(&self) -> &[TupleData] {
        &self.0
    }
}

impl Tuple {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        let col_len = buf.read_i16_be()?;
        let mut tuple = Vec::with_capacity(col_len as usize);
        for _ in 0..col_len {
            tuple.push(TupleData::parse(buf)?);
        }

        Ok(Tuple(tuple))
    }
}

/// A column as it appears in the replication stream
#[derive(Debug)]
pub struct Column {
    flags: i8,
    name: Bytes,
    type_id: i32,
    type_modifier: i32,
}

impl Column {
    #[inline]
    /// Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as
    /// part of the key.
    pub fn flags(&self) -> i8 {
        self.flags
    }

    #[inline]
    /// Name of the column.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    /// ID of the column's data type.
    pub fn type_id(&self) -> i32 {
        self.type_id
    }

    #[inline]
    /// Type modifier of the column (`atttypmod`).
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }
}

impl Column {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        Ok(Self {
            flags: buf.read_i8()?,
            name: buf.read_cstr()?,
            type_id: buf.read_i32_be()?,
            type_modifier: buf.read_i32_be()?,
        })
    }
}

/// The data of an individual column as it appears in the replication stream
#[derive(Debug)]
pub enum TupleData {
    /// Represents a NULL value
    Null,
    /// Represents an unchanged TOASTed value (the actual value is not sent).
    UnchangedToast,
    /// Column data as text formatted value.
    Text(Bytes),
}

impl TupleData {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        let type_tag = buf.read_u8()?;

        let tuple = match type_tag {
            TUPLE_DATA_NULL_TAG => TupleData::Null,
            TUPLE_DATA_TOAST_TAG => TupleData::UnchangedToast,
            TUPLE_DATA_TEXT_TAG => {
                let len = buf.read_i32_be()?;
                let data = buf.read_len(len as usize)?;
                TupleData::Text(data)
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(tuple)
    }
}

/// A BEGIN statement
#[derive(Debug)]
pub struct BeginBody {
    final_lsn: u64,
    timestamp: i64,
    xid: u32,
}

impl BeginBody {
    #[inline]
    /// Gets the final lsn of the transaction
    pub fn final_lsn(&self) -> Lsn {
        self.final_lsn
    }

    #[inline]
    /// Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    /// Xid of the transaction.
    pub fn xid(&self) -> u32 {
        self.xid
    }
}

/// A COMMIT statement
#[derive(Debug)]
pub struct CommitBody {
    flags: i8,
    commit_lsn: u64,
    end_lsn: u64,
    timestamp: i64,
}

impl CommitBody {
    #[inline]
    /// The LSN of the commit.
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn
    }

    #[inline]
    /// The end LSN of the transaction.
    pub fn end_lsn(&self) -> Lsn {
        self.end_lsn
    }

    #[inline]
    /// Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    /// Flags; currently unused (will be 0).
    pub fn flags(&self) -> i8 {
        self.flags
    }
}

/// An Origin replication message
///
/// Note that there can be multiple Origin messages inside a single transaction.
#[derive(Debug)]
pub struct OriginBody {
    commit_lsn: u64,
    name: Bytes,
}

impl OriginBody {
    #[inline]
    /// The LSN of the commit on the origin server.
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn
    }

    #[inline]
    /// Name of the origin.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }
}

/// Describes the REPLICA IDENTITY setting of a table
#[derive(Debug)]
pub enum ReplicaIdentity {
    /// default selection for replica identity (primary key or nothing)
    Default,
    /// no replica identity is logged for this relation
    Nothing,
    /// all columns are logged as replica identity
    Full,
    /// An explicitly chosen candidate key's columns are used as replica identity.
    /// Note this will still be set if the index has been dropped; in that case it
    /// has the same meaning as 'd'.
    Index,
}

/// A Relation replication message
#[derive(Debug)]
pub struct RelationBody {
    xid: Option<u32>,
    rel_id: u32,
    namespace: Bytes,
    name: Bytes,
    replica_identity: ReplicaIdentity,
    columns: Vec<Column>,
}

impl RelationBody {
    /// The transaction ID of the transaction that started the stream
    #[inline]
    pub fn xid(&self) -> Option<u32> {
        self.xid
    }

    #[inline]
    /// ID of the relation.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// Namespace (empty string for pg_catalog).
    pub fn namespace(&self) -> io::Result<&str> {
        get_str(&self.namespace)
    }

    #[inline]
    /// Relation name.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    /// Replica identity setting for the relation
    pub fn replica_identity(&self) -> &ReplicaIdentity {
        &self.replica_identity
    }

    #[inline]
    /// The column definitions of this relation
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

/// A Type replication message
#[derive(Debug)]
pub struct TypeBody {
    xid: Option<u32>,
    id: u32,
    namespace: Bytes,
    name: Bytes,
}

impl TypeBody {
    /// The transaction ID of the transaction that started the stream
    #[inline]
    pub fn xid(&self) -> Option<u32> {
        self.xid
    }

    /// ID of the data type.
    #[inline]
    pub fn id(&self) -> Oid {
        self.id
    }

    #[inline]
    /// Namespace (empty string for pg_catalog).
    pub fn namespace(&self) -> io::Result<&str> {
        get_str(&self.namespace)
    }

    #[inline]
    /// Name of the data type.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }
}

/// An INSERT statement
#[derive(Debug)]
pub struct InsertBody {
    xid: Option<u32>,
    rel_id: u32,
    tuple: Tuple,
}

impl InsertBody {
    #[inline]
    pub fn xid(&self) -> Option<u32> {
        self.xid
    }

    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// The inserted tuple
    pub fn tuple(&self) -> &Tuple {
        &self.tuple
    }
}

/// An UPDATE statement
#[derive(Debug)]
pub struct UpdateBody {
    xid: Option<u32>,
    rel_id: u32,
    old_tuple: Option<Tuple>,
    key_tuple: Option<Tuple>,
    new_tuple: Tuple,
}

impl UpdateBody {
    #[inline]
    pub fn xid(&self) -> Option<u32> {
        self.xid
    }

    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// This field is optional and is only present if the update changed data in any of the
    /// column(s) that are part of the REPLICA IDENTITY index.
    pub fn key_tuple(&self) -> Option<&Tuple> {
        self.key_tuple.as_ref()
    }

    #[inline]
    /// This field is optional and is only present if table in which the update happened has
    /// REPLICA IDENTITY set to FULL.
    pub fn old_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }

    #[inline]
    /// The new tuple
    pub fn new_tuple(&self) -> &Tuple {
        &self.new_tuple
    }
}

/// A DELETE statement
#[derive(Debug)]
pub struct DeleteBody {
    xid: Option<u32>,
    rel_id: u32,
    old_tuple: Option<Tuple>,
    key_tuple: Option<Tuple>,
}

impl DeleteBody {
    #[inline]
    pub fn xid(&self) -> Option<u32> {
        self.xid
    }

    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// This field is present if the table in which the delete has happened uses an index as
    /// REPLICA IDENTITY.
    pub fn key_tuple(&self) -> Option<&Tuple> {
        self.key_tuple.as_ref()
    }

    #[inline]
    /// This field is present if the table in which the delete has happened has REPLICA IDENTITY
    /// set to FULL.
    pub fn old_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }
}

/// A TRUNCATE statement
#[derive(Debug)]
pub struct TruncateBody {
    xid: Option<u32>,
    options: i8,
    rel_ids: Vec<u32>,
}

impl TruncateBody {
    #[inline]
    pub fn xid(&self) -> Option<u32> {
        self.xid
    }

    #[inline]
    /// The IDs of the relations corresponding to the ID in the relation messages
    pub fn rel_ids(&self) -> &[u32] {
        &self.rel_ids
    }

    #[inline]
    /// Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
    pub fn options(&self) -> i8 {
        self.options
    }
}

// A Stream Start replication message (v2)
#[derive(Debug)]
pub struct StreamStartBody {
    xid: u32,
    is_first_segment: u8,
}

impl StreamStartBody {
    #[inline]
    /// The transaction ID of the transaction that started the stream
    pub fn xid(&self) -> u32 {
        self.xid
    }

    #[inline]
    /// Whether this is the first segment of the stream for this XID
    pub fn is_first_segment(&self) -> u8 {
        self.is_first_segment
    }
}

/// A Stream Stop replication message (v2)
#[derive(Debug)]
pub struct StreamStopBody {
    // This message has no additional data fields
}

/// A Stream Commit replication message (v2)
#[derive(Debug)]
pub struct StreamCommitBody {
    xid: u32,
    flags: i8,
    commit_lsn: u64,
    end_lsn: u64,
    timestamp: i64,
}

impl StreamCommitBody {
    #[inline]
    /// Xid of the transaction.
    pub fn xid(&self) -> u32 {
        self.xid
    }

    #[inline]
    /// Flags; currently unused.
    pub fn flags(&self) -> i8 {
        self.flags
    }

    #[inline]
    /// The LSN of the commit.
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn
    }

    #[inline]
    /// The end LSN of the transaction.
    pub fn end_lsn(&self) -> Lsn {
        self.end_lsn
    }

    #[inline]
    /// Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

/// A Stream Abort replication message (v2)
#[derive(Debug)]
pub struct StreamAbortBody {
    xid: u32,
    subxid: u32,
}

impl StreamAbortBody {
    #[inline]
    /// Xid of the transaction.
    pub fn xid(&self) -> u32 {
        self.xid
    }

    #[inline]
    /// Xid of the subtransaction (will be same as xid of the transaction for top-level transactions).
    pub fn subxid(&self) -> u32 {
        self.subxid
    }
}

struct Buffer {
    bytes: Bytes,
}

impl Buffer {
    #[inline(always)]
    fn read_cstr(&mut self) -> io::Result<Bytes> {
        let chunk = self.bytes.chunk();
        match memchr(0, chunk) {
            Some(pos) => {
                let cstr = self.bytes.split_to(pos);
                self.bytes.advance(1);
                Ok(cstr)
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }

    #[inline(always)]
    fn read_all(&mut self) -> Bytes {
        let len = self.bytes.remaining();
        self.bytes.split_to(len)
    }

    #[inline]
    fn ensure(&self, need: usize) -> io::Result<()> {
        if self.bytes.remaining() < need {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ))
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    fn read_u8(&mut self) -> io::Result<u8> {
        Ok(self.bytes.get_u8())
    }

    #[inline(always)]
    fn read_i8(&mut self) -> io::Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    #[inline(always)]
    fn read_i16_be(&mut self) -> io::Result<i16> {
        Ok(self.bytes.get_i16())
    }

    #[inline(always)]
    fn read_i32_be(&mut self) -> io::Result<i32> {
        Ok(self.bytes.get_i32())
    }

    #[inline(always)]
    fn read_u32_be(&mut self) -> io::Result<u32> {
        Ok(self.bytes.get_u32())
    }

    #[inline(always)]
    fn read_i64_be(&mut self) -> io::Result<i64> {
        Ok(self.bytes.get_i64())
    }

    #[inline(always)]
    fn read_u64_be(&mut self) -> io::Result<u64> {
        Ok(self.bytes.get_u64())
    }

    #[inline(always)]
    fn read_len(&mut self, len: usize) -> io::Result<Bytes> {
        self.ensure(len)?;
        Ok(self.bytes.split_to(len))
    }
}

#[inline]
fn get_str(buf: &[u8]) -> io::Result<&str> {
    str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn be_i16(x: i16) -> [u8; 2] {
        x.to_be_bytes()
    }
    fn be_u32(x: u32) -> [u8; 4] {
        x.to_be_bytes()
    }
    fn be_i32(x: i32) -> [u8; 4] {
        x.to_be_bytes()
    }
    fn be_u64(x: u64) -> [u8; 8] {
        x.to_be_bytes()
    }
    fn be_i64(x: i64) -> [u8; 8] {
        x.to_be_bytes()
    }

    fn cstr(s: &str) -> Vec<u8> {
        let mut v = Vec::with_capacity(s.len() + 1);
        v.extend_from_slice(s.as_bytes());
        v.push(0);
        v
    }

    #[test]
    fn buffer_read_cstr_success() {
        let mut data = Vec::new();
        data.extend_from_slice(&cstr("hello"));
        data.extend_from_slice(b"rest");
        let mut buf = Buffer {
            bytes: Bytes::from(data),
        };
        let s = buf.read_cstr().unwrap();
        assert_eq!(get_str(&s).unwrap(), "hello");
        // Next read should see the first byte of "rest"
        assert_eq!(buf.read_u8().unwrap(), b'r');
    }

    #[test]
    fn buffer_read_cstr_eof() {
        let mut buf = Buffer {
            bytes: Bytes::from_static(b"no-null"),
        };
        let err = buf.read_cstr().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn buffer_read_len_ok_and_eof() {
        let mut buf = Buffer {
            bytes: Bytes::from_static(b"abcdef"),
        };
        let part = buf.read_len(3).unwrap();
        assert_eq!(&part[..], b"abc");
        let rest = buf.read_all();
        assert_eq!(&rest[..], b"def");
        let mut buf = Buffer {
            bytes: Bytes::from_static(b"xyz"),
        };
        let err = buf.read_len(5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn replication_message_parse_xlogdata() {
        let mut v = Vec::new();
        v.push(XLOG_DATA_TAG);
        v.extend_from_slice(&be_u64(1));
        v.extend_from_slice(&be_u64(2));
        v.extend_from_slice(&be_i64(3));
        v.extend_from_slice(b"payload");
        let msg = ReplicationMessage::<Bytes>::parse(&Bytes::from(v)).unwrap();
        match msg {
            ReplicationMessage::XLogData(body) => {
                assert_eq!(body.wal_start(), 1);
                assert_eq!(body.wal_end(), 2);
                assert_eq!(body.timestamp(), 3);
                assert_eq!(&body.data()[..], b"payload");
            }
            _ => panic!("expected XLogData"),
        }
    }

    #[test]
    fn replication_message_parse_keepalive() {
        let mut v = Vec::new();
        v.push(PRIMARY_KEEPALIVE_TAG);
        v.extend_from_slice(&be_u64(9));
        v.extend_from_slice(&be_i64(8));
        v.push(1);
        let msg = ReplicationMessage::<Bytes>::parse(&Bytes::from(v)).unwrap();
        match msg {
            ReplicationMessage::PrimaryKeepAlive(body) => {
                assert_eq!(body.wal_end(), 9);
                assert_eq!(body.timestamp(), 8);
                assert_eq!(body.reply(), 1);
            }
            _ => panic!("expected PrimaryKeepAlive"),
        }
    }

    #[test]
    fn replication_message_parse_unknown_tag() {
        let v = Bytes::from_static(b"z");
        let err = ReplicationMessage::<Bytes>::parse(&v).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn logical_parse_begin_commit_origin() {
        let in_stream = Cell::new(false);
        // Begin
        let mut v = Vec::new();
        v.push(BEGIN_TAG);
        v.extend_from_slice(&be_u64(100));
        v.extend_from_slice(&be_i64(200));
        v.extend_from_slice(&be_u32(300));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::Begin(b) => {
                assert_eq!(b.final_lsn(), 100);
                assert_eq!(b.timestamp(), 200);
                assert_eq!(b.xid(), 300);
            }
            _ => panic!("expected Begin"),
        }

        // Commit
        let mut v = Vec::new();
        v.push(COMMIT_TAG);
        v.push(0);
        v.extend_from_slice(&be_u64(111));
        v.extend_from_slice(&be_u64(112));
        v.extend_from_slice(&be_i64(113));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::Commit(c) => {
                assert_eq!(c.flags(), 0);
                assert_eq!(c.commit_lsn(), 111);
                assert_eq!(c.end_lsn(), 112);
                assert_eq!(c.timestamp(), 113);
            }
            _ => panic!("expected Commit"),
        }

        // Origin
        let mut v = Vec::new();
        v.push(ORIGIN_TAG);
        v.extend_from_slice(&be_u64(999));
        v.extend_from_slice(&cstr("origin"));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::Origin(o) => {
                assert_eq!(o.commit_lsn(), 999);
                assert_eq!(o.name().unwrap(), "origin");
            }
            _ => panic!("expected Origin"),
        }
    }

    #[test]
    fn logical_parse_relation_and_type_with_and_without_xid() {
        // Relation without xid
        let in_stream = Cell::new(false);
        let mut v = Vec::new();
        v.push(RELATION_TAG);
        v.extend_from_slice(&be_u32(42)); // rel_id
        v.extend_from_slice(&cstr("public"));
        v.extend_from_slice(&cstr("t"));
        v.push(REPLICA_IDENTITY_DEFAULT_TAG);
        v.extend_from_slice(&be_i16(1)); // 1 column
                                         // column
        v.push(0); // flags
        v.extend_from_slice(&cstr("id"));
        v.extend_from_slice(&be_i32(23)); // type_id
        v.extend_from_slice(&be_i32(-1)); // typmod
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::Relation(r) => {
                assert_eq!(r.xid(), None);
                assert_eq!(r.rel_id(), 42);
                assert_eq!(r.namespace().unwrap(), "public");
                assert_eq!(r.name().unwrap(), "t");
                assert!(matches!(r.replica_identity(), ReplicaIdentity::Default));
                assert_eq!(r.columns().len(), 1);
                assert_eq!(r.columns()[0].flags(), 0);
                assert_eq!(r.columns()[0].name().unwrap(), "id");
                assert_eq!(r.columns()[0].type_id(), 23);
                assert_eq!(r.columns()[0].type_modifier(), -1);
            }
            _ => panic!("expected Relation"),
        }

        // Type with xid
        let in_stream = Cell::new(true);
        let mut v = Vec::new();
        v.push(TYPE_TAG);
        v.extend_from_slice(&be_u32(7)); // xid present
        v.extend_from_slice(&be_u32(9999)); // id
        v.extend_from_slice(&cstr("pg_catalog"));
        v.extend_from_slice(&cstr("int4"));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::Type(t) => {
                assert_eq!(t.xid(), Some(7));
                assert_eq!(t.id(), 9999);
                assert_eq!(t.namespace().unwrap(), "pg_catalog");
                assert_eq!(t.name().unwrap(), "int4");
            }
            _ => panic!("expected Type"),
        }
    }

    fn tuple_bytes(elems: &[TupleData]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&be_i16(elems.len() as i16));
        for e in elems {
            match e {
                TupleData::Null => v.push(TUPLE_DATA_NULL_TAG),
                TupleData::UnchangedToast => v.push(TUPLE_DATA_TOAST_TAG),
                TupleData::Text(b) => {
                    v.push(TUPLE_DATA_TEXT_TAG);
                    v.extend_from_slice(&be_i32(b.len() as i32));
                    v.extend_from_slice(&b[..]);
                }
            }
        }
        v
    }

    #[test]
    fn logical_parse_insert_update_delete_truncate() {
        // Insert with two tuple items
        let in_stream = Cell::new(false);
        let mut v = Vec::new();
        v.push(INSERT_TAG);
        v.extend_from_slice(&be_u32(10)); // rel_id
        v.push(TUPLE_NEW_TAG);
        let tuple = tuple_bytes(&[TupleData::Null, TupleData::Text(Bytes::from_static(b"42"))]);
        v.extend_from_slice(&tuple);
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::Insert(i) => {
                assert_eq!(i.xid(), None);
                assert_eq!(i.rel_id(), 10);
                assert_eq!(i.tuple().tuple_data().len(), 2);
            }
            _ => panic!("expected Insert"),
        }

        // Update with old then new
        let mut v = Vec::new();
        v.push(UPDATE_TAG);
        v.extend_from_slice(&be_u32(11)); // rel_id
        v.push(TUPLE_OLD_TAG);
        v.extend_from_slice(&tuple_bytes(&[TupleData::Text(Bytes::from_static(b"old"))]));
        v.push(TUPLE_NEW_TAG);
        v.extend_from_slice(&tuple_bytes(&[TupleData::Text(Bytes::from_static(b"new"))]));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &Cell::new(false)).unwrap();
        match msg {
            LogicalReplicationMessage::Update(u) => {
                assert!(u.old_tuple().is_some());
                assert!(u.key_tuple().is_none());
                assert_eq!(u.new_tuple().tuple_data().len(), 1);
            }
            _ => panic!("expected Update"),
        }

        // Update with key then new
        let mut v = Vec::new();
        v.push(UPDATE_TAG);
        v.extend_from_slice(&be_u32(12));
        v.push(TUPLE_KEY_TAG);
        v.extend_from_slice(&tuple_bytes(&[TupleData::Text(Bytes::from_static(b"key"))]));
        v.push(TUPLE_NEW_TAG);
        v.extend_from_slice(&tuple_bytes(&[TupleData::Text(Bytes::from_static(b"new"))]));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &Cell::new(false)).unwrap();
        match msg {
            LogicalReplicationMessage::Update(u) => {
                assert!(u.key_tuple().is_some());
                assert!(u.old_tuple().is_none());
            }
            _ => panic!("expected Update"),
        }

        // Delete with old
        let mut v = Vec::new();
        v.push(DELETE_TAG);
        v.extend_from_slice(&be_u32(13));
        v.push(TUPLE_OLD_TAG);
        v.extend_from_slice(&tuple_bytes(&[TupleData::Text(Bytes::from_static(
            b"gone",
        ))]));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &Cell::new(false)).unwrap();
        match msg {
            LogicalReplicationMessage::Delete(d) => {
                assert!(d.old_tuple().is_some());
                assert!(d.key_tuple().is_none());
                assert_eq!(d.rel_id(), 13);
            }
            _ => panic!("expected Delete"),
        }

        // Truncate with two rel_ids and both options unset
        let mut v = Vec::new();
        v.push(TRUNCATE_TAG);
        v.extend_from_slice(&be_i32(2));
        v.push(0);
        v.extend_from_slice(&be_u32(100));
        v.extend_from_slice(&be_u32(200));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &Cell::new(false)).unwrap();
        match msg {
            LogicalReplicationMessage::Truncate(t) => {
                assert_eq!(t.rel_ids(), &[100, 200]);
                assert_eq!(t.options(), 0);
            }
            _ => panic!("expected Truncate"),
        }
    }

    #[test]
    fn logical_parse_v2_stream_messages_and_state() {
        let in_stream = Cell::new(false);
        // StreamStart sets in_streamed_transaction = true
        let mut v = Vec::new();
        v.push(STREAM_START_TAG);
        v.extend_from_slice(&be_u32(777));
        v.push(1);
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 2, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::StreamStart(s) => {
                assert_eq!(s.xid(), 777);
                assert_eq!(s.is_first_segment(), 1);
                assert!(in_stream.get());
            }
            _ => panic!("expected StreamStart"),
        }

        // StreamStop sets in_streamed_transaction = false
        let v = Bytes::from_static(&[STREAM_STOP_TAG]);
        let msg = LogicalReplicationMessage::parse(&v, 2, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::StreamStop(_) => {
                assert!(!in_stream.get());
            }
            _ => panic!("expected StreamStop"),
        }

        // StreamCommit clears state
        let mut v = Vec::new();
        v.push(STREAM_COMMIT_TAG);
        v.extend_from_slice(&be_u32(1));
        v.push(0);
        v.extend_from_slice(&be_u64(10));
        v.extend_from_slice(&be_u64(11));
        v.extend_from_slice(&be_i64(12));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 2, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::StreamCommit(c) => {
                assert_eq!(c.xid(), 1);
                assert_eq!(c.flags(), 0);
                assert_eq!(c.commit_lsn(), 10);
                assert_eq!(c.end_lsn(), 11);
                assert_eq!(c.timestamp(), 12);
                assert!(!in_stream.get());
            }
            _ => panic!("expected StreamCommit"),
        }

        // StreamAbort clears state
        let mut v = Vec::new();
        v.push(STREAM_ABORT_TAG);
        v.extend_from_slice(&be_u32(5));
        v.extend_from_slice(&be_u32(6));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 2, &in_stream).unwrap();
        match msg {
            LogicalReplicationMessage::StreamAbort(a) => {
                assert_eq!(a.xid(), 5);
                assert_eq!(a.subxid(), 6);
                assert!(!in_stream.get());
            }
            _ => panic!("expected StreamAbort"),
        }
    }

    #[test]
    fn tuple_and_tupledata_parsing_edge_cases() {
        // Zero columns
        let mut v = Vec::new();
        v.push(INSERT_TAG);
        v.extend_from_slice(&be_u32(1));
        v.push(TUPLE_NEW_TAG);
        v.extend_from_slice(&be_i16(0));
        let msg = LogicalReplicationMessage::parse(&Bytes::from(v), 1, &Cell::new(false)).unwrap();
        match msg {
            LogicalReplicationMessage::Insert(i) => {
                assert_eq!(i.tuple().tuple_data().len(), 0);
            }
            _ => panic!("expected Insert"),
        }

        // TupleData variants
        let mut buf = Buffer {
            bytes: Bytes::from_static(&[TUPLE_DATA_NULL_TAG]),
        };
        assert!(matches!(
            TupleData::parse(&mut buf).unwrap(),
            TupleData::Null
        ));

        let mut buf = Buffer {
            bytes: Bytes::from_static(&[TUPLE_DATA_TOAST_TAG]),
        };
        assert!(matches!(
            TupleData::parse(&mut buf).unwrap(),
            TupleData::UnchangedToast
        ));

        let mut data = Vec::new();
        data.push(TUPLE_DATA_TEXT_TAG);
        data.extend_from_slice(&be_i32(3));
        data.extend_from_slice(b"abc");
        let mut buf = Buffer {
            bytes: Bytes::from(data),
        };
        match TupleData::parse(&mut buf).unwrap() {
            TupleData::Text(b) => assert_eq!(&b[..], b"abc"),
            _ => panic!(),
        }
    }

    #[test]
    fn get_str_valid_and_invalid_utf8() {
        assert_eq!(get_str(b"ok").unwrap(), "ok");
        let err = get_str(&[0xFF, 0xFE]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn logical_parse_unknown_tag() {
        let in_stream = Cell::new(false);
        let v = Bytes::from_static(b"Z");
        let err = LogicalReplicationMessage::parse(&v, 1, &in_stream).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
