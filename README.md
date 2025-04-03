# Slimmed-down Rust-Postgres

This repository contains a slimmed-down version of the [rust-postgres] client with only the following crates:

- tokio-postgres: A native, asynchronous PostgreSQL client 
- postgres-replication: Protocol definitions for the Postgres logical replication protocol
- postgres-protocol: Low level Postgres protocol APIs
- postgres-types: Conversions between Rust and Postgres values (required dependency)
- postgres-derive: An internal crate used by postgres-types (required dependency)

All testing components related to these crates have been retained, including:
- postgres: The synchronous PostgreSQL client (kept for testing purposes)
- postgres-derive-test: Testing code for postgres-derive
- test: Shared test utilities and certificates
- docker: Docker setup for testing

## Original Repository

This is derived from the [rust-postgres] client by Steven Fackler. The original repository contains additional crates that have been removed in this version.

[rust-postgres]: https://github.com/sfackler/rust-postgres
