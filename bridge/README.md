# mmbus-bridge

Cross-machine relay for [mmbus](..) topics.  A bridge process attaches
to a local mmbus, subscribes to a configured set of topics, and
forwards each message to peer bridges over the network.  Inbound
frames from peers are republished locally — so apps on the other end
see them on the same local `Bus` they would for a same-machine
publisher, with no changes to the local API.

See [`../docs/rfc-multi-machine.md`](../docs/rfc-multi-machine.md) for
the full design.  This crate is a separate workspace member so its
network + crypto dependencies don't bleed into the core mmbus build
matrix.

## Status

This is a **work in progress**.  Stages, in order:

| Stage | Scope | Status |
|-------|-------|--------|
| **B0** | Config parsing + wire-frame codec, no I/O | shipped |
| B1    | Local subscribe + TCP forward to one peer | next |
| B2    | Receive from peer + dedupe by `origin_id` + republish locally | |
| B3    | N-peer mesh + per-peer bounded buffers | |
| B4    | QUIC (quinn) transport behind a feature flag; preshared-key auth | |
| B5    | Python helper `mmbus.bridge.start(config_path)`; systemd unit | |

Today the binary loads + validates a TOML config and prints a summary,
no network traffic.  The frame codec is tested round-trip.

## Build + test

```bash
cd bridge
cargo test
cargo run -- sample-config.toml
```

The `[workspace]` block in `Cargo.toml` keeps the parent `cargo test`
out — work on the bridge here without retriggering the core mmbus
build.

## Config

See [`sample-config.toml`](sample-config.toml) for an annotated
example.  Required fields: `bus`.  Everything else has sensible
defaults; the validator rejects empty bus names, malformed endpoints,
and duplicate peer names.
