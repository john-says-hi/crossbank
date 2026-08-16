# crossbank

Cross-platform persistent key/value storage for Rust. One API on native and in the browser.

> **Status: pre-alpha, milestone zero.** Nothing works yet. The crate is an empty scaffold
> while the test lanes are being proven. Do not depend on this.

## Why

Rust has no mature library that persists data well on *both* native and the web. The pieces
all exist separately — `redb`, `sled` and `fjall` are native-only; `idb`, `indexed-db` and
`indxdb` are the browser half only. The closest thing that spans both is
[`bevy_pkv`](https://github.com/johanhelsing/bevy_pkv), and its author names the gap in his
own README:

> *"Perhaps IndexedDb and something else would have been a better choice, but its API is
> complicated, and I wanted a simple implementation and a simple synchronous API."*

That choice caps it at `localStorage`'s ~5–10 MB. crossbank takes the other fork: an
async-first API so the browser backend can be real IndexedDB, with room for hundreds of
megabytes.

The design is modelled on [Hive](https://github.com/IO-Design-Team/hive_ce), the Flutter
key/value store — its architecture and ergonomics, not its file format.

## Design

- **Two container types.** An eager `Locker` keeps values in RAM for synchronous reads; a
  `LazyLocker` keeps only the key index and fetches values on demand.
- **serde-typed values** with a pluggable codec. `Vec<u8>` works as a value type.
- **Big values are handled.** Transparent auto-chunking, plus a streaming reader/writer so a
  multi-gigabyte value never has to exist in memory at once.
- **Ordered string keys** with prefix, range, reverse and limit scans.
- **Transactions** scoped to a container: commit or roll back as a unit.
- **Watch streams** at container and key level.
- **Pluggable encryption** via a `Cipher` trait. No crypto is bundled.
- **Quota-aware.** Requests persistent storage, exposes usage, and can shed least-recently-used
  entries from containers you mark disposable.

### Backends

| Backend | Target |
|---|---|
| memory | all |
| [`redb`](https://github.com/cberner/redb) | Linux, macOS, Windows, Android, iOS |
| IndexedDB | `wasm32-unknown-unknown` |

No async runtime dependency — `futures` only. The library spawns nothing itself, and works
under threaded/shared-memory wasm builds.

## Testing

Every backend must pass one shared conformance suite. If a behaviour is not in the suite, it
is not a guaranteed behaviour.

```sh
cargo nextest run                              # native, all backends
wasm-pack test --headless --firefox            # IndexedDB in a real browser
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
