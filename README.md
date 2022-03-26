# ChiselStore (Powered by Omni-paxos)

[![Rust](https://github.com/chiselstrike/chiselstore/actions/workflows/rust.yml/badge.svg)](https://github.com/chiselstrike/chiselstore/actions/workflows/rust.yml)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

A forked verision of [ChiselStore](https://github.com/chiselstrike/chiselstore). ChiselStore is an embeddable, distributed [SQLite](https://www.sqlite.org/index.html) for Rust. This verision is powered by [OmniPaxos](https://github.com/haraldng/omnipaxos), an in-development replicated log library that provides better resilience and reconfiguration compared to Raft.

SQLite is a fast and compact relational database management system, but it is limited to single-node configurations.
ChiselStore extends SQLite to run on a cluster of machines with the [OmniPaxos consensus algorithm](https://haraldng.github.io/omnipaxos/foreword.html).
With ChiselStore, you get the benefits of easy-to-use, embeddable SQLite but with OmniPaxos's high availability and fault tolerance.

For more information, check out the following [blog post](https://glaubercosta-11125.medium.com/winds-of-change-in-web-data-728187331f53).

## Features

* SQLite with OmniPaxos's high availability and fault tolerance
* Strong consistency with optional relaxed reads
* Embeddable Rust library

### Roadmap

* Efficient node restarts (with OmniPaxos snapshots)
* Dynamic cluster membership (with OmniPaxos consensus)
* Support executing non-deterministic SQL functions

## Getting Started

See the [example server](examples) of how to use the ChiselStore library.

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in ChiselStore by you, shall be licensed as MIT, without any additional
terms or conditions.
