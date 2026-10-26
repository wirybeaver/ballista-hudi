# ballista-hudi

An independent, read-only Apache Hudi integration for Apache DataFusion Ballista.
The crate owns Hudi provider serialization and reconstruction through Ballista's
existing logical codec extension point. It requires no Ballista modifications.

Initial scope: local filesystem Hudi copy-on-write (COW) tables with Parquet base
files, projection, filtering, and distributed standalone execution. MOR execution,
writes, dynamic plugins, and remote cluster validation are outside this scope.

## Compatibility

- Ballista 55.0.0; DataFusion 55.2; Arrow 59.x (59.3.1 in the lockfile).
- Rust 1.94.1 or newer and a **multi-thread Tokio runtime**.
- Hudi is pinned to official `apache/hudi-rs` commit
  `017f0bb278ab882898dd78cd0b25a0a9db1c979e`, the merge of
  [hudi-rs #698](https://github.com/apache/hudi-rs/pull/698).
  Release 0.5.0 predates this compatibility work. Publishing is disabled until
  an official compatible Hudi release is available.
- Hudi's optional RocksDB spill tier is disabled. This does not remove Hudi's
  other dependencies, including its Lance reader.

`Cargo.lock` pins the development and CI dependency graph. Native dependencies
need a C/C++ toolchain, CMake, and Protocol Buffers (`protoc`). On macOS these can
be installed with `brew install cmake protobuf` alongside Xcode command line tools.

## Standalone example

Add this crate as a path dependency and Ballista with its `standalone` feature.
The application also needs DataFusion 55.2 and Tokio with `macros` and
`rt-multi-thread` enabled.

```rust,no_run
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_hudi::{register_hudi_codec, register_hudi_table};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::common::Result<()> {
    let config = register_hudi_codec(
        SessionConfig::new_with_ballista()
            .with_target_partitions(2)
            .with_ballista_standalone_parallelism(2),
    );
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::standalone_with_state(state).await?;
    register_hudi_table(
        &ctx,
        "users",
        "file:///absolute/path/to/hudi-cow-table",
        std::iter::empty::<(&str, &str)>(),
    )
    .await?;
    ctx.sql("SELECT id, name FROM users WHERE \"isActive\" ORDER BY id")
        .await?
        .show()
        .await?;
    Ok(())
}
```

`register_hudi_codec` replaces the session's logical extension codec. It handles
`HudiDataSource` and delegates other providers, extension nodes, and file formats
to `BallistaLogicalExtensionCodec`. Applications composing additional codecs can
use the public `HudiLogicalExtensionCodec` in their own wrapper.

The wire recipe uses the versioned prefix `ballista-contrib-hudi-v1\0` followed
by protobuf base URI and sorted options. Reconstruction opens the table again;
it does not pin a snapshot or copy table data. Tables must remain stable during
a query. Options are serialized into plans, so apply the same access controls
to plans as to storage credentials supplied through those options.

Provider decoding is synchronous in DataFusion. This crate uses
`tokio::task::block_in_place` to reconstruct Hudi asynchronously and consequently
requires a multi-thread runtime wherever decoding occurs.

## Remote deployments

Remote clusters have not been validated. A custom client, scheduler, and executor
must embed the crate and configure the codec in every process that encodes or
decodes a provider. Installing it only in a client does not configure stock remote
Ballista binaries. Each process also needs access to the same table URI and
storage configuration.

## Development

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The tests use the pinned upstream Hudi COW fixture on local disk, exercise a
provider round trip, and query through an embedded scheduler with two independent
in-process executors (one task slot each). The convenience example above uses
Ballista's standalone helper, which starts one executor with two task slots.

Design source: [standalone integration handoff](https://gist.github.com/wirybeaver/e5cfdd54188965e177a9e2463ad2560b).
Licensed under Apache-2.0; see LICENSE and NOTICE.
