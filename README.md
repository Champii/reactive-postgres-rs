# Reactive PostgreSQL for Rust

Watch your queries results change as new rows are inserted/updated/deleted

Work in progress

This little example connects to localhost as user postgres. This will be configurable.

```rust
use reactive_pg::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Foo {
    id: i32,
    name: String,
}

#[tokio::main]
async fn main() {
    "SELECT * from foo where id < 10"
        .watch::<Foo>(|event| println!("{:#?}", event))
        .await
	.await // <- The second await will block until the connection is dropped
	.unwrap();
}
```
