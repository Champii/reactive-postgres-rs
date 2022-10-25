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
    let cb = |events| {
        for event in events {
            match event {
                Event::Insert(row) => println!("insert: {:?}", row),
                Event::Update(row) => println!("change: {:?}", row),
                Event::Delete(id) => println!("delete: {:?}", id),
            }
        }
    };

    let (_, join_handle) =
        watch::<Foo>(
	    "SELECT * from foo where id < 10",
	    Box::new(cb),
	).await;

    join_handle.await.unwrap();
}
```
