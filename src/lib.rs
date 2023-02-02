mod watcher;
pub use watcher::{watch, Event, WatchableSql};

/* #[cfg(test)]
mod tests {
    use crate::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Foo {
        x: i32,
        y: i32,
    }

    #[tokio::test]
    async fn it_works() {
        let handler = watch::<Foo>(
            "SELECT * from foo",
            Box::new(move |events| {
                for event in events {
                    match event {
                        Event::Insert(row) => println!("insert: {:?}", row),
                        Event::Update(row) => println!("change: {:?}", row),
                        Event::Delete(id) => println!("delete: {:?}", id),
                    }
                }
            }),
        )
        .await;

        drop(handler);
    }
} */
