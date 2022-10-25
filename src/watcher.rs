use async_trait::async_trait;
use derive_new::new;
use futures::StreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};
use tokio::{sync::RwLock, task::JoinHandle};

use postgres_query::FromSqlRow;
use tokio_postgres::{
    tls::NoTlsStream, types::Json, AsyncMessage, Client, Connection, NoTls, Socket,
};

#[derive(Default)]
pub struct TmpTable {
    pub name: String,
    pub fields: Vec<String>,
}

#[derive(new)]
pub struct Watcher<T> {
    ctx: Arc<RwLock<WatcherCtx<T>>>,
}

#[derive(Serialize, Deserialize, Debug, FromSqlRow)]
pub struct JsonEvent {
    pub id: i32,
    pub op: i32,
    pub data: serde_json::Value,
}

impl<T> Watcher<T>
where
    T: Sync + Send + 'static + DeserializeOwned,
{
    pub async fn start(
        &mut self,
        mut connection: Connection<Socket, NoTlsStream>,
    ) -> JoinHandle<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        let handle = tokio::spawn(async move {
            let mut stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));

            while let Some(message) = stream.next().await {
                let message = message
                    .map_err(|e| {
                        eprintln!("failed to get message from db: {}", e);
                        e
                    })
                    .unwrap();

                if let AsyncMessage::Notification(not) = &message {
                    let updated_table_nb: u32 = serde_json::from_str(not.payload()).unwrap();

                    tx.send(updated_table_nb)
                        .await
                        .map_err(|e| {
                            eprintln!("failed to send message on channel: {:?}", e);
                            e
                        })
                        .unwrap();
                }
            }
        });

        let ctx = Arc::clone(&self.ctx);

        tokio::spawn(async move {
            while rx.recv().await.is_some() {
                ctx.write().await.handle_event().await;
            }
        });

        self.ctx.write().await.start().await;

        handle
    }
}

#[derive(new)]
pub struct WatcherCtx<T> {
    cb: Box<dyn Fn(Vec<Event<T>>) + Sync + Send + 'static>,
    query: String,
    pub client: Client,

    #[new(default)]
    triggers: Vec<String>,

    #[new(default)]
    result_table: TmpTable,

    #[new(default)]
    source_tables: Vec<String>,

    phantom: std::marker::PhantomData<T>,
}

impl<T> WatcherCtx<T>
where
    T: Sync + Send + 'static + DeserializeOwned,
{
    pub async fn start(&mut self) {
        self.client
            .query("LISTEN __live_update;", &[])
            .await
            .unwrap();

        self.setup_query_result_table().await;
        self.get_query_result_columns().await;
        self.collect_source_tables();
        self.create_triggers().await;
    }

    pub async fn get_query_result_columns(&self) -> Vec<(String, String)> {
        let query = format!("SELECT * FROM ({}) AS tamere LIMIT 1", self.query);

        let columns = self
            .client
            .query(&query, &[])
            .await
            .unwrap()
            .get(0)
            .unwrap()
            .columns()
            .iter()
            .map(|c| (c.name().to_string(), c.type_().name().to_string()))
            .collect();

        columns
    }

    pub fn collect_source_tables(&mut self) {
        use sqlparser::dialect::PostgreSqlDialect;
        let sql_ast =
            sqlparser::parser::Parser::parse_sql(&PostgreSqlDialect {}, &self.query).unwrap();
        use sqlparser::ast::{SetExpr, Statement, TableFactor};
        let mut names = Vec::new();

        for stmt in sql_ast {
            match stmt {
                Statement::Query(query) => match &*query.body {
                    SetExpr::Select(select) => {
                        for table in &select.from {
                            match &table.relation {
                                TableFactor::Table {
                                    name,
                                    alias: _,
                                    args: _,
                                    with_hints: _,
                                } => names.push(name.to_string()),
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        self.source_tables = names;
    }

    pub async fn setup_query_result_table(&mut self) {
        let tmp_table_name = "query_result";

        let columns = self.get_query_result_columns().await;

        let fields = columns.iter().map(|(name, _)| name.clone()).collect();

        let columns_def = columns
            .iter()
            .map(|(name, t)| format!("{} {} NOT NULL", name, t))
            .collect::<Vec<_>>()
            .join(",\n");

        let query = format!(
            r#"
			CREATE TEMP TABLE {} (
                {}
			)
		"#,
            tmp_table_name, columns_def,
        );

        self.client.execute(&query, &[]).await.unwrap();

        self.result_table = TmpTable {
            name: tmp_table_name.to_string(),
            fields,
        };
    }

    pub async fn create_triggers(&mut self) {
        for (i, table_name) in self.source_tables.iter().enumerate() {
            if !self.triggers.contains(&table_name) {
                let trigger_name = &format!(r#""__live_update{}""#, table_name);
                let l_key = i.to_string();

                let drop_sql = format!(
                    r#"
					DROP TRIGGER IF EXISTS
						{}
					ON
						{}
				"#,
                    trigger_name, table_name
                );

                self.client.execute(&drop_sql, &[]).await.unwrap();

                let func_sql = format!(
                    r#"
					CREATE OR REPLACE FUNCTION pg_temp.{}()
					RETURNS TRIGGER AS $$
						BEGIN
							EXECUTE pg_notify('__live_update', '{}');
						RETURN NULL;
						END;
					$$ LANGUAGE plpgsql
				"#,
                    trigger_name, l_key
                );

                self.client.execute(&func_sql, &[]).await.unwrap();

                let create_sql = format!(
                    "
					CREATE TRIGGER
						{}
					AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON
						{}
					EXECUTE PROCEDURE pg_temp.{}()
				",
                    trigger_name, table_name, trigger_name
                );

                self.client.execute(&create_sql, &[]).await.unwrap();

                self.triggers.push(table_name.clone());
            }
        }
    }

    pub async fn update_result_table(&mut self) {
        let i_table = "query_result";

        let q_obj = self
            .result_table
            .fields
            .iter()
            .map(|name| format!("'{name}', i.{name}"))
            .collect::<Vec<_>>()
            .join(",");

        let cols = self.result_table.fields.join(", ");
        let update_sql = format!(
            "WITH
				q AS (
					SELECT
						*,
						ROW_NUMBER() OVER() AS lol
					FROM
						({}) t
				),
				i AS (
					INSERT INTO {i_table} (
                        {cols}
					)
					SELECT
{cols}
					FROM
						q
					WHERE q.id NOT IN (
                        SELECT id FROM {i_table}
                    )
					RETURNING
{i_table}.*
				)
			SELECT
				jsonb_build_object(
					'id', i.id,
					'op', 1,
					'data', jsonb_build_object(
                        {q_obj}
                    )
				) AS c
			FROM
				i JOIN
				q ON
					i.id = q.id
		",
            self.query
        );

        let res: Vec<_> = self
            .client
            .query(&update_sql, &[])
            .await
            .unwrap_or_else(|err| {
                panic!("error {}", err);
            });

        let res = res
            .into_iter()
            .map(|row| row.get("c"))
            .collect::<Vec<Json<serde_json::Value>>>();

        let json_value = res.iter().map(|json| json.0.clone()).collect::<Vec<_>>();
        let json_events = json_value
            .into_iter()
            .map(|json| serde_json::from_value(json).unwrap())
            .collect::<Vec<JsonEvent>>();

        let events = json_events
            .into_iter()
            .map(|event| {
                let data = serde_json::from_value(event.data).unwrap();
                match event.op {
                    1 => Event::Insert(data),
                    _ => unimplemented!(),
                }
            })
            .collect::<Vec<Event<T>>>();

        (self.cb)(events);
    }

    pub async fn handle_event(&mut self) {
        self.update_result_table().await;
    }
}

pub async fn watch<T>(
    query: &str,
    handler: Box<dyn Fn(Vec<Event<T>>) + Sync + Send + 'static>,
) -> JoinHandle<()>
where
    T: Debug + Send + Sync + 'static + DeserializeOwned,
{
    let (client, connection) = tokio_postgres::connect("host=localhost user=postgres", NoTls)
        .await
        .unwrap();

    let mut watcher = Watcher::new(Arc::new(RwLock::new(WatcherCtx::new(
        handler,
        query.to_string(),
        client,
    ))));

    watcher.start(connection).await
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Event<T> {
    Insert(T),
    Update(T),
    Delete(i32),
}

#[async_trait]
pub trait WatchableSql<F> {
    async fn watch<T>(&self, handler: F) -> JoinHandle<()>
    where
        F: Fn(Vec<Event<T>>) + Sync + Send + 'static,
        T: Debug + Send + Sync + 'static + DeserializeOwned;
}

#[async_trait]
impl<F> WatchableSql<F> for str {
    async fn watch<T>(&self, handler: F) -> JoinHandle<()>
    where
        F: Fn(Vec<Event<T>>) + Sync + Send + 'static,
        T: Debug + Send + Sync + 'static + DeserializeOwned,
    {
        watch(self, Box::new(handler)).await
    }
}
