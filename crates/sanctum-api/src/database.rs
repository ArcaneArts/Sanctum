use std::time::SystemTime;

use sanctum_core::{Error, Result, UPLOAD_FRAME_BYTES};
use sanctum_proto::v1::{self, sql_value::Value};
use sanctum_service::{
    DatabaseTarget,
    database_snapshot::{self, Description, SqlValue},
};
use tokio::{io::AsyncReadExt, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status};
use uuid::Uuid;

use crate::{Backend, Reply, status, unix_ms, work};

pub(super) async fn open(
    backend: &Backend,
    cancellation: CancellationToken,
    request: v1::DatabaseTarget,
) -> Reply<v1::DatabaseSession> {
    let target = parse_target(request.clone()).map_err(status)?;
    let collection = target.collection.is_some();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    let details = match backend {
        Backend::Local(service) => {
            let service = service.clone();
            let session =
                work(move || service.open_database_session(target, || cancellation.is_cancelled()))
                    .await?;
            SessionDetails {
                id: session.id,
                expires_at: session.expires_at,
                bytes: session.bytes,
                snapshot_generation: None,
                description: &session.description,
            }
            .wire(request, collection)?
        }
        Backend::Hosted(catalog) => {
            let session = catalog
                .open_database_session(&target)
                .await
                .map_err(status)?;
            SessionDetails {
                id: session.id,
                expires_at: session.expires_at,
                bytes: session.bytes,
                snapshot_generation: Some(session.snapshot_generation),
                description: &session.description,
            }
            .wire(request, collection)?
        }
    };
    Ok(Response::new(details))
}

struct SessionDetails<'a> {
    id: Uuid,
    expires_at: SystemTime,
    bytes: u64,
    snapshot_generation: Option<Uuid>,
    description: &'a Description,
}

impl SessionDetails<'_> {
    #[allow(clippy::result_large_err)]
    fn wire(
        self,
        request: v1::DatabaseTarget,
        collection: bool,
    ) -> std::result::Result<v1::DatabaseSession, Status> {
        Ok(v1::DatabaseSession {
            session: Some(v1::DatabaseSessionRef {
                target: Some(request),
                generation: self.id.to_string(),
            }),
            expires_at_unix_ms: unix_ms(self.expires_at).map_err(status)?,
            bytes: self.bytes,
            schema_version: self.description.schema_version as u32,
            sqlite_version: self.description.sqlite_version.into(),
            schema: self
                .description
                .schema
                .iter()
                .map(|object| v1::DatabaseSchemaObject {
                    kind: object.kind.clone(),
                    name: object.name.clone(),
                    table: object.table.clone(),
                    sql: object.sql.clone(),
                })
                .collect(),
            sqlite_features: if collection {
                vec!["FTS5".into()]
            } else {
                Vec::new()
            },
            snapshot_generation: self
                .snapshot_generation
                .map(|generation| generation.to_string()),
        })
    }
}

pub(super) async fn query(
    backend: &Backend,
    cancellation: CancellationToken,
    request: v1::QueryDatabaseRequest,
) -> Reply<v1::QueryDatabaseResponse> {
    let (target, generation) = parse_session(request.session).map_err(status)?;
    let parameters = request
        .parameters
        .into_iter()
        .map(parse_value)
        .collect::<Result<Vec<_>>>()
        .map_err(status)?;
    let _cancel_on_drop = cancellation.clone().drop_guard();
    let limit = request.limit.unwrap_or(25);
    let page = match backend {
        Backend::Local(service) => {
            let service = service.clone();
            work(move || {
                let session = service.database_session(&target, generation)?;
                session.query(&request.sql, parameters, request.offset, limit, move || {
                    cancellation.is_cancelled()
                })
            })
            .await?
        }
        Backend::Hosted(catalog) => {
            let session = catalog
                .database_session(&target, generation)
                .await
                .map_err(status)?;
            work(move || {
                database_snapshot::query(
                    session.path(),
                    &request.sql,
                    parameters,
                    request.offset,
                    limit,
                    move || cancellation.is_cancelled(),
                )
            })
            .await?
        }
    };
    Ok(Response::new(v1::QueryDatabaseResponse {
        columns: page.columns,
        rows: page
            .rows
            .into_iter()
            .map(|row| v1::SqlRow {
                values: row.into_iter().map(wire_value).collect(),
            })
            .collect(),
        next_offset: page.next_offset,
    }))
}

pub(super) async fn download(
    backend: &Backend,
    shutdown: CancellationToken,
    request: v1::DatabaseSessionRef,
) -> Reply<ReceiverStream<std::result::Result<v1::DatabaseFrame, Status>>> {
    let (target, generation) = parse_session(Some(request)).map_err(status)?;
    let (session, file): (Box<dyn Send>, _) = match backend {
        Backend::Local(service) => {
            let service = service.clone();
            let (session, file) = work(move || {
                let session = service.database_session(&target, generation)?;
                let file = session.reopen()?;
                Ok((session, file))
            })
            .await?;
            (Box::new(session), file)
        }
        Backend::Hosted(catalog) => {
            let session = catalog
                .database_session(&target, generation)
                .await
                .map_err(status)?;
            let file = session.reopen().map_err(status)?;
            (Box::new(session), file)
        }
    };
    let (sender, receiver) = mpsc::channel(2);
    tokio::spawn(async move {
        // Retain the snapshot until the stream is done, including on Windows.
        let _session = session;
        let stream = async {
            let mut file = tokio::fs::File::from_std(file);
            let mut buffer = vec![0; UPLOAD_FRAME_BYTES];
            loop {
                let Ok(permit) = sender.reserve().await else {
                    break;
                };
                match file.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(bytes) => permit.send(Ok(v1::DatabaseFrame {
                        data: buffer[..bytes].to_vec(),
                    })),
                    Err(error) => {
                        permit.send(Err(status(error.into())));
                        break;
                    }
                }
            }
        };
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = stream => {}
        }
    });
    Ok(Response::new(ReceiverStream::new(receiver)))
}

pub(super) async fn close(backend: &Backend, request: v1::DatabaseSessionRef) -> Reply<v1::Empty> {
    let (target, generation) = parse_session(Some(request)).map_err(status)?;
    match backend {
        Backend::Local(service) => {
            let service = service.clone();
            work(move || service.close_database_session(&target, generation)).await?;
        }
        Backend::Hosted(catalog) => catalog
            .close_database_session(&target, generation)
            .await
            .map_err(status)?,
    }
    Ok(Response::new(v1::Empty {}))
}

fn parse_session(session: Option<v1::DatabaseSessionRef>) -> Result<(DatabaseTarget, Uuid)> {
    let session = session.ok_or_else(|| Error::Invalid("database session is required".into()))?;
    let target = session
        .target
        .ok_or_else(|| Error::Invalid("database target is required".into()))?;
    let generation = session
        .generation
        .parse()
        .map_err(|_| Error::Invalid("invalid database generation".into()))?;
    Ok((parse_target(target)?, generation))
}

pub(super) fn parse_target(target: v1::DatabaseTarget) -> Result<DatabaseTarget> {
    Ok(DatabaseTarget {
        tenant: target.tenant.parse()?,
        collection: target.collection.map(|value| value.parse()).transpose()?,
    })
}

fn parse_value(value: v1::SqlValue) -> Result<SqlValue> {
    match value.value {
        Some(Value::Null(_)) => Ok(SqlValue::Null),
        Some(Value::Integer(value)) => Ok(SqlValue::Integer(value)),
        Some(Value::Real(value)) if value.is_finite() => Ok(SqlValue::Real(value)),
        Some(Value::Text(value)) => Ok(SqlValue::Text(value)),
        Some(Value::Blob(value)) => Ok(SqlValue::Blob(value)),
        _ => Err(Error::Invalid(
            "SQL parameters require a typed value and finite real numbers".into(),
        )),
    }
}

fn wire_value(value: SqlValue) -> v1::SqlValue {
    v1::SqlValue {
        value: Some(match value {
            SqlValue::Null => Value::Null(v1::Empty {}),
            SqlValue::Integer(value) => Value::Integer(value),
            SqlValue::Real(value) => Value::Real(value),
            SqlValue::Text(value) => Value::Text(value),
            SqlValue::Blob(value) => Value::Blob(value),
        }),
    }
}
