use sanctum_core::{Error, Result, UPLOAD_FRAME_BYTES};
use sanctum_proto::v1::{self, sql_value::Value};
use sanctum_service::{DatabaseTarget, Service, database_snapshot::SqlValue};
use tokio::{io::AsyncReadExt, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status};
use uuid::Uuid;

use crate::{Reply, status, unix_ms, work};

pub(super) async fn open(
    service: &Service,
    cancellation: CancellationToken,
    request: v1::DatabaseTarget,
) -> Reply<v1::DatabaseSession> {
    let target = parse_target(request.clone()).map_err(status)?;
    let collection = target.collection.is_some();
    let service = service.clone();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    let session =
        work(move || service.open_database_session(target, || cancellation.is_cancelled())).await?;
    Ok(Response::new(v1::DatabaseSession {
        session: Some(v1::DatabaseSessionRef {
            target: Some(request),
            generation: session.id.to_string(),
        }),
        expires_at_unix_ms: unix_ms(session.expires_at).map_err(status)?,
        bytes: session.bytes,
        schema_version: session.description.schema_version as u32,
        sqlite_version: session.description.sqlite_version.into(),
        schema: session
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
    }))
}

pub(super) async fn query(
    service: &Service,
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
    let service = service.clone();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    let page = work(move || {
        let session = service.database_session(&target, generation)?;
        session.query(
            &request.sql,
            parameters,
            request.offset,
            request.limit.unwrap_or(25),
            move || cancellation.is_cancelled(),
        )
    })
    .await?;
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
    service: &Service,
    shutdown: CancellationToken,
    request: v1::DatabaseSessionRef,
) -> Reply<ReceiverStream<std::result::Result<v1::DatabaseFrame, Status>>> {
    let (target, generation) = parse_session(Some(request)).map_err(status)?;
    let service = service.clone();
    let (session, file) = work(move || {
        let session = service.database_session(&target, generation)?;
        let file = session.reopen()?;
        Ok((session, file))
    })
    .await?;
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

pub(super) async fn close(service: &Service, request: v1::DatabaseSessionRef) -> Reply<v1::Empty> {
    let (target, generation) = parse_session(Some(request)).map_err(status)?;
    let service = service.clone();
    work(move || service.close_database_session(&target, generation)).await?;
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

fn parse_target(target: v1::DatabaseTarget) -> Result<DatabaseTarget> {
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
