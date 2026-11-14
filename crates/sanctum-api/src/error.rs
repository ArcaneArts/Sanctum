use prost::Message;
use prost_types::Any;
use sanctum_core::Error;
use sanctum_proto::v1::RpcError;
use tonic::{
    Code, Status,
    metadata::{MetadataMap, MetadataValue},
};
use uuid::Uuid;

pub(crate) fn status(error: Error) -> Status {
    from_error(error, None)
}

pub(crate) fn from_error(error: Error, operation: Option<Uuid>) -> Status {
    let (code, reason, message) = match error {
        Error::Invalid(message) => (Code::InvalidArgument, "invalid_argument", message),
        Error::NotFound(message) => (Code::NotFound, "not_found", message),
        Error::Conflict(message) => (Code::AlreadyExists, "conflict", message),
        Error::StaleRevision(message) => (Code::FailedPrecondition, "stale_revision", message),
        Error::Busy(message) => (Code::Unavailable, "unavailable", message),
        Error::Io(_) | Error::Internal(_) => {
            tracing::error!(%error, "application operation failed");
            (
                Code::Internal,
                "internal",
                "storage or database operation failed".into(),
            )
        }
    };
    detail(
        code,
        message,
        reason,
        code == Code::Unavailable,
        operation,
        None,
    )
}

pub(crate) fn invalid_argument(field: &str, message: impl Into<String>) -> Status {
    detail(
        Code::InvalidArgument,
        message,
        "invalid_argument",
        false,
        None,
        Some(field),
    )
}

pub(crate) fn upload_limit() -> Status {
    detail(
        Code::ResourceExhausted,
        "source exceeds the configured upload limit",
        "source_too_large",
        false,
        None,
        Some("data"),
    )
}

pub(crate) fn operation_status(
    code: Code,
    message: impl Into<String>,
    reason: &str,
    retryable: bool,
    operation: Uuid,
) -> Status {
    detail(code, message, reason, retryable, Some(operation), None)
}

fn detail(
    code: Code,
    message: impl Into<String>,
    reason: &str,
    retryable: bool,
    operation: Option<Uuid>,
    field: Option<&str>,
) -> Status {
    let mut metadata = MetadataMap::new();
    if let Some(operation) = operation {
        metadata.insert_bin(
            "sanctum-operation-id-bin",
            MetadataValue::from_bytes(operation.as_bytes()),
        );
    }
    let detail = RpcError {
        reason: reason.into(),
        retryable,
        operation_id: operation.map(|id| id.to_string()),
        field: field.map(str::to_owned),
    };
    let message = message.into();
    let envelope = tonic_types::Status {
        code: code as i32,
        message: message.clone(),
        details: vec![Any {
            type_url: "type.googleapis.com/sanctum.v1.RpcError".into(),
            value: detail.encode_to_vec(),
        }],
    };
    Status::with_details_and_metadata(code, message, envelope.encode_to_vec().into(), metadata)
}
