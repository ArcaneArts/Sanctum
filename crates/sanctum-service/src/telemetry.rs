use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sanctum_core::{Scope, ScopeId};
use sanctum_embedding::{EmbedError, Provider, Purpose};
use uuid::Uuid;

use crate::{SearchMode, embedding::Coverage};

pub(crate) fn queue_age(created_at: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(created_at)
}

pub(crate) fn ingestion_stage(
    scope: &Scope,
    operation: Uuid,
    stage: &'static str,
    created_at: u64,
    input_bytes: u64,
    elapsed: Duration,
    outcome: &'static str,
) {
    tracing::debug!(
        event = "ingestion_stage",
        tenant = %scope.tenant,
        collection = %scope.collection,
        %operation,
        stage,
        outcome,
        input_bytes,
        operation_age_seconds = queue_age(created_at),
        duration_ms = elapsed.as_secs_f64() * 1_000.0,
        "completed an ingestion stage"
    );
}

pub(crate) fn vector_reuse(scope: &Scope, operation: Uuid, chunks: u64, reused: u64) {
    tracing::debug!(
        event = "vector_reuse",
        tenant = %scope.tenant,
        collection = %scope.collection,
        %operation,
        chunks,
        reused,
        "measured vector reuse at lexical publication"
    );
}

pub(crate) async fn embed(
    tenant: &ScopeId,
    collection: Option<&ScopeId>,
    provider: &dyn Provider,
    purpose: Purpose,
    texts: &[String],
) -> Result<Vec<Vec<f32>>, EmbedError> {
    let start = Instant::now();
    let result = sanctum_embedding::embed(provider, purpose, texts).await;
    let outcome = match &result {
        Ok(_) => "ok",
        Err(EmbedError::Transient(_)) => "transient",
        Err(EmbedError::Permanent(_)) => "permanent",
        Err(EmbedError::Input(_)) => "invalid_input",
    };
    let purpose = match purpose {
        Purpose::Document => "document",
        Purpose::Query => "query",
    };
    tracing::debug!(
        event = "embedding_request",
        tenant = %tenant,
        collection = collection.map(ScopeId::as_str),
        profile = provider.profile_id(),
        purpose,
        outcome,
        inputs = texts.len(),
        duration_ms = start.elapsed().as_secs_f64() * 1_000.0,
        "completed an embedding request"
    );
    result
}

pub(crate) fn search(
    tenant: &ScopeId,
    collection: Option<&ScopeId>,
    elapsed: Duration,
    mode: SearchMode,
    coverage: Coverage,
    hits: usize,
    degraded: Option<&str>,
) {
    let mode = match mode {
        SearchMode::Lexical => "lexical",
        SearchMode::Semantic => "semantic",
        SearchMode::Hybrid => "hybrid",
    };
    tracing::debug!(
        event = "search",
        tenant = %tenant,
        collection = collection.map(ScopeId::as_str),
        mode,
        duration_ms = elapsed.as_secs_f64() * 1_000.0,
        hits,
        chunks = coverage.total,
        embedded_chunks = coverage.embedded,
        embedding_errors = coverage.failed,
        degraded,
        "completed a search"
    );
}
