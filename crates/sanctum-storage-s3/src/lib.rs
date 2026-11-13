use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::{
    Attribute, ClientOptions, GetOptions, GetRange, ObjectMeta, ObjectStore, PutMode, PutResult,
    UpdateVersion,
    aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut},
    list::{PaginatedListOptions, PaginatedListStore},
    path::Path,
};
use sanctum_core::{ByteRange, Error, Result};
use sanctum_storage::{
    Condition, Download, Key, ListPage, MAX_CONTROL_BYTES, Object, Reader, Storage, Version,
    list_limit, range_len,
};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

const PART_BYTES: usize = 8 * 1024 * 1024;

pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub allow_http: bool,
    pub virtual_hosted_style: bool,
}

pub struct S3Storage(AmazonS3);

impl S3Storage {
    /// Uses the AWS credential chain supported by the S3 client.
    pub fn new(config: S3Config) -> Result<Self> {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(config.bucket)
            .with_region(config.region)
            .with_virtual_hosted_style_request(config.virtual_hosted_style)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(config.allow_http)
                    .with_connect_timeout(Duration::from_secs(10))
                    .with_timeout(Duration::from_secs(60)),
            );
        if let Some(endpoint) = config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        builder.build().map(Self).map_err(storage_error)
    }
}

#[async_trait::async_trait]
impl Storage for S3Storage {
    async fn head(&self, key: &Key) -> Result<Object> {
        object(self.0.head(&path(key)).await.map_err(storage_error)?)
    }

    async fn get(&self, key: &Key) -> Result<Download> {
        let response = self.0.get(&path(key)).await.map_err(storage_error)?;
        require_identity(&response)?;
        let object = object(response.meta.clone())?;
        let stream = response.into_stream().map_err(std::io::Error::other);
        Ok(Download {
            object,
            reader: Box::pin(StreamReader::new(stream)),
        })
    }

    async fn read_range(&self, key: &Key, range: ByteRange) -> Result<Bytes> {
        let size = range_len(range)?;
        let requested = range.start..range.end;
        let response = self
            .0
            .get_opts(
                &path(key),
                GetOptions {
                    range: Some(GetRange::Bounded(requested.clone())),
                    ..Default::default()
                },
            )
            .await
            .map_err(storage_error)?;
        require_identity(&response)?;
        if response.range != requested {
            return Err(invalid_response("S3 returned a different byte range"));
        }
        let mut output = BytesMut::with_capacity(size);
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(storage_error)? {
            if chunk.len() > size - output.len() {
                return Err(invalid_response("S3 returned more bytes than requested"));
            }
            output.extend_from_slice(&chunk);
        }
        if output.len() != size {
            return Err(invalid_response("S3 returned fewer bytes than requested"));
        }
        tracing::debug!(
            requested_bytes = size,
            transferred_bytes = output.len(),
            "S3 range read"
        );
        Ok(output.freeze())
    }

    async fn put_stream(&self, key: &Key, mut reader: Reader, length: u64) -> Result<Version> {
        let path = path(key);
        if length <= PART_BYTES as u64 {
            let mut buffer = vec![0; length as usize];
            reader.read_exact(&mut buffer).await?;
            require_eof(&mut reader).await?;
            return self
                .0
                .put(&path, buffer.into())
                .await
                .map(version)
                .map_err(storage_error);
        }
        let mut upload = self.0.put_multipart(&path).await.map_err(storage_error)?;
        let result = async {
            let mut remaining = length;
            while remaining > 0 {
                let bytes = remaining.min(PART_BYTES as u64) as usize;
                let mut buffer = vec![0; bytes];
                reader.read_exact(&mut buffer).await?;
                upload
                    .put_part(buffer.into())
                    .await
                    .map_err(storage_error)?;
                remaining -= bytes as u64;
            }
            require_eof(&mut reader).await?;
            upload.complete().await.map(version).map_err(storage_error)
        }
        .await;
        if result.is_err()
            && let Err(error) = upload.abort().await
        {
            tracing::warn!(%error, "could not abort multipart upload");
        }
        result
    }

    async fn put_control(&self, key: &Key, bytes: Bytes, condition: Condition) -> Result<Version> {
        if bytes.len() > MAX_CONTROL_BYTES {
            return Err(Error::Invalid(
                "storage control object exceeds 1 MiB".into(),
            ));
        }
        let mode = match condition {
            Condition::Create => PutMode::Create,
            Condition::Match(expected) => PutMode::Update(UpdateVersion {
                e_tag: Some(expected.etag.ok_or_else(|| {
                    Error::Invalid("S3 conditional replacement requires an ETag".into())
                })?),
                version: expected.generation,
            }),
        };
        self.0
            .put_opts(&path(key), bytes.into(), mode.into())
            .await
            .map(version)
            .map_err(storage_error)
    }

    async fn list(
        &self,
        prefix: Option<&Key>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ListPage> {
        let prefix = prefix.map(|prefix| format!("{prefix}/"));
        let page = self
            .0
            .list_paginated(
                prefix.as_deref(),
                PaginatedListOptions {
                    max_keys: Some(list_limit(limit)?),
                    page_token: cursor.map(str::to_owned),
                    ..Default::default()
                },
            )
            .await
            .map_err(storage_error)?;
        let objects = page
            .result
            .objects
            .into_iter()
            .map(object)
            .collect::<Result<_>>()?;
        Ok(ListPage {
            objects,
            next_cursor: page.page_token,
        })
    }

    async fn delete(&self, key: &Key) -> Result<()> {
        match self.0.delete(&path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(storage_error(error)),
        }
    }
}

async fn require_eof(reader: &mut Reader) -> Result<()> {
    let mut byte = [0];
    if reader.read(&mut byte).await? != 0 {
        return Err(Error::Invalid(
            "object body is longer than its declared length".into(),
        ));
    }
    Ok(())
}

fn require_identity(response: &object_store::GetResult) -> Result<()> {
    if response
        .attributes
        .get(&Attribute::ContentEncoding)
        .is_some_and(|encoding| !encoding.as_ref().eq_ignore_ascii_case("identity"))
    {
        return Err(invalid_response(
            "S3 object has an unsupported content encoding",
        ));
    }
    Ok(())
}

fn path(key: &Key) -> Path {
    Path::from(key.as_str())
}

fn object(meta: ObjectMeta) -> Result<Object> {
    Ok(Object {
        key: Key::parse(meta.location.as_ref())?,
        length: meta.size,
        version: Version {
            etag: meta.e_tag,
            generation: meta.version,
        },
        modified_at: meta.last_modified.into(),
    })
}

fn version(result: PutResult) -> Version {
    Version {
        etag: result.e_tag,
        generation: result.version,
    }
}

fn invalid_response(message: &'static str) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

fn storage_error(error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { .. } => Error::NotFound("S3 object was not found".into()),
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => {
            Error::Conflict("S3 object precondition failed".into())
        }
        _ => Error::Internal(Box::new(error)),
    }
}
