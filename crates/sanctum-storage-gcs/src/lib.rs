use std::{
    io,
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::{Buf, Bytes, BytesMut};
use futures::{TryStreamExt, stream};

#[cfg(test)]
mod tests;
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use reqwest::{
    Client, Method, Response, StatusCode, Url,
    header::{self, HeaderMap, HeaderName},
};
use sanctum_core::{ByteRange, Error, Result};
use sanctum_storage::{
    Condition, Download, Key, ListPage, MAX_CONTROL_BYTES, Object, Reader, Storage, Version,
    list_limit, range_len,
};
use serde::Deserialize;
use tokio::{io::AsyncReadExt, sync::OnceCell};
use tokio_util::io::StreamReader;

const ENDPOINT: &str = "https://storage.googleapis.com/";
const CLOUD_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
const PART_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UPLOAD_RETRIES: u32 = 4;
const MAX_DOWNLOAD_ATTEMPTS: u32 = 4;
const DOWNLOAD_OPEN_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_DOWNLOAD_RESUMES: u32 = 4;
const MAX_RANGE_ATTEMPTS: u32 = 3;
const RANGE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const RANGE_DEADLINE: Duration = Duration::from_secs(15);

#[async_trait::async_trait]
trait Credential: Send + Sync {
    async fn token(&self) -> Result<String>;
}

#[derive(Default)]
struct ApplicationCredentials(OnceCell<AccessTokenCredentials>);

#[async_trait::async_trait]
impl Credential for ApplicationCredentials {
    async fn token(&self) -> Result<String> {
        let credentials = self
            .0
            .get_or_try_init(|| async {
                tokio::task::spawn_blocking(|| {
                    Builder::default()
                        .with_scopes([CLOUD_SCOPE])
                        .build_access_token_credentials()
                })
                .await
                .map_err(|error| Error::Internal(Box::new(error)))?
                .map_err(|_| {
                    Error::Busy("Google Application Default Credentials are unavailable".into())
                })
            })
            .await?;
        credentials
            .access_token()
            .await
            .map(|value| value.token)
            .map_err(|_| Error::Busy("could not refresh Google Cloud Storage credentials".into()))
    }
}

#[derive(Clone)]
pub struct GcsStorage {
    bucket: String,
    endpoint: Url,
    client: Client,
    credentials: Arc<dyn Credential>,
}

impl GcsStorage {
    pub fn new(bucket: String) -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self::with_endpoint(
            bucket,
            Url::parse(ENDPOINT).expect("valid Google endpoint"),
            Arc::new(ApplicationCredentials::default()),
        )
    }

    fn with_endpoint(
        bucket: String,
        endpoint: Url,
        credentials: Arc<dyn Credential>,
    ) -> Result<Self> {
        if bucket.len() < 3
            || bucket.len() > 222
            || !bucket.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".-_".contains(&byte)
            })
        {
            return Err(Error::Invalid("invalid Google Cloud Storage bucket".into()));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|error| Error::Internal(Box::new(error)))?;
        Ok(Self {
            bucket,
            endpoint,
            client,
            credentials,
        })
    }

    fn objects_url(&self, upload: bool, key: Option<&Key>) -> Url {
        let mut url = self.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .expect("storage endpoint is hierarchical");
            segments.clear();
            if upload {
                segments.push("upload");
            }
            segments.extend(["storage", "v1", "b", &self.bucket, "o"]);
            if let Some(key) = key {
                segments.push(key.as_str());
            }
        }
        url
    }

    async fn request(&self, method: Method, url: Url) -> Result<reqwest::RequestBuilder> {
        Ok(self
            .client
            .request(method, url)
            .bearer_auth(self.credentials.token().await?)
            .header(header::ACCEPT_ENCODING, "identity"))
    }

    async fn media(
        &self,
        key: &Key,
        range: Option<ByteRange>,
        generation: Option<&str>,
    ) -> Result<Response> {
        let mut url = self.objects_url(false, Some(key));
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("alt", "media");
            if let Some(generation) = generation {
                query.append_pair("ifGenerationMatch", generation);
            }
        }
        let mut request = self.request(Method::GET, url).await?;
        if let Some(range) = range {
            request = request.header(
                header::RANGE,
                format!("bytes={}-{}", range.start, range.end - 1),
            );
        }
        let response = request.send().await.map_err(transfer_error)?;
        let expected = if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        if response.status() != expected {
            return Err(if response.status().is_success() {
                invalid_response("GCS returned an unexpected status for the requested byte range")
            } else {
                http_error(response.status())
            });
        }
        if response
            .headers()
            .get_all(header::CONTENT_ENCODING)
            .iter()
            .any(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(invalid_response(
                "GCS object uses an unsupported content encoding",
            ));
        }
        Ok(response)
    }

    async fn read_exact_range(
        &self,
        key: &Key,
        range: ByteRange,
        size: usize,
        transferred: &mut usize,
    ) -> Result<Bytes> {
        let response = self.media(key, Some(range), None).await?;
        response_range(&response, range)?;
        check_download_length(&response, size as u64)?;
        let mut result = BytesMut::with_capacity(size);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream
            .try_next()
            .await
            .map_err(|_| Error::Busy("GCS byte range response was interrupted".into()))?
        {
            *transferred += chunk.len();
            if chunk.len() > size - result.len() {
                return Err(invalid_response("GCS returned more bytes than requested"));
            }
            result.extend_from_slice(&chunk);
        }
        if result.len() != size {
            return Err(invalid_response("GCS returned fewer bytes than requested"));
        }
        Ok(result.freeze())
    }

    async fn write_small(&self, key: &Key, bytes: Bytes, generation: &str) -> Result<Version> {
        let mut url = self.objects_url(true, None);
        url.query_pairs_mut()
            .append_pair("name", key.as_str())
            .append_pair("uploadType", "media")
            .append_pair("ifGenerationMatch", generation);
        let length = bytes.len() as u64;
        let response = self
            .request(Method::POST, url)
            .await?
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .map_err(transfer_error)?;
        check(response.status())?;
        let object: ObjectMetadata = response.json().await.map_err(transfer_error)?;
        if object.name != key.as_str() || object.size()? != length {
            return Err(invalid_response(
                "GCS uploaded object differs from its request",
            ));
        }
        object.version()
    }

    async fn upload_part(&self, session: &Url, range: String, bytes: Bytes) -> Result<Response> {
        // A resumable upload URI grants access to that session. Keep it out of errors.
        let response = self
            .client
            .put(session.clone())
            .header(header::CONTENT_RANGE, range)
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(bytes)
            .send()
            .await
            .map_err(transfer_error)?;
        match response.status() {
            StatusCode::OK | StatusCode::CREATED | StatusCode::PERMANENT_REDIRECT => Ok(response),
            StatusCode::REQUEST_TIMEOUT => Err(Error::Busy("GCS upload request timed out".into())),
            status => Err(http_error(status)),
        }
    }

    async fn put_resumable(&self, key: &Key, reader: Reader, length: u64) -> Result<Version> {
        let mut url = self.objects_url(true, None);
        url.query_pairs_mut()
            .append_pair("name", key.as_str())
            .append_pair("uploadType", "resumable")
            .append_pair("ifGenerationMatch", "0");
        let response = self
            .request(Method::POST, url)
            .await?
            .header("x-upload-content-length", length)
            .header("x-upload-content-type", "application/octet-stream")
            .header(header::CONTENT_LENGTH, 0)
            .send()
            .await
            .map_err(transfer_error)?;
        check(response.status())?;
        let session = header_value(response.headers(), header::LOCATION)
            .and_then(|location| self.endpoint.join(location).ok())
            .filter(|url| {
                url.origin() == self.endpoint.origin()
                    && url.username().is_empty()
                    && url.password().is_none()
            })
            .ok_or_else(|| invalid_response("GCS resumable upload omitted its session URL"))?;
        let result = self.upload_contents(&session, key, reader, length).await;
        if result.is_err() {
            let _ = self
                .client
                .delete(session)
                .header(header::CONTENT_LENGTH, 0)
                .timeout(Duration::from_secs(5))
                .send()
                .await;
        }
        result
    }

    async fn upload_contents(
        &self,
        session: &Url,
        key: &Key,
        mut reader: Reader,
        length: u64,
    ) -> Result<Version> {
        let mut buffer = Bytes::new();
        let mut committed = 0;
        let mut sent_until = 0;
        let mut probing = false;
        let mut retries = 0;
        loop {
            let response = if probing {
                self.upload_part(session, format!("bytes */{length}"), Bytes::new())
                    .await
            } else {
                let size = (length - committed).min(PART_BYTES) as usize;
                let end = committed + size as u64;
                if buffer.len() < size {
                    // Keep bytes after the acknowledged offset. Refill so the next
                    // non-final part is still a multiple of 256 KiB.
                    let mut part = buffer
                        .try_into_mut()
                        .unwrap_or_else(|remaining| BytesMut::from(remaining.as_ref()));
                    let buffered = part.len();
                    part.resize(size, 0);
                    reader.read_exact(&mut part[buffered..]).await?;
                    if end == length && reader.read(&mut [0]).await? != 0 {
                        return Err(Error::Invalid(
                            "object body is longer than its declared length".into(),
                        ));
                    }
                    buffer = part.freeze();
                }
                sent_until = end;
                self.upload_part(
                    session,
                    format!("bytes {committed}-{}/{length}", end - 1),
                    buffer.clone(),
                )
                .await
            };
            let outcome = match response {
                Ok(response) if response.status() == StatusCode::PERMANENT_REDIRECT => {
                    upload_offset(response.headers()).map(UploadOutcome::Incomplete)
                }
                Ok(response) if sent_until == length => upload_version(response, key, length)
                    .await
                    .map(UploadOutcome::Complete),
                Ok(_) => return Err(invalid_response("GCS completed a resumable upload early")),
                Err(error) => Err(error),
            };
            match outcome {
                Ok(UploadOutcome::Complete(version)) => return Ok(version),
                Ok(UploadOutcome::Incomplete(offset)) => {
                    if offset < committed || offset > sent_until || offset == length {
                        return Err(invalid_response(
                            "GCS acknowledged an invalid upload offset",
                        ));
                    }
                    probing = false;
                    if offset > committed {
                        buffer.advance((offset - committed) as usize);
                        committed = offset;
                        retries = 0;
                        continue;
                    }
                    if retries == MAX_UPLOAD_RETRIES {
                        return Err(Error::Busy("GCS resumable upload made no progress".into()));
                    }
                }
                Err(Error::Busy(_)) if retries < MAX_UPLOAD_RETRIES => probing = true,
                Err(error) => return Err(error),
            }
            tokio::time::sleep(Duration::from_millis(100 << retries)).await;
            retries += 1;
        }
    }
}

enum UploadOutcome {
    Incomplete(u64),
    Complete(Version),
}

fn upload_offset(headers: &HeaderMap) -> Result<u64> {
    let Some(range) = headers.get(header::RANGE) else {
        return Ok(0);
    };
    range
        .to_str()
        .ok()
        .and_then(|range| range.strip_prefix("bytes=0-"))
        .and_then(|end| end.parse::<u64>().ok())
        .and_then(|end| end.checked_add(1))
        .ok_or_else(|| invalid_response("GCS returned an invalid upload range"))
}

async fn upload_version(response: Response, key: &Key, length: u64) -> Result<Version> {
    let mut bytes = BytesMut::new();
    let mut body = response.bytes_stream();
    while let Some(chunk) = body
        .try_next()
        .await
        .map_err(|_| Error::Busy("GCS resumable upload response was interrupted".into()))?
    {
        if chunk.len() > MAX_CONTROL_BYTES - bytes.len() {
            return Err(invalid_response("GCS returned oversized upload metadata"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let uploaded: ObjectMetadata = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response("GCS returned invalid upload metadata"))?;
    if uploaded.size()? != length || uploaded.name != key.as_str() {
        return Err(invalid_response(
            "GCS uploaded object differs from its request",
        ));
    }
    uploaded.version()
}

#[async_trait::async_trait]
impl Storage for GcsStorage {
    async fn head(&self, key: &Key) -> Result<Object> {
        let response = self
            .request(Method::GET, self.objects_url(false, Some(key)))
            .await?
            .send()
            .await
            .map_err(transfer_error)?;
        check(response.status())?;
        let object = response
            .json::<ObjectMetadata>()
            .await
            .map_err(transfer_error)?
            .object()?;
        if &object.key != key {
            return Err(invalid_response("GCS metadata returned a different object"));
        }
        Ok(object)
    }

    async fn get(&self, key: &Key) -> Result<Download> {
        self.open_download(key, DOWNLOAD_OPEN_TIMEOUT).await
    }

    async fn read_range(&self, key: &Key, range: ByteRange) -> Result<Bytes> {
        let size = range_len(range)?;
        let mut transferred = 0;
        let (result, attempts) = tokio::time::timeout(RANGE_DEADLINE, async {
            let mut attempt = 1;
            loop {
                let result = tokio::time::timeout(
                    RANGE_ATTEMPT_TIMEOUT,
                    self.read_exact_range(key, range, size, &mut transferred),
                )
                .await
                .unwrap_or_else(|_| Err(Error::Busy("GCS byte range attempt timed out".into())));
                match result {
                    Ok(bytes) => return Ok((bytes, attempt)),
                    Err(Error::Busy(_)) if attempt < MAX_RANGE_ATTEMPTS => {
                        // Nothing from a failed response has left the adapter.
                        // Replay the exact requested range, discarding partial bytes.
                        tokio::time::sleep(Duration::from_millis(100 << (attempt - 1))).await;
                        attempt += 1;
                    }
                    Err(error) => return Err(error),
                }
            }
        })
        .await
        .map_err(|_| Error::Busy("GCS byte range read exceeded its deadline".into()))??;
        tracing::debug!(
            requested_bytes = size,
            transferred_bytes = transferred,
            attempts,
            "GCS range read"
        );
        Ok(result)
    }

    async fn put_stream(&self, key: &Key, mut reader: Reader, length: u64) -> Result<Version> {
        if length > MAX_CONTROL_BYTES as u64 {
            return self.put_resumable(key, reader, length).await;
        }
        let mut bytes = vec![0; length as usize];
        reader.read_exact(&mut bytes).await?;
        if reader.read(&mut [0]).await? != 0 {
            return Err(Error::Invalid(
                "object body is longer than its declared length".into(),
            ));
        }
        self.write_small(key, bytes.into(), "0").await
    }

    async fn put_control(&self, key: &Key, bytes: Bytes, condition: Condition) -> Result<Version> {
        if bytes.len() > MAX_CONTROL_BYTES {
            return Err(Error::Invalid(
                "storage control object exceeds 1 MiB".into(),
            ));
        }
        let match_generation = match condition {
            Condition::Create => "0".into(),
            Condition::Match(expected) => expected
                .generation
                .filter(|value| valid_generation(value))
                .ok_or_else(|| {
                    Error::Invalid("GCS conditional replacement requires a generation".into())
                })?,
        };
        self.write_small(key, bytes, &match_generation).await
    }

    async fn list(
        &self,
        prefix: Option<&Key>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ListPage> {
        let mut url = self.objects_url(false, None);
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("maxResults", &list_limit(limit)?.to_string());
            if let Some(prefix) = prefix {
                query.append_pair("prefix", &format!("{prefix}/"));
            }
            if let Some(cursor) = cursor {
                query.append_pair("pageToken", cursor);
            }
        }
        let response = self
            .request(Method::GET, url)
            .await?
            .send()
            .await
            .map_err(transfer_error)?;
        check(response.status())?;
        let page: Page = response.json().await.map_err(transfer_error)?;
        let objects = page
            .items
            .into_iter()
            .map(ObjectMetadata::object)
            .collect::<Result<_>>()?;
        Ok(ListPage {
            objects,
            next_cursor: page.next_page_token,
        })
    }

    async fn delete(&self, key: &Key) -> Result<()> {
        let response = self
            .request(Method::DELETE, self.objects_url(false, Some(key)))
            .await?
            .send()
            .await
            .map_err(transfer_error)?;
        if response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            check(response.status())
        }
    }
}

impl GcsStorage {
    async fn open_download(&self, key: &Key, attempt_timeout: Duration) -> Result<Download> {
        let mut attempt = 1;
        loop {
            // A retry starts with fresh metadata. Once media headers have been
            // accepted, body retries stay pinned to the chosen generation.
            let opened = tokio::time::timeout(attempt_timeout, async {
                let object = self.head(key).await?;
                let response = match self
                    .media(key, None, object.version.generation.as_deref())
                    .await
                {
                    Err(Error::Conflict(_) | Error::NotFound(_)) => {
                        return Err(Error::Busy(
                            "GCS object changed during download open".into(),
                        ));
                    }
                    result => result?,
                };
                self.download(object, response)
            })
            .await
            .unwrap_or_else(|_| Err(Error::Busy("GCS download open timed out".into())));
            match opened {
                Err(Error::Busy(_)) if attempt < MAX_DOWNLOAD_ATTEMPTS => {
                    tokio::time::sleep(Duration::from_millis(100 << (attempt - 1))).await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    fn download(&self, object: Object, response: Response) -> Result<Download> {
        check_download_length(&response, object.length)?;
        let generation = object.version.generation.clone().ok_or_else(|| {
            invalid_response("GCS download metadata omitted its object generation")
        })?;
        let download = ResumableDownload {
            storage: self.clone(),
            key: object.key.clone(),
            generation,
            length: object.length,
            response,
            offset: 0,
        };
        let stream = stream::try_unfold(download, |mut download| async move {
            let chunk = download.next_chunk().await.map_err(|error| match error {
                Error::Io(error) => error,
                error => io::Error::other(error),
            })?;
            Ok::<_, io::Error>(chunk.map(|bytes| (bytes, download)))
        });
        Ok(Download {
            object,
            reader: Box::pin(StreamReader::new(Box::pin(stream))),
        })
    }
}

struct ResumableDownload {
    storage: GcsStorage,
    key: Key,
    generation: String,
    length: u64,
    response: Response,
    offset: u64,
}

impl ResumableDownload {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let mut retries = 0;
        loop {
            match self.response.chunk().await {
                Ok(Some(chunk)) if chunk.is_empty() => continue,
                Ok(Some(chunk)) => {
                    if chunk.len() as u64 > self.length - self.offset {
                        return Err(invalid_response("GCS returned more bytes than expected"));
                    }
                    self.offset += chunk.len() as u64;
                    return Ok(Some(chunk));
                }
                Ok(None) if self.offset == self.length => return Ok(None),
                Err(error) if self.offset == self.length => {
                    return Err(transfer_error(error));
                }
                Ok(None) | Err(_) => self.resume(&mut retries).await?,
            }
        }
    }

    async fn resume(&mut self, retries: &mut u32) -> Result<()> {
        let range = ByteRange {
            start: self.offset,
            end: self.length,
        };
        while *retries < MAX_DOWNLOAD_RESUMES {
            tokio::time::sleep(Duration::from_millis(100 << *retries)).await;
            *retries += 1;
            match self
                .storage
                .media(&self.key, Some(range), Some(&self.generation))
                .await
            {
                Ok(response) => {
                    let total = response_range(&response, range)?;
                    if total != self.length {
                        return Err(invalid_response(
                            "GCS resumed a download with a different object length",
                        ));
                    }
                    check_download_length(&response, range.end - range.start)?;
                    tracing::debug!(
                        event = "gcs_download_resume",
                        offset = self.offset,
                        object_bytes = self.length,
                        attempt = *retries,
                        "resumed a GCS object download"
                    );
                    self.response = response;
                    return Ok(());
                }
                Err(Error::Busy(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::Busy(
            "GCS download stalled before reaching its expected length".into(),
        ))
    }
}

fn check_download_length(response: &Response, expected: u64) -> Result<()> {
    if response
        .content_length()
        .is_some_and(|length| length != expected)
    {
        return Err(invalid_response(
            "GCS download length differs from its metadata",
        ));
    }
    Ok(())
}

fn response_range(response: &Response, range: ByteRange) -> Result<u64> {
    let expected = format!("bytes {}-{}", range.start, range.end - 1);
    header_value(response.headers(), header::CONTENT_RANGE)
        .and_then(|value| value.split_once('/'))
        .filter(|(actual, _)| *actual == expected)
        .and_then(|(_, total)| total.parse::<u64>().ok())
        .filter(|total| *total >= range.end)
        .ok_or_else(|| invalid_response("GCS returned a different byte range"))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ObjectMetadata {
    name: String,
    size: String,
    etag: Option<String>,
    generation: String,
    updated: String,
}

impl ObjectMetadata {
    fn size(&self) -> Result<u64> {
        self.size
            .parse()
            .map_err(|_| invalid_response("GCS returned an invalid object size"))
    }

    fn version(&self) -> Result<Version> {
        version(self.etag.clone(), Some(&self.generation))
    }

    fn object(self) -> Result<Object> {
        let length = self.size()?;
        let version = self.version()?;
        let modified_at = chrono::DateTime::parse_from_rfc3339(&self.updated)
            .map_err(|_| invalid_response("GCS returned an invalid object modification time"))?;
        Ok(Object {
            key: Key::parse(self.name)?,
            length,
            version,
            modified_at: SystemTime::from(modified_at),
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    #[serde(default)]
    items: Vec<ObjectMetadata>,
    next_page_token: Option<String>,
}

fn valid_generation(value: &str) -> bool {
    value.parse::<u64>().is_ok_and(|value| value > 0)
}

fn version(etag: Option<String>, generation: Option<&str>) -> Result<Version> {
    let generation = generation
        .filter(|generation| valid_generation(generation))
        .ok_or_else(|| invalid_response("GCS response omitted its object generation"))?;
    Ok(Version {
        etag,
        generation: Some(generation.into()),
    })
}

fn header_value(headers: &HeaderMap, name: HeaderName) -> Option<&str> {
    headers.get(name)?.to_str().ok()
}

fn check(status: StatusCode) -> Result<()> {
    if status.is_success() {
        Ok(())
    } else {
        Err(http_error(status))
    }
}

fn http_error(status: StatusCode) -> Error {
    match status {
        StatusCode::NOT_FOUND => Error::NotFound("GCS object or bucket was not found".into()),
        StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT => {
            Error::Conflict("GCS object precondition failed".into())
        }
        StatusCode::REQUEST_TIMEOUT => Error::Busy("GCS request timed out".into()),
        StatusCode::TOO_MANY_REQUESTS => Error::Busy("GCS rate limit reached".into()),
        status if status.is_server_error() => {
            Error::Busy(format!("GCS request returned HTTP {status}"))
        }
        _ => Error::Invalid(format!("GCS request returned HTTP {status}")),
    }
}

fn invalid_response(message: &'static str) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}

fn transfer_error(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Busy("GCS request timed out".into())
    } else if error.is_decode() {
        invalid_response("GCS returned invalid object metadata")
    } else {
        Error::Busy("GCS HTTP transfer failed".into())
    }
}
