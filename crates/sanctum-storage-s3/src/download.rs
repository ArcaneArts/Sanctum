use std::{
    io,
    ops::Range,
    sync::{Arc, Mutex},
};

use http::{HeaderMap, HeaderValue, StatusCode, header};
use object_store::{
    ClientOptions, GetOptions, GetRange,
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpService, ReqwestConnector,
    },
};

#[derive(Debug)]
pub(super) struct CheckedConnector;

#[derive(Debug)]
struct CheckedClient(HttpClient);

impl HttpConnector for CheckedConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(CheckedClient(
            ReqwestConnector::default().connect(options)?,
        )))
    }
}

#[derive(Clone)]
struct ReadRequest(Arc<Mutex<ReadState>>);

struct ReadState {
    range: Option<Range<u64>>,
    identity: Option<Identity>,
}

struct Identity {
    length: u64,
    etag: Option<HeaderValue>,
    version: Option<HeaderValue>,
}

pub(super) fn read_options(range: Option<Range<u64>>) -> GetOptions {
    let mut options = GetOptions {
        range: range.clone().map(GetRange::Bounded),
        ..Default::default()
    };
    options
        .extensions
        .insert(ReadRequest(Arc::new(Mutex::new(ReadState {
            range,
            identity: None,
        }))));
    options
}

#[async_trait::async_trait]
impl HttpService for CheckedClient {
    async fn call(&self, mut request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let read = request.extensions().get::<ReadRequest>().cloned();
        let range = read
            .as_ref()
            .and_then(|_| request.headers().get(header::RANGE).cloned());
        if read.is_some() {
            request.headers_mut().insert(
                header::ACCEPT_ENCODING,
                HeaderValue::from_static("identity"),
            );
        }
        let response = self.0.execute(request).await?;
        if let Some(read) = read
            && response.status().is_success()
        {
            read.check(range.as_ref(), &response)?;
        }
        Ok(response)
    }
}

impl ReadRequest {
    fn check(&self, range: Option<&HeaderValue>, response: &HttpResponse) -> Result<(), HttpError> {
        // object_store validates the original response, then resumes the stream
        // internally. Its body retry checks the ETag but bypasses range validation.
        let mut state = self.0.lock().expect("read identity lock");
        let headers = response.headers();
        let length =
            content_length(headers).ok_or_else(|| invalid("missing S3 download length"))?;
        let etag = headers.get(header::ETAG);
        let version = headers.get("x-amz-version-id");
        if headers
            .get(header::CONTENT_ENCODING)
            .is_some_and(|encoding| !encoding.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(invalid("S3 object has an unsupported content encoding"));
        }
        let total = match range {
            Some(range) if response.status() == StatusCode::PARTIAL_CONTENT => {
                let (actual, total) = header_range(headers)
                    .ok_or_else(|| invalid("S3 download has an invalid Content-Range"))?;
                let end = state.range.as_ref().map_or(total, |range| range.end);
                if parse_range(range.as_bytes(), "bytes=").as_ref() != Some(&actual)
                    || actual.end != end
                    || length != actual.end - actual.start
                {
                    return Err(invalid("S3 download returned a different byte range"));
                }
                total
            }
            None if response.status() == StatusCode::OK
                && !headers.contains_key(header::CONTENT_RANGE) =>
            {
                length
            }
            _ => {
                return Err(invalid(
                    "S3 download returned an unexpected HTTP status or range",
                ));
            }
        };
        if let Some(original) = &state.identity {
            if range.is_none()
                || original.length != total
                || original.etag.as_ref() != etag
                || original.version.as_ref() != version
            {
                return Err(invalid("S3 resumed download returned a different object"));
            }
            tracing::debug!(
                event = "s3_download_resume",
                remaining_bytes = length,
                "resumed an S3 object download"
            );
        } else {
            if state.range.is_some() != range.is_some() {
                return Err(invalid("S3 download request range changed"));
            }
            state.identity = Some(Identity {
                length: total,
                etag: etag.cloned(),
                version: version.cloned(),
            });
        }
        Ok(())
    }
}

fn header_range(headers: &HeaderMap) -> Option<(Range<u64>, u64)> {
    let (range, total) = headers
        .get(header::CONTENT_RANGE)?
        .to_str()
        .ok()?
        .split_once('/')?;
    let range = parse_range(range.as_bytes(), "bytes ")?;
    let total = total.parse().ok()?;
    (range.end <= total).then_some((range, total))
}

fn parse_range(bytes: &[u8], prefix: &str) -> Option<Range<u64>> {
    let (start, end) = std::str::from_utf8(bytes)
        .ok()?
        .strip_prefix(prefix)?
        .split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = end.parse::<u64>().ok()?.checked_add(1)?;
    (start < end).then_some(start..end)
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

fn invalid(message: &'static str) -> HttpError {
    HttpError::new(
        HttpErrorKind::Decode,
        io::Error::new(io::ErrorKind::InvalidData, message),
    )
}
