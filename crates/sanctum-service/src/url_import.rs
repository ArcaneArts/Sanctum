use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{
    Client, StatusCode, Url,
    header::{self, HeaderMap, HeaderName, HeaderValue},
};
use sanctum_core::{Error, ExternalSource, ImportCredential, Result, Scope, UrlProvenance, is_rtf};
use tokio::io::AsyncWriteExt;

use crate::{
    ImportEvent, Service, TextUpload,
    local_import::{ImportProvenance, default_text},
};

const MAX_REDIRECTS: usize = 5;
const MAX_URL_BYTES: usize = 4096;

#[derive(Clone)]
pub(crate) struct UrlImporter {
    client: Client,
    origins: Option<Arc<HashMap<String, Option<ImportCredential>>>>,
}

impl UrlImporter {
    pub(crate) fn new() -> Result<Self> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|error| Error::Internal(Box::new(error)))?;
        Ok(Self {
            client,
            origins: None,
        })
    }

    fn credential(&self, url: &Url) -> Result<Option<&ImportCredential>> {
        match &self.origins {
            None => Ok(None),
            Some(origins) => origins
                .get(&url.origin().ascii_serialization())
                .map(Option::as_ref)
                .ok_or_else(|| Error::Invalid("URL import origin is not allowed".into())),
        }
    }

    pub(crate) fn authorization(&self, url: &Url) -> Result<Option<String>> {
        let Some(credential) = self.credential(url)? else {
            return Ok(None);
        };
        let secret = std::env::var(secret_env(credential))
            .ok()
            .filter(|secret| !secret.is_empty())
            .ok_or_else(|| Error::Invalid("configured import credential is unavailable".into()))?;
        if secret.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(Error::Invalid(
                "configured import credential contains a control character".into(),
            ));
        }
        let value = match credential {
            ImportCredential::Bearer(_) => {
                if !secret.is_ascii() {
                    return Err(Error::Invalid(
                        "configured bearer token must be ASCII".into(),
                    ));
                }
                format!("Bearer {secret}")
            }
            ImportCredential::Basic(config) => {
                let value = format!("{}:{secret}", config.username);
                format!("Basic {}", STANDARD.encode(value))
            }
        };
        Ok(Some(value))
    }

    async fn get(&self, requested: &Url) -> Result<reqwest::Response> {
        let mut url = requested.clone();
        for redirect in 0..=MAX_REDIRECTS {
            let mut request = self
                .client
                .get(url.clone())
                .header(header::ACCEPT_ENCODING, "identity");
            if let Some(authorization) = self.authorization(&url)? {
                let mut value = HeaderValue::from_str(&authorization).map_err(|_| {
                    Error::Invalid("configured import credential is not a valid HTTP header".into())
                })?;
                value.set_sensitive(true);
                request = request.header(header::AUTHORIZATION, value);
            }
            let response = request.send().await.map_err(request_error)?;
            let status = response.status();
            match status {
                StatusCode::OK => return Ok(response),
                StatusCode::MOVED_PERMANENTLY
                | StatusCode::FOUND
                | StatusCode::SEE_OTHER
                | StatusCode::TEMPORARY_REDIRECT
                | StatusCode::PERMANENT_REDIRECT
                    if redirect < MAX_REDIRECTS =>
                {
                    let location =
                        header_text(response.headers(), header::LOCATION).ok_or_else(|| {
                            Error::Invalid(format!(
                                "URL returned HTTP {status} without a usable Location"
                            ))
                        })?;
                    let next = url
                        .join(&location)
                        .map_err(|_| Error::Invalid("invalid URL import redirect target".into()))?;
                    if url.scheme() == "https" && next.scheme() != "https" {
                        return Err(Error::Invalid(
                            "HTTPS URL import cannot redirect to HTTP".into(),
                        ));
                    }
                    url = validate_url(next)?;
                }
                _ => {
                    let message = format!("URL returned HTTP {status}");
                    return Err(
                        if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
                            Error::Busy(message)
                        } else {
                            Error::Invalid(message)
                        },
                    );
                }
            }
        }
        unreachable!("the final redirect returns an error")
    }
}

impl Service {
    /// An explicit list restricts every outbound request, including redirects.
    /// Credentials are chosen by the operator for an exact HTTPS origin.
    pub fn with_url_origins(
        mut self,
        origins: Option<HashMap<String, Option<ImportCredential>>>,
    ) -> Result<Self> {
        if let Some(origins) = origins {
            let mut normalized = HashMap::new();
            for (origin, credential) in origins {
                let url = parse_url(&origin)?;
                if url.path() != "/" || url.query().is_some() || origin.contains('#') {
                    return Err(Error::Invalid(
                        "URL import policy requires origins without paths, queries, or fragments"
                            .into(),
                    ));
                }
                if let Some(credential) = &credential {
                    validate_credential(&url, credential)?;
                }
                if normalized
                    .insert(url.origin().ascii_serialization(), credential)
                    .is_some()
                {
                    return Err(Error::Invalid("duplicate URL import origin".into()));
                }
            }
            self.url_importer.origins = Some(Arc::new(normalized));
        }
        Ok(self)
    }

    pub async fn import_url(
        &self,
        scope: Scope,
        url: &str,
        retain_original: bool,
    ) -> Result<(ImportEvent, UrlProvenance)> {
        let requested = parse_url(url)?;
        let service = self.clone();
        let scope = tokio::task::spawn_blocking(move || {
            service.require_collection(&scope)?;
            Ok::<_, Error>(scope)
        })
        .await
        .map_err(task_error)??;
        let mut response = self.url_importer.get(&requested).await?;
        let headers = response.headers();
        let provenance = UrlProvenance {
            requested_url: requested.to_string(),
            final_url: response.url().to_string(),
            etag: header_text(headers, header::ETAG),
            last_modified: header_text(headers, header::LAST_MODIFIED),
            content_type: header_text(headers, header::CONTENT_TYPE),
        };
        let name = source_name(response.url(), provenance.content_type.as_deref())?;
        if headers
            .get_all(header::CONTENT_ENCODING)
            .iter()
            .any(|value| value != "identity")
        {
            return Err(Error::Invalid(
                "URL response uses an unsupported content encoding".into(),
            ));
        }
        if response
            .content_length()
            .is_some_and(|size| size > self.max_source_bytes)
        {
            return Err(size_error());
        }
        let external = ExternalSource {
            namespace: "url".into(),
            id: requested.to_string(),
        };
        let service = self.clone();
        let (staged, output) = tokio::task::spawn_blocking(move || {
            let staged = service.stage_upload(
                scope,
                TextUpload {
                    name,
                    external: Some(external),
                    replacement: None,
                    retain_original,
                    initial_metadata: Vec::new(),
                    initial_chunk_metadata: Vec::new(),
                },
            )?;
            let output = staged.reopen()?;
            Ok::<_, Error>((staged, output))
        })
        .await
        .map_err(task_error)??;
        let mut output = tokio::fs::File::from_std(output);
        let mut hasher = blake3::Hasher::new();
        let mut bytes = 0;
        while let Some(chunk) = response.chunk().await.map_err(request_error)? {
            bytes += chunk.len() as u64;
            if bytes > self.max_source_bytes {
                return Err(size_error());
            }
            hasher.update(&chunk);
            output.write_all(&chunk).await?;
        }
        output.flush().await?;
        drop(output);
        let saved = provenance.clone();
        let event = tokio::task::spawn_blocking(move || {
            staged.enqueue_import(
                bytes,
                hasher.finalize().to_hex().to_string(),
                Some(ImportProvenance::Url(saved)),
            )
        })
        .await
        .map_err(task_error)??;
        Ok((event, provenance))
    }
}

fn secret_env(credential: &ImportCredential) -> &str {
    match credential {
        ImportCredential::Bearer(env) => env,
        ImportCredential::Basic(config) => &config.password_env,
    }
}

fn validate_credential(url: &Url, credential: &ImportCredential) -> Result<()> {
    let env = secret_env(credential);
    if url.scheme() != "https"
        || env.is_empty()
        || !env
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(Error::Invalid(
            "import credentials require an HTTPS origin and an environment variable name".into(),
        ));
    }
    if let ImportCredential::Basic(config) = credential
        && (config.username.is_empty()
            || config
                .username
                .bytes()
                .any(|byte| byte == b':' || byte.is_ascii_control()))
    {
        return Err(Error::Invalid(
            "basic import credentials require a username without colons or control characters"
                .into(),
        ));
    }
    Ok(())
}

pub(crate) fn parse_url(input: &str) -> Result<Url> {
    if input.len() > MAX_URL_BYTES {
        return Err(Error::Invalid("URL import address is too long".into()));
    }
    validate_url(
        Url::parse(input).map_err(|_| Error::Invalid("invalid URL import address".into()))?,
    )
}

fn validate_url(mut url: Url) -> Result<Url> {
    url.set_fragment(None);
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.as_str().len() > MAX_URL_BYTES
    {
        return Err(Error::Invalid(
            "URL import requires an HTTP(S) URL without embedded credentials".into(),
        ));
    }
    Ok(url)
}

fn header_text(headers: &HeaderMap, name: HeaderName) -> Option<String> {
    headers.get(name)?.to_str().ok().map(str::to_owned)
}

fn source_name(url: &Url, content_type: Option<&str>) -> Result<String> {
    let path_name = url
        .path_segments()
        .and_then(Iterator::last)
        .unwrap_or_default();
    let name = if path_name.is_empty() || path_name.len() > 1019 {
        "source"
    } else {
        path_name
    };
    let supported_path = default_text(Path::new(path_name));
    let mime = content_type
        .map(str::parse::<mime::Mime>)
        .transpose()
        .map_err(|_| Error::Invalid("invalid URL response content type".into()))?;
    let media = mime.as_ref().map(mime::Mime::essence_str);
    let rtf = matches!(
        media,
        Some("application/rtf" | "application/x-rtf" | "text/rtf")
    ) || is_rtf(name)
        && matches!(
            media,
            None | Some("text/plain" | "application/octet-stream")
        );
    let text = match &mime {
        None => supported_path,
        Some(mime) => {
            mime.type_() == mime::TEXT
                || matches!(
                    mime.essence_str(),
                    "application/json"
                        | "application/x-ndjson"
                        | "application/xml"
                        | "application/yaml"
                        | "application/x-yaml"
                        | "application/toml"
                        | "application/javascript"
                )
                || matches!(mime.suffix(), Some(mime::JSON | mime::XML))
                || mime.essence_str() == "application/octet-stream" && supported_path
        }
    };
    if rtf {
        return Ok(if is_rtf(name) {
            name.into()
        } else {
            format!("{name}.rtf")
        });
    }
    if !text {
        return Err(Error::Invalid(
            "unsupported URL response content type".into(),
        ));
    }
    if mime
        .as_ref()
        .and_then(|mime| mime.get_param(mime::CHARSET))
        .is_some_and(|charset| {
            !matches!(
                charset.as_str().to_ascii_lowercase().as_str(),
                "utf-8" | "us-ascii"
            )
        })
    {
        return Err(Error::Invalid(
            "URL response text must be UTF-8 or ASCII".into(),
        ));
    }
    // The shared extractor selects RTF by filename.
    Ok(if is_rtf(name) {
        format!("{name}.txt")
    } else {
        name.into()
    })
}

fn size_error() -> Error {
    Error::Invalid("URL response exceeds the configured upload limit".into())
}

fn request_error(error: reqwest::Error) -> Error {
    if error.is_builder() {
        return Error::Invalid("invalid URL import request configuration".into());
    }
    // A transport error can carry the URL, which may contain sensitive data.
    Error::Busy(if error.is_timeout() {
        "URL import request timed out".into()
    } else if error.is_connect() {
        "could not connect to URL import destination".into()
    } else {
        "URL import HTTP transfer failed".into()
    })
}

fn task_error(error: tokio::task::JoinError) -> Error {
    Error::Internal(Box::new(error))
}
