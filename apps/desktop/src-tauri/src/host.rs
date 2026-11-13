use std::{
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use sanctum_config::{Config, EmbeddingConfig};
use sanctum_proto::v1::sanctum_client::SanctumClient;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager, menu::MenuItem};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, Notify, RwLock, oneshot},
};
use tonic::transport::{Channel, Endpoint};

const START_TIMEOUT: Duration = Duration::from_secs(15);
const STOP_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone)]
pub(crate) struct Host {
    inner: Arc<Inner>,
}

struct Inner {
    settings_path: PathBuf,
    server: PathBuf,
    cli: PathBuf,
    state: RwLock<State>,
    desired: Mutex<Desired>,
    wake: Notify,
}

struct Desired {
    settings: Settings,
    generation: u64,
    quitting: bool,
    quit: Option<oneshot::Sender<()>>,
}

struct State {
    phase: &'static str,
    error: Option<String>,
    ready: Option<Ready>,
    generation: u64,
}

struct Ready {
    listeners: Listeners,
    client: SanctumClient<Channel>,
}

struct Running {
    ready: Ready,
    child: Child,
    control: ChildStdin,
}

#[derive(Clone, Deserialize, Serialize)]
struct Listeners {
    grpc: SocketAddr,
    mcp: Option<SocketAddr>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Settings {
    pub storage_root: String,
    pub grpc_port: u16,
    pub mcp_port: u16,
    pub local_worker_concurrency: usize,
    #[serde(default, deserialize_with = "read_embedding")]
    pub embedding: Option<EmbeddingConfig>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LegacyOllamaSettings {
    endpoint: String,
    model: String,
    revision: String,
    dimensions: usize,
    timeout_seconds: u64,
    document_prefix: String,
    query_prefix: String,
}

fn read_embedding<'de, D>(deserializer: D) -> std::result::Result<Option<EmbeddingConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Persisted {
        Current(EmbeddingConfig),
        Legacy(LegacyOllamaSettings),
    }
    let Some(value) = Option::<Persisted>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match value {
        Persisted::Current(config) => Ok(Some(config)),
        Persisted::Legacy(settings) => serde_json::from_value(serde_json::json!({
            "provider": "ollama",
            "endpoint": settings.endpoint,
            "timeout_seconds": settings.timeout_seconds,
            "profile": {
                "model": settings.model,
                "revision": settings.revision,
                "dimensions": settings.dimensions,
                "document_prefix": settings.document_prefix,
                "query_prefix": settings.query_prefix,
            }
        }))
        .map(Some)
        .map_err(serde::de::Error::custom),
    }
}

#[derive(Serialize)]
pub(crate) struct RuntimeStatus {
    phase: &'static str,
    error: Option<String>,
    mcp_url: Option<String>,
    stdio: Option<StdioConfig>,
    restart_pending: bool,
}

#[derive(Serialize)]
struct StdioConfig {
    command: PathBuf,
    args: [String; 3],
}

impl Settings {
    fn config(&self) -> Result<Config> {
        ensure!(
            !self.storage_root.trim().is_empty(),
            "choose a storage folder"
        );
        ensure!(
            Path::new(&self.storage_root).is_absolute(),
            "storage folder must be an absolute path"
        );
        ensure!(
            self.grpc_port == 0 || self.grpc_port != self.mcp_port,
            "gRPC and MCP must use different ports"
        );
        Config::from_json(
            &serde_json::json!({
                "version": 1,
                "storage_root": self.storage_root,
                "grpc_listen": format!("127.0.0.1:{}", self.grpc_port),
                "mcp_listen": format!("127.0.0.1:{}", self.mcp_port),
                "local_worker_concurrency": self.local_worker_concurrency,
                "embedding": self.embedding,
            })
            .to_string(),
        )
        .map_err(Into::into)
    }
}

impl Host {
    pub(crate) fn open(app: &AppHandle) -> Result<Self> {
        let (settings_path, default_path) = paths(app)?;
        let settings: Settings = match std::fs::read(&settings_path) {
            Ok(bytes) => serde_json::from_slice(&bytes).context("read Sanctum desktop settings")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let settings = Settings {
                    storage_root: default_path.to_string_lossy().into_owned(),
                    grpc_port: 47550,
                    mcp_port: 47551,
                    local_worker_concurrency: 1,
                    embedding: None,
                };
                write_settings(&settings_path, &settings)?;
                settings
            }
            Err(error) => return Err(error.into()),
        };
        let binary = std::env::current_exe()?;
        let sibling =
            |name: &str| binary.with_file_name(format!("{name}{}", std::env::consts::EXE_SUFFIX));
        Ok(Self {
            inner: Arc::new(Inner {
                settings_path,
                server: sibling("sanctum-server"),
                cli: sibling("sanctum-cli"),
                state: RwLock::new(State {
                    phase: "starting",
                    error: None,
                    ready: None,
                    generation: 0,
                }),
                desired: Mutex::new(Desired {
                    settings,
                    generation: 1,
                    quitting: false,
                    quit: None,
                }),
                wake: Notify::new(),
            }),
        })
    }

    pub(crate) async fn settings(&self) -> Settings {
        self.inner.desired.lock().await.settings.clone()
    }

    pub(crate) async fn save_settings(&self, settings: Settings) -> Result<()> {
        if let Some(embedding) = settings.config()?.embedding {
            embedding.validate()?;
        }
        let mut desired = self.inner.desired.lock().await;
        let path = self.inner.settings_path.clone();
        let persisted = settings.clone();
        tokio::task::spawn_blocking(move || write_settings(&path, &persisted)).await??;
        desired.settings = settings;
        desired.generation += 1;
        Ok(())
    }

    pub(crate) fn apply_settings(&self) {
        self.inner.wake.notify_one();
    }

    pub(crate) async fn quit(&self) -> bool {
        let (stopped, done) = oneshot::channel();
        let mut desired = self.inner.desired.lock().await;
        if desired.quitting {
            return false;
        }
        desired.quitting = true;
        desired.quit = Some(stopped);
        drop(desired);
        self.inner.wake.notify_one();
        let _ = done.await;
        true
    }

    pub(crate) async fn status(&self) -> RuntimeStatus {
        let generation = self.inner.desired.lock().await.generation;
        let state = self.inner.state.read().await;
        let mcp_url = state
            .ready
            .as_ref()
            .and_then(|ready| ready.listeners.mcp)
            .map(|address| format!("http://{address}/mcp"));
        let stdio = mcp_url.as_ref().map(|url| StdioConfig {
            command: self.inner.cli.clone(),
            args: ["mcp-stdio".into(), "--mcp-url".into(), url.clone()],
        });
        RuntimeStatus {
            phase: state.phase,
            error: state.error.clone(),
            mcp_url,
            stdio,
            restart_pending: state.generation != 0 && state.generation != generation,
        }
    }

    pub(crate) async fn client(&self) -> Result<SanctumClient<Channel>> {
        self.inner
            .state
            .read()
            .await
            .ready
            .as_ref()
            .map(|ready| ready.client.clone())
            .ok_or_else(|| anyhow!("Sanctum server is not ready"))
    }

    pub(crate) async fn run(self, tray_status: MenuItem<tauri::Wry>) {
        let mut backoff = Duration::from_secs(1);
        loop {
            let (settings, generation) = {
                let mut desired = self.inner.desired.lock().await;
                if let Some(quit) = desired.quit.take() {
                    let _ = quit.send(());
                    return;
                }
                (desired.settings.clone(), desired.generation)
            };
            self.transition(&tray_status, "starting", None, None, generation)
                .await;
            let error = match self.start(settings).await {
                Ok(Running {
                    ready,
                    mut child,
                    control,
                }) => {
                    let started = std::time::Instant::now();
                    self.transition(&tray_status, "running", None, Some(ready), generation)
                        .await;
                    tokio::select! {
                        result = child.wait() => {
                            if started.elapsed() >= Duration::from_secs(30) {
                                backoff = Duration::from_secs(1);
                            }
                            match result {
                                Ok(status) => format!("Server exited with {status}"),
                                Err(error) => format!("Could not wait for server: {error}"),
                            }
                        },
                        _ = self.inner.wake.notified() => {
                            self.transition(&tray_status, "stopping", None, None, generation).await;
                            drop(control);
                            stop_child(&mut child).await;
                            backoff = Duration::from_secs(1);
                            continue;
                        }
                    }
                }
                Err(error) => error.to_string(),
            };
            self.transition(&tray_status, "retrying", Some(error), None, generation)
                .await;
            tokio::select! {
                _ = tokio::time::sleep(backoff) => backoff = (backoff * 2).min(Duration::from_secs(30)),
                _ = self.inner.wake.notified() => backoff = Duration::from_secs(1),
            }
        }
    }

    async fn transition(
        &self,
        tray_status: &MenuItem<tauri::Wry>,
        phase: &'static str,
        error: Option<String>,
        ready: Option<Ready>,
        generation: u64,
    ) {
        *self.inner.state.write().await = State {
            phase,
            error,
            ready,
            generation,
        };
        let _ = tray_status.set_text(match phase {
            "running" => "Server running",
            "retrying" => "Server unavailable, retrying",
            "stopping" => "Stopping server…",
            _ => "Starting server…",
        });
    }

    async fn start(&self, settings: Settings) -> Result<Running> {
        let config = serde_json::to_string(&settings.config()?)?;
        let mut command = Command::new(&self.inner.server);
        command
            .env(sanctum_config::CONFIG_ENV, config)
            .env("SANCTUM_MANAGED_STDIO", "1")
            .env("NO_COLOR", "1")
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(windows)]
        command.creation_flags(0x0800_0000);
        let mut child = command
            .spawn()
            .context("Could not launch the Sanctum server sidecar")?;
        // Child::wait closes the child's own stdin handle before waiting.
        let control = child.stdin.take().context("missing server control pipe")?;
        let mut out = BufReader::new(child.stdout.take().context("missing server output pipe")?);
        let err = child.stderr.take().context("missing server log pipe")?;
        let logs = Arc::new(Mutex::new(std::collections::VecDeque::with_capacity(12)));
        let log_task = logs.clone();
        let drain = tokio::spawn(async move {
            let mut lines = BufReader::new(err).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut recent = log_task.lock().await;
                if recent.len() == 12 {
                    recent.pop_front();
                }
                recent.push_back(line);
            }
        });
        let ready = tokio::time::timeout(START_TIMEOUT, async {
            let mut line = String::new();
            ensure!(
                out.read_line(&mut line).await? != 0,
                "Server exited during startup"
            );
            let listeners: Listeners =
                serde_json::from_str(&line).context("invalid server startup response")?;
            let client = Endpoint::from_shared(format!("http://{}", listeners.grpc))?
                .connect_timeout(Duration::from_secs(3))
                .connect()
                .await?;
            let client = SanctumClient::new(client);
            Ok::<_, anyhow::Error>(Ready { listeners, client })
        })
        .await;
        match ready {
            Ok(Ok(ready)) => Ok(Running {
                ready,
                child,
                control,
            }),
            failure => {
                drop(control);
                stop_child(&mut child).await;
                let _ = drain.await;
                let error = logs.lock().await.back().cloned();
                let reason = match failure {
                    Ok(Err(error)) => error.to_string(),
                    Err(_) => "Timed out waiting for the server to start".into(),
                    Ok(Ok(_)) => unreachable!(),
                };
                Err(anyhow!(
                    "{}",
                    error.map_or(reason.clone(), |line| format!("{reason}. {line}"))
                ))
            }
        }
    }
}

fn paths(app: &AppHandle) -> Result<(PathBuf, PathBuf)> {
    if cfg!(debug_assertions)
        && let Some(root) = std::env::var_os("SANCTUM_DESKTOP_DEV_ROOT")
    {
        let root = PathBuf::from(root);
        ensure!(
            root.is_absolute(),
            "SANCTUM_DESKTOP_DEV_ROOT must be an absolute path"
        );
        return Ok((root.join("config.json"), root.join("storage")));
    }
    Ok((
        app.path().app_config_dir()?.join("config.json"),
        app.path().app_local_data_dir()?.join("storage"),
    ))
}

async fn stop_child(child: &mut Child) {
    if tokio::time::timeout(STOP_TIMEOUT, child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
    }
}

fn write_settings(path: &Path, settings: &Settings) -> Result<()> {
    let dir = path.parent().context("settings file has no parent")?;
    std::fs::create_dir_all(dir)?;
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    serde_json::to_writer_pretty(&mut file, settings)?;
    writeln!(file)?;
    file.as_file().sync_all()?;
    file.persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_existing_ollama_settings_and_accepts_provider_config() {
        let root = tempfile::tempdir().unwrap();
        let settings = |embedding| {
            serde_json::from_value::<Settings>(serde_json::json!({
                "storage_root": root.path(),
                "grpc_port": 0,
                "mcp_port": 0,
                "local_worker_concurrency": 1,
                "embedding": embedding
            }))
            .unwrap()
        };
        let legacy = settings(serde_json::json!({
            "endpoint": "http://127.0.0.1:11434",
            "model": "embeddinggemma-2:740m",
            "revision": "example",
            "dimensions": 768,
            "timeout_seconds": 30,
            "document_prefix": "doc:",
            "query_prefix": "query:"
        }));
        let Some(EmbeddingConfig::Ollama(ollama)) = legacy.config().unwrap().embedding else {
            panic!("expected the existing Ollama profile");
        };
        assert_eq!(ollama.profile.document_prefix, "doc:");
        assert_eq!(ollama.profile.query_prefix, "query:");
        let profile = serde_json::json!({"model":"example","revision":"1","dimensions":768});
        let hosted = settings(serde_json::json!({
            "provider": "openai",
            "base_url": "https://openrouter.ai/api/v1",
            "api_key_env": "SANCTUM_TEST_EMBEDDINGS",
            "profile": profile
        }));
        let config = hosted.config().unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let parsed = Config::from_json(&json).unwrap();
        let Some(EmbeddingConfig::OpenAi(openai)) = parsed.embedding else {
            panic!("expected the OpenAI-compatible profile");
        };
        assert_eq!(
            openai.api_key_env.as_deref(),
            Some("SANCTUM_TEST_EMBEDDINGS")
        );
        EmbeddingConfig::OpenAi(openai).validate().unwrap();
        let google = settings(serde_json::json!({
            "provider": "google",
            "project": "sanctum-test",
            "location": "us-central1",
            "profile": profile
        }));
        let google = google.config().unwrap().embedding.unwrap();
        assert!(matches!(&google, EmbeddingConfig::Google(_)));
        google.validate().unwrap();
    }
}
