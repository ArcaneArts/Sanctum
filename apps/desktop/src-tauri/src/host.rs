use std::{
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use sanctum_config::{Config, EmbeddingConfig, ObjectStorageConfig};
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
    saved: Configuration,
    applied: Configuration,
    quitting: bool,
    quit: Option<oneshot::Sender<()>>,
}

#[derive(Clone)]
struct Configuration {
    settings: Settings,
    generation: u64,
}

struct State {
    phase: &'static str,
    error: Option<String>,
    ready: Option<Ready>,
    generation: u64,
}

struct Ready {
    listeners: Listeners,
    storage_backend: &'static str,
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
    pub object_storage: Option<ObjectStorageConfig>,
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
    pub(crate) storage_backend: Option<&'static str>,
    generation: u64,
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
        let object_storage = self.object_storage.as_ref();
        match object_storage {
            None => {
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
            }
            Some(ObjectStorageConfig::S3 { bucket, region, .. }) => {
                ensure!(!bucket.trim().is_empty(), "enter an S3 bucket");
                ensure!(!region.trim().is_empty(), "enter an S3 region");
            }
            Some(ObjectStorageConfig::Gcs { bucket, .. }) => {
                ensure!(
                    !bucket.trim().is_empty(),
                    "enter a Google Cloud Storage bucket"
                );
            }
        }
        Config::from_json(
            &serde_json::json!({
                "version": 1,
                "storage_root": self.storage_root,
                "grpc_listen": format!("127.0.0.1:{}", self.grpc_port),
                "mcp_listen": object_storage.is_none().then(|| format!("127.0.0.1:{}", self.mcp_port)),
                "object_storage": object_storage,
                "standalone_object_worker": object_storage.is_some(),
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
                    object_storage: None,
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
        Ok(Self::new(
            settings_path,
            settings,
            sibling("sanctum-server"),
            sibling("sanctum-cli"),
        ))
    }

    fn new(settings_path: PathBuf, settings: Settings, server: PathBuf, cli: PathBuf) -> Self {
        Self {
            inner: Arc::new(Inner {
                settings_path,
                server,
                cli,
                state: RwLock::new(State {
                    phase: "starting",
                    error: None,
                    ready: None,
                    generation: 1,
                }),
                desired: Mutex::new(Desired {
                    saved: Configuration {
                        settings: settings.clone(),
                        generation: 1,
                    },
                    applied: Configuration {
                        settings,
                        generation: 1,
                    },
                    quitting: false,
                    quit: None,
                }),
                wake: Notify::new(),
            }),
        }
    }

    pub(crate) async fn settings(&self) -> Settings {
        self.inner.desired.lock().await.saved.settings.clone()
    }

    pub(crate) async fn save_settings(&self, settings: Settings) -> Result<()> {
        if let Some(embedding) = settings.config()?.embedding {
            embedding.validate()?;
        }
        let mut desired = self.inner.desired.lock().await;
        let path = self.inner.settings_path.clone();
        let persisted = settings.clone();
        tokio::task::spawn_blocking(move || write_settings(&path, &persisted)).await??;
        desired.saved.settings = settings;
        desired.saved.generation += 1;
        Ok(())
    }

    pub(crate) async fn apply_settings(&self) {
        let mut desired = self.inner.desired.lock().await;
        if desired.applied.generation != desired.saved.generation {
            desired.applied = desired.saved.clone();
            self.inner.wake.notify_one();
        }
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

    pub(crate) async fn snapshot(&self) -> (RuntimeStatus, Option<SanctumClient<Channel>>) {
        let generation = self.inner.desired.lock().await.saved.generation;
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
        let status = RuntimeStatus {
            phase: state.phase,
            error: state.error.clone(),
            mcp_url,
            storage_backend: state.ready.as_ref().map(|ready| ready.storage_backend),
            generation: state.generation,
            stdio,
            restart_pending: state.generation != generation,
        };
        (
            status,
            state.ready.as_ref().map(|ready| ready.client.clone()),
        )
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
            let Configuration {
                settings,
                generation,
            } = {
                let mut desired = self.inner.desired.lock().await;
                if let Some(quit) = desired.quit.take() {
                    let _ = quit.send(());
                    return;
                }
                desired.applied.clone()
            };
            self.transition(&tray_status, "starting", None, None, generation)
                .await;
            let error = match self.start(settings, generation).await {
                Ok(Some(Running {
                    ready,
                    mut child,
                    control,
                })) => {
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
                        _ = self.wait_for_change(generation) => {
                            self.transition(&tray_status, "stopping", None, None, generation).await;
                            drop(control);
                            stop_child(&mut child).await;
                            backoff = Duration::from_secs(1);
                            continue;
                        }
                    }
                }
                Ok(None) => {
                    backoff = Duration::from_secs(1);
                    continue;
                }
                Err(error) => error.to_string(),
            };
            self.transition(&tray_status, "retrying", Some(error), None, generation)
                .await;
            tokio::select! {
                _ = tokio::time::sleep(backoff) => backoff = (backoff * 2).min(Duration::from_secs(30)),
                _ = self.wait_for_change(generation) => backoff = Duration::from_secs(1),
            }
        }
    }

    async fn wait_for_change(&self, generation: u64) {
        loop {
            let changed = self.inner.wake.notified();
            {
                let desired = self.inner.desired.lock().await;
                if desired.quitting || desired.applied.generation != generation {
                    return;
                }
            }
            changed.await;
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

    async fn start(&self, settings: Settings, generation: u64) -> Result<Option<Running>> {
        let config = settings.config()?;
        let storage_backend = match &config.object_storage {
            None => "filesystem",
            Some(ObjectStorageConfig::S3 { .. }) => "s3",
            Some(ObjectStorageConfig::Gcs { .. }) => "gcs",
        };
        let config = serde_json::to_string(&config)?;
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
        let ready = async {
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
            Ok::<_, anyhow::Error>(Ready {
                listeners,
                storage_backend,
                client,
            })
        };
        let startup = tokio::select! {
            biased;
            _ = self.wait_for_change(generation) => None,
            ready = tokio::time::timeout(START_TIMEOUT, ready) => Some(ready),
        };
        let reason = match startup {
            Some(Ok(Ok(ready))) => {
                return Ok(Some(Running {
                    ready,
                    child,
                    control,
                }));
            }
            Some(Ok(Err(error))) => Some(error.to_string()),
            Some(Err(_)) => Some("Timed out waiting for the server to start".into()),
            None => None,
        };
        drop(control);
        stop_child(&mut child).await;
        let Some(reason) = reason else {
            drain.abort();
            return Ok(None);
        };
        let _ = drain.await;
        let error = logs.lock().await.back().cloned();
        Err(anyhow!(match error {
            Some(line) => format!("{reason}. {line}"),
            None => reason,
        }))
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
    fn only_applied_settings_are_used_for_server_recovery() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let root = tempfile::tempdir().unwrap();
                let initial = Settings {
                    storage_root: root.path().join("initial").to_string_lossy().into_owned(),
                    object_storage: None,
                    grpc_port: 0,
                    mcp_port: 0,
                    local_worker_concurrency: 1,
                    embedding: None,
                };
                let path = root.path().join("config.json");
                let host = Host::new(
                    path.clone(),
                    initial.clone(),
                    PathBuf::new(),
                    PathBuf::new(),
                );
                let old = host.inner.desired.lock().await.applied.generation;
                let mut saved = initial.clone();
                saved.grpc_port = 48001;
                host.save_settings(saved.clone()).await.unwrap();
                assert!(host.snapshot().await.0.restart_pending);
                assert_eq!(host.settings().await.grpc_port, 48001);
                assert_eq!(
                    host.inner.desired.lock().await.applied.settings.grpc_port,
                    0
                );
                assert_no_restart(&host, old).await;

                host.apply_settings().await;
                tokio::time::timeout(Duration::from_secs(1), host.wait_for_change(old))
                    .await
                    .unwrap();
                let applied = host.inner.desired.lock().await.applied.clone();
                assert_eq!(applied.settings.grpc_port, 48001);
                saved.grpc_port = 48002;
                host.save_settings(saved).await.unwrap();
                assert_eq!(
                    host.inner.desired.lock().await.applied.settings.grpc_port,
                    48001
                );
                assert_no_restart(&host, applied.generation).await;
                host.inner.state.write().await.generation = applied.generation;
                assert!(host.snapshot().await.0.restart_pending);
                let disk: Settings = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                assert_eq!(disk.grpc_port, 48002);

                host.apply_settings().await;
                let applied = host.inner.desired.lock().await.applied.generation;
                host.inner.state.write().await.generation = applied;
                assert!(!host.snapshot().await.0.restart_pending);
                host.apply_settings().await;
                assert_no_restart(&host, applied).await;
            });
    }

    #[cfg(unix)]
    #[test]
    fn applying_settings_interrupts_startup_and_waits_for_the_child() {
        assert_interrupts_startup(false);
    }

    #[cfg(unix)]
    #[test]
    fn quitting_interrupts_startup_and_waits_for_the_child() {
        assert_interrupts_startup(true);
    }

    #[cfg(unix)]
    fn assert_interrupts_startup(quit: bool) {
        use std::os::unix::fs::PermissionsExt;

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let root = tempfile::tempdir().unwrap();
                let started = root.path().join("started");
                let stopped = root.path().join("stopped");
                let server = root.path().join("sanctum-server");
                std::fs::write(
                    &server,
                    "#!/bin/sh\ncd -- \"$(dirname -- \"$0\")\" || exit 1\n: > started\nIFS= read -r request\n: > stopped\n",
                )
                .unwrap();
                std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o700)).unwrap();
                let initial = Settings {
                    storage_root: root.path().join("store").to_string_lossy().into_owned(),
                    object_storage: None,
                    grpc_port: 0,
                    mcp_port: 0,
                    local_worker_concurrency: 1,
                    embedding: None,
                };
                let host = Host::new(root.path().join("config.json"), initial.clone(), server, PathBuf::new());
                let worker = host.clone();
                let startup = tokio::spawn(async move { worker.start(initial, 1).await });

                tokio::time::timeout(Duration::from_secs(5), async {
                    while !started.exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("sidecar must begin waiting for startup");
                let quit = if quit {
                    let host = host.clone();
                    Some(tokio::spawn(async move { host.quit().await }))
                } else {
                    let mut changed = host.settings().await;
                    changed.grpc_port = 48001;
                    host.save_settings(changed).await.unwrap();
                    host.apply_settings().await;
                    None
                };

                let running = tokio::time::timeout(Duration::from_secs(5), startup)
                    .await
                    .expect("startup should stop promptly")
                    .unwrap()
                    .unwrap();
                assert!(running.is_none());
                assert!(stopped.exists(), "wait for the sidecar to receive EOF and exit");
                if let Some(quit) = quit {
                    host.inner.desired.lock().await.quit.take().unwrap().send(()).unwrap();
                    assert!(quit.await.unwrap());
                }
            });
    }

    async fn assert_no_restart(host: &Host, generation: u64) {
        assert!(
            tokio::time::timeout(Duration::from_millis(20), host.wait_for_change(generation))
                .await
                .is_err()
        );
    }

    #[test]
    fn storage_settings_preserve_filesystem_and_enable_the_managed_object_worker() {
        let root = tempfile::tempdir().unwrap();
        let old = serde_json::json!({
            "storage_root": root.path(),
            "grpc_port": 47550,
            "mcp_port": 47551,
            "local_worker_concurrency": 4
        });
        let settings: Settings = serde_json::from_value(old.clone()).unwrap();
        let config = settings.config().unwrap();
        assert!(config.object_storage.is_none());
        assert_eq!(config.mcp_listen.unwrap().port(), 47551);
        assert_eq!(config.local_worker_concurrency, 4);
        assert!(!config.standalone_object_worker);

        for storage in [
            serde_json::json!({"backend":"s3", "bucket":"sample", "region":"us-east-1", "prefix":"test", "endpoint":"http://127.0.0.1:9000", "allow_http":true}),
            serde_json::json!({"backend":"gcs", "bucket":"sample", "prefix":"test"}),
        ] {
            let mut value = old.clone();
            value["object_storage"] = storage;
            value["mcp_port"] = value["grpc_port"].clone();
            let settings: Settings = serde_json::from_value(value).unwrap();
            let config = settings.config().unwrap();
            assert!(config.object_storage.is_some());
            assert_eq!(config.grpc_listen, "127.0.0.1:47550".parse().unwrap());
            assert!(config.mcp_listen.is_none());
            assert!(config.hosted_worker_listen.is_none());
            assert!(config.standalone_object_worker);
            assert_eq!(config.storage_root, root.path());
        }
    }

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
