use std::{
    collections::{BTreeSet, HashSet},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use sanctum_core::{Error, ExternalSource, GitProvenance, Result, Scope};
use tempfile::TempDir;

use crate::{
    ImportEvent, ImportOptions, Service, TextUpload,
    local_import::{ImportProvenance, Selection},
    url_import::parse_url,
};

const FETCH_TIMEOUT: Duration = Duration::from_secs(300);

type ImportKey = (String, String, String);

#[derive(Clone, Default)]
pub(crate) struct ActiveImports(Arc<Mutex<HashSet<ImportKey>>>);

struct ActiveImport {
    imports: ActiveImports,
    key: ImportKey,
}

impl Drop for ActiveImport {
    fn drop(&mut self) {
        if let Ok(mut imports) = self.imports.0.lock() {
            imports.remove(&self.key);
        }
    }
}

pub enum GitRepository {
    Local(PathBuf),
    Remote(String),
}

pub struct GitImportEvent {
    pub import: ImportEvent,
    pub git: Option<GitProvenance>,
}

enum Repository {
    Local(PathBuf),
    Remote(reqwest::Url),
}

impl Repository {
    fn identity(&self) -> Result<&str> {
        match self {
            Self::Local(path) => path
                .to_str()
                .ok_or_else(|| Error::Invalid("Git repository path is not UTF-8".into())),
            Self::Remote(url) => Ok(url.as_str()),
        }
    }
}

impl Service {
    pub fn import_git(
        &self,
        scope: &Scope,
        repository: GitRepository,
        reference: &str,
        options: &ImportOptions,
        stopped: impl Fn() -> bool,
        mut emit: impl FnMut(GitImportEvent) -> bool,
    ) -> Result<()> {
        self.require_collection(scope)?;
        let select = Selection::new(options)?;
        let repository = match repository {
            GitRepository::Local(path) => Repository::Local(path.canonicalize()?),
            GitRepository::Remote(url) => {
                let url = parse_url(&url)?;
                if url.scheme() != "https" || url.query().is_some() {
                    return Err(Error::Invalid(
                        "Git remote requires an HTTPS URL without a query".into(),
                    ));
                }
                Repository::Remote(url)
            }
        };
        let identity = repository.identity()?;
        let reference = parse_ref(reference, matches!(repository, Repository::Local(_)))?;
        let namespace = format!("git:{}", git_namespace(identity, reference));
        let _active = self.begin_git_import(scope, &namespace)?;

        let mut temporary = None;
        let (path, revision) = match &repository {
            Repository::Local(path) => (path.as_path(), reference),
            Repository::Remote(url) => {
                let authorization = self.url_importer.authorization(url)?;
                let dir = temporary.insert(tempfile::tempdir()?);
                fetch(
                    dir,
                    url.as_str(),
                    reference,
                    authorization.as_deref(),
                    &stopped,
                )?;
                (dir.path(), "FETCH_HEAD")
            }
        };
        let commit = resolve_commit(path, revision)?;
        let mut tree = git(path)
            .args(["ls-tree", "-r", "-l", "-z", "--full-tree", &commit])
            .stdout(Stdio::piped())
            .spawn()
            .map(GitChild)
            .map_err(git_spawn_error)?;
        let mut tree_output = tree.stdout();
        let mut blobs = None;
        let mut seen = BTreeSet::new();
        let mut had_errors = false;
        while let Some(record) = read_record(&mut tree_output, 0, 8192)? {
            if stopped() {
                return Ok(());
            }
            let Some((file, blob, size)) = regular_blob(&record)? else {
                continue;
            };
            if !select.file_selected(Path::new(file)) {
                continue;
            }
            seen.insert(file.to_owned());
            let provenance = GitProvenance {
                repository: identity.into(),
                reference: reference.into(),
                commit: commit.clone(),
                path: file.into(),
                blob: blob.into(),
            };
            let staged = if size > self.max_source_bytes {
                Err(Error::Invalid(
                    "Git blob exceeds the configured upload limit".into(),
                ))
            } else {
                self.stage_upload(
                    scope.clone(),
                    TextUpload {
                        name: file
                            .rsplit('/')
                            .next()
                            .expect("Git paths have a filename")
                            .into(),
                        external: Some(ExternalSource {
                            namespace: namespace.clone(),
                            id: file.into(),
                        }),
                        replacement: None,
                        retain_original: options.retain_original,
                        initial_metadata: Vec::new(),
                        initial_chunk_metadata: Vec::new(),
                    },
                )
            };
            let import = match staged.and_then(|staged| {
                if let Some(event) = staged.reuse_git_import(&provenance)? {
                    return Ok(event);
                }
                if blobs.is_none() {
                    blobs = Some(BlobReader::new(path)?);
                }
                let reader = blobs.as_mut().expect("batch reader initialized");
                let (bytes, hash) = match reader.copy(blob, size, &staged, &stopped) {
                    Ok(result) => result,
                    Err(error) => {
                        // A truncated frame cannot be reused for a different object.
                        blobs = None;
                        return Err(error);
                    }
                };
                staged.enqueue_import(bytes, hash, Some(ImportProvenance::Git(provenance.clone())))
            }) {
                Ok(event) => event,
                Err(error) => {
                    had_errors = true;
                    ImportEvent::failed(file, &error)
                }
            };
            if !emit(GitImportEvent {
                import,
                git: Some(provenance),
            }) {
                return Ok(());
            }
        }
        if !tree.wait()?.success() {
            return Err(Error::Invalid(
                "Git could not enumerate the selected commit".into(),
            ));
        }
        if had_errors {
            return Ok(());
        }
        let mut cursor = String::new();
        loop {
            let active = self
                .database(scope)?
                .imported_sources(&namespace, &cursor)?;
            let pending = self
                .queue
                .pending_import_paths(scope, &namespace, &cursor)?;
            let paths: BTreeSet<_> = active.into_iter().chain(pending).collect();
            if paths.is_empty() {
                return Ok(());
            }
            for file in paths.into_iter().take(128) {
                if stopped() {
                    return Ok(());
                }
                if !seen.contains(&file)
                    && select.file_selected(Path::new(&file))
                    && let Some(import) =
                        self.missing_import(scope, &namespace, &file, select.prune_missing)?
                    && !emit(GitImportEvent { import, git: None })
                {
                    return Ok(());
                }
                cursor = file;
            }
        }
    }

    fn begin_git_import(&self, scope: &Scope, namespace: &str) -> Result<ActiveImport> {
        let key = (
            scope.tenant.to_string(),
            scope.collection.to_string(),
            namespace.into(),
        );
        let mut imports = self
            .git_imports
            .0
            .lock()
            .map_err(|_| Error::Busy("Git import coordinator is unavailable".into()))?;
        if !imports.insert(key.clone()) {
            return Err(Error::Busy(
                "this repository and ref are already being imported into the collection".into(),
            ));
        }
        Ok(ActiveImport {
            imports: self.git_imports.clone(),
            key,
        })
    }
}

fn git_namespace(identity: &str, reference: &str) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(identity.as_bytes());
    hash.update(b"\0");
    hash.update(reference.as_bytes());
    hash.finalize().to_hex().to_string()
}

fn parse_ref(reference: &str, local: bool) -> Result<&str> {
    let reference = if reference.is_empty() {
        "HEAD"
    } else {
        reference
    };
    if reference == "HEAD" || (local && object_id(reference)) {
        return Ok(reference);
    }
    if !(reference.starts_with("refs/heads/") || reference.starts_with("refs/tags/"))
        || reference.len() > 1024
        || reference.contains("..")
        || reference.contains("@{")
        || reference
            .bytes()
            .any(|byte| byte.is_ascii_control() || b" :?*^~[\\".contains(&byte))
    {
        return Err(Error::Invalid("select HEAD, refs/heads/NAME, or refs/tags/NAME; local imports also accept a full commit ID".into()));
    }
    Ok(reference)
}

fn resolve_commit(path: &Path, revision: &str) -> Result<String> {
    let output = git(path)
        .args([
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{revision}^{{commit}}"),
        ])
        .output()
        .map_err(git_spawn_error)?;
    if let Ok(text) = std::str::from_utf8(&output.stdout)
        && output.status.success()
        && object_id(text.trim_end())
    {
        return Ok(text.trim_end().into());
    }
    Err(Error::NotFound(
        "Git repository or selected commit could not be resolved".into(),
    ))
}

fn fetch(
    dir: &TempDir,
    url: &str,
    reference: &str,
    authorization: Option<&str>,
    stopped: &impl Fn() -> bool,
) -> Result<()> {
    let path = dir.path();
    if !git(path)
        .args(["init", "--bare", "--quiet", "--template="])
        .stdout(Stdio::null())
        .status()
        .map_err(git_spawn_error)?
        .success()
    {
        return Err(Error::Busy(
            "could not initialize Git import workspace".into(),
        ));
    }
    let mut fetch = git(path);
    fetch.args(["-c", "protocol.https.allow=always"]);
    if let Some(ca) = std::env::var_os("GIT_SSL_CAINFO") {
        fetch.env("GIT_SSL_CAINFO", ca);
    }
    if let Some(authorization) = authorization {
        // Keep the credential out of process arguments and repository config.
        fetch
            .args(["--config-env=http.extraHeader=SANCTUM_GIT_AUTHORIZATION"])
            .env(
                "SANCTUM_GIT_AUTHORIZATION",
                format!("Authorization: {authorization}"),
            );
    }
    let mut child = fetch
        .args([
            "fetch",
            "--quiet",
            "--no-tags",
            "--no-recurse-submodules",
            "--depth=1",
            "--",
            url,
            reference,
        ])
        .stdout(Stdio::null())
        .spawn()
        .map(GitChild)
        .map_err(git_spawn_error)?;
    let deadline = Instant::now() + FETCH_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return if status.success() {
                Ok(())
            } else {
                Err(Error::Busy(format!("Git HTTPS fetch failed ({status})")))
            };
        }
        if stopped() || Instant::now() >= deadline {
            return Err(Error::Busy("Git HTTPS fetch stopped or timed out".into()));
        }
        std::thread::sleep(Duration::from_millis(40));
    }
}

fn git(path: &Path) -> Command {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os() {
        if key
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("GIT_")
        {
            command.env_remove(key);
        }
    }
    command
        .current_dir(path)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        )
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_NO_LAZY_FETCH", "1")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .args([
            "-c",
            "protocol.allow=never",
            "-c",
            "credential.helper=",
            "-c",
            "http.followRedirects=false",
            "-c",
            "http.proxy=",
            "-c",
            "http.lowSpeedLimit=1",
            "-c",
            "http.lowSpeedTime=30",
            "-c",
            "core.hooksPath=",
            "-c",
            "gc.auto=0",
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    command
}

fn git_spawn_error(error: io::Error) -> Error {
    if error.kind() == io::ErrorKind::NotFound {
        Error::Invalid("Git import requires a git executable and an accessible repository".into())
    } else {
        error.into()
    }
}

struct GitChild(Child);

impl GitChild {
    fn stdout(&mut self) -> BufReader<ChildStdout> {
        BufReader::new(self.0.stdout.take().expect("Git stdout is piped"))
    }

    fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        self.0.wait()
    }

    fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        self.0.try_wait()
    }
}

impl Drop for GitChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

struct BlobReader {
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    _child: GitChild,
}

impl BlobReader {
    fn new(path: &Path) -> Result<Self> {
        let mut child = git(path)
            .args(["cat-file", "--batch"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map(GitChild)
            .map_err(git_spawn_error)?;
        let input = child.0.stdin.take().expect("Git stdin is piped");
        let output = child.stdout();
        Ok(Self {
            input,
            output,
            _child: child,
        })
    }

    fn copy(
        &mut self,
        blob: &str,
        expected_size: u64,
        staged: &crate::StagedUpload,
        stopped: &impl Fn() -> bool,
    ) -> Result<(u64, String)> {
        writeln!(self.input, "{blob}")?;
        let header = read_record(&mut self.output, b'\n', 256)?.ok_or_else(invalid_output)?;
        let header = std::str::from_utf8(&header).map_err(|_| invalid_output())?;
        let mut fields = header.split_ascii_whitespace();
        if fields.next() != Some(blob) || fields.next() != Some("blob") {
            return Err(invalid_output());
        }
        let len = fields
            .next()
            .and_then(|size| size.parse::<u64>().ok())
            .ok_or_else(invalid_output)?;
        if len != expected_size {
            return Err(invalid_output());
        }
        let input = (&mut self.output).take(len);
        let (size, hash) = staged.copy_import(input, stopped)?;
        if size != len {
            return Err(invalid_output());
        }
        let mut newline = [0];
        self.output.read_exact(&mut newline)?;
        if newline != [b'\n'] {
            return Err(invalid_output());
        }
        Ok((size, hash))
    }
}

fn read_record(reader: &mut impl BufRead, delimiter: u8, limit: u64) -> Result<Option<Vec<u8>>> {
    let mut record = Vec::new();
    if reader.take(limit).read_until(delimiter, &mut record)? == 0 {
        return Ok(None);
    }
    if record.pop() != Some(delimiter) {
        return Err(invalid_output());
    }
    Ok(Some(record))
}

fn regular_blob(record: &[u8]) -> Result<Option<(&str, &str, u64)>> {
    let Some(tab) = record.iter().position(|byte| *byte == b'\t') else {
        return Err(invalid_output());
    };
    let header = std::str::from_utf8(&record[..tab]).map_err(|_| invalid_output())?;
    let mut fields = header.split_ascii_whitespace();
    if !matches!(fields.next(), Some("100644" | "100755")) || fields.next() != Some("blob") {
        return Ok(None);
    }
    let blob = fields
        .next()
        .filter(|value| object_id(value))
        .ok_or_else(invalid_output)?;
    let size = fields
        .next()
        .and_then(|size| size.parse::<u64>().ok())
        .ok_or_else(invalid_output)?;
    let Ok(path) = std::str::from_utf8(&record[tab + 1..]) else {
        return Ok(None);
    };
    if path.is_empty()
        || path
            .split('/')
            .any(|segment| matches!(segment, "" | "." | "..") || segment.contains('\\'))
    {
        return Ok(None);
    }
    Ok(Some((path, blob, size)))
}

fn object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn invalid_output() -> Error {
    Error::Invalid("Git returned an invalid or incomplete object record".into())
}
