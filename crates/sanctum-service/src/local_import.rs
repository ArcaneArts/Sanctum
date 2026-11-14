use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use sanctum_core::{Error, ExternalSource, GitProvenance, Result, Scope, UrlProvenance, is_rtf};
use sanctum_db_sqlite::{CollectionDb, SourceIdentity};
use uuid::Uuid;
use walkdir::WalkDir;

use crate::{Service, StagedUpload, TextUpload, local_jobs::new_job};

#[derive(Default, serde::Deserialize, serde::Serialize)]
pub struct ImportOptions {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub prune_missing: bool,
    pub retain_original: bool,
}

#[derive(Clone, Copy)]
pub enum ImportState {
    Queued,
    Pending,
    Unchanged,
    Missing,
    Deleted,
    Failed,
}

pub struct ImportEvent {
    pub path: String,
    pub state: ImportState,
    pub source: Option<Uuid>,
    pub revision: Option<Uuid>,
    pub operation: Option<Uuid>,
    pub message: Option<String>,
}

impl ImportEvent {
    fn new(path: &str, state: ImportState) -> Self {
        Self {
            path: path.into(),
            state,
            source: None,
            revision: None,
            operation: None,
            message: None,
        }
    }

    pub(crate) fn failed(path: &str, error: &impl std::fmt::Display) -> Self {
        Self {
            message: Some(error.to_string()),
            ..Self::new(path, ImportState::Failed)
        }
    }
}

pub(crate) struct Selection {
    include: GlobSet,
    exclude: GlobSet,
    pub(crate) prune_missing: bool,
}

impl Selection {
    pub(crate) fn new(options: &ImportOptions) -> Result<Self> {
        Ok(Self {
            include: globs(&options.include)?,
            exclude: globs(&options.exclude)?,
            prune_missing: options.prune_missing,
        })
    }

    fn directory_excluded(&self, relative: &Path) -> bool {
        relative
            .file_name()
            .is_some_and(|name| matches!(name.to_str(), Some(".git" | "node_modules")))
            || self.exclude.is_match(relative)
    }

    pub(crate) fn file_selected(&self, relative: &Path) -> bool {
        !self.exclude.is_match(relative)
            && !relative
                .ancestors()
                .skip(1)
                .any(|path| self.directory_excluded(path))
            && if self.include.is_empty() {
                default_text(relative)
            } else {
                self.include.is_match(relative)
            }
    }
}

impl Service {
    pub(crate) fn validate_watch(
        &self,
        scope: &Scope,
        path: &Path,
        options: &ImportOptions,
    ) -> Result<PathBuf> {
        self.require_collection(scope)?;
        if !fs::symlink_metadata(path)?.file_type().is_dir() {
            return Err(Error::Invalid("watch path must be a directory".into()));
        }
        let root = fs::canonicalize(path)?;
        if root.starts_with(self.files.root()) {
            return Err(Error::Invalid(
                "cannot watch Sanctum's managed storage".into(),
            ));
        }
        Selection::new(options)?;
        Ok(root)
    }

    /// Import a path on the local server's filesystem. Returning false stops the scan.
    pub fn import_local(
        &self,
        scope: &Scope,
        path: &Path,
        options: &ImportOptions,
        emit: impl FnMut(ImportEvent) -> bool,
    ) -> Result<()> {
        self.import_local_until(scope, path, options, || false, emit)
    }

    pub(crate) fn import_local_until(
        &self,
        scope: &Scope,
        path: &Path,
        options: &ImportOptions,
        stopped: impl Fn() -> bool,
        mut emit: impl FnMut(ImportEvent) -> bool,
    ) -> Result<()> {
        self.require_collection(scope)?;
        let file_type = fs::symlink_metadata(path)?.file_type();
        if !(file_type.is_dir() || file_type.is_file()) {
            return Err(Error::Invalid("select a regular file or directory".into()));
        }
        if options.prune_missing && !file_type.is_dir() {
            return Err(Error::Invalid("prune_missing requires a directory".into()));
        }
        let root = fs::canonicalize(path)?;
        if root.starts_with(self.files.root()) {
            return Err(Error::Invalid(
                "cannot import from Sanctum's managed storage".into(),
            ));
        }
        let select = Selection::new(options)?;
        let namespace = format!(
            "local-fs:{}",
            blake3::hash(root.as_os_str().as_encoded_bytes()).to_hex(),
        );
        let base = if file_type.is_dir() {
            root.as_path()
        } else {
            root.parent()
                .ok_or_else(|| Error::Invalid("file path has no parent".into()))?
        };
        let walk = WalkDir::new(&root)
            .follow_links(false)
            .follow_root_links(false)
            .into_iter()
            .filter_entry(|entry| {
                let relative = entry
                    .path()
                    .strip_prefix(base)
                    .expect("entry is under base");
                let excluded_directory = entry.depth() > 0
                    && entry.file_type().is_dir()
                    && select.directory_excluded(relative);
                !(entry.path().starts_with(self.files.root()) || excluded_directory)
            });
        let mut had_errors = false;
        for entry in walk {
            if stopped() {
                return Ok(());
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    had_errors = true;
                    let path = error.path().unwrap_or(&root).display().to_string();
                    if !emit(ImportEvent::failed(&path, &error)) {
                        return Ok(());
                    }
                    continue;
                }
            };
            let relative = entry
                .path()
                .strip_prefix(base)
                .expect("entry is under base");
            if !entry.file_type().is_file() || !select.file_selected(relative) {
                continue;
            }
            let id = match relative_id(relative) {
                Ok(id) => id,
                Err(error) => {
                    had_errors = true;
                    if !emit(ImportEvent::failed(&relative.display().to_string(), &error)) {
                        return Ok(());
                    }
                    continue;
                }
            };
            let event = match self.import_file(
                scope,
                entry.path(),
                &namespace,
                &id,
                options.retain_original,
                &stopped,
            ) {
                Ok(event) => event,
                Err(error) => {
                    had_errors = true;
                    ImportEvent::failed(&id, &error)
                }
            };
            if !emit(event) {
                return Ok(());
            }
        }
        if file_type.is_dir() && !had_errors && !stopped() {
            self.reconcile_import(scope, &root, &namespace, &select, &stopped, emit)?;
        }
        Ok(())
    }

    fn import_file(
        &self,
        scope: &Scope,
        path: &Path,
        namespace: &str,
        id: &str,
        retain_original: bool,
        stopped: impl Fn() -> bool,
    ) -> Result<ImportEvent> {
        let external = ExternalSource {
            namespace: namespace.into(),
            id: id.into(),
        };
        let staged = self.stage_upload(
            scope.clone(),
            TextUpload {
                name: id.rsplit('/').next().unwrap_or(id).into(),
                external: Some(external.clone()),
                replacement: None,
                retain_original,
                initial_metadata: Vec::new(),
                initial_chunk_metadata: Vec::new(),
            },
        )?;
        let mut input = File::open(path)?;
        let before = input.metadata()?;
        let (bytes, digest) = staged.copy_import(&mut input, &stopped)?;
        let after = input.metadata()?;
        if bytes != before.len()
            || bytes != after.len()
            || before.modified().ok() != after.modified().ok()
        {
            return Err(Error::Busy(
                "source changed while it was being imported".into(),
            ));
        }
        staged.enqueue_import(bytes, digest, None)
    }
}

pub(crate) enum ImportProvenance {
    Url(UrlProvenance),
    Git(GitProvenance),
}

impl StagedUpload {
    pub(crate) fn copy_import(
        &self,
        mut input: impl Read,
        stopped: &impl Fn() -> bool,
    ) -> Result<(u64, String)> {
        let mut output = self.reopen()?;
        let mut buffer = [0; 64 * 1024];
        let mut hasher = blake3::Hasher::new();
        let mut bytes = 0;
        loop {
            if stopped() {
                return Err(Error::Busy("import scan stopped".into()));
            }
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            bytes += read as u64;
            if bytes > self.service.max_source_bytes {
                return Err(Error::Invalid(
                    "file exceeds the configured upload limit".into(),
                ));
            }
            hasher.update(&buffer[..read]);
            output.write_all(&buffer[..read])?;
        }
        Ok((bytes, hasher.finalize().to_hex().to_string()))
    }

    pub(crate) fn enqueue_import(
        self,
        bytes: u64,
        hash: String,
        provenance: Option<ImportProvenance>,
    ) -> Result<ImportEvent> {
        let service = self.service.clone();
        let _reader = service
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        let scope = self.scope.clone();
        service.local_writes.run(&scope, || {
            self.enqueue_import_locked(bytes, hash, provenance)
        })
    }

    fn enqueue_import_locked(
        self,
        bytes: u64,
        hash: String,
        provenance: Option<ImportProvenance>,
    ) -> Result<ImportEvent> {
        let scope = &self.scope;
        let external = self.import_external();
        let id = external.id.clone();
        let database = self.service.database(scope)?;
        let (current, pending) = self.import_heads(&database)?;
        if let Some(event) = self.reuse_import(&current, &pending, |digest, _| {
            digest == Some(hash.as_str())
        })? {
            return Ok(event);
        }
        let source = current
            .as_ref()
            .map(|source| source.source)
            .or_else(|| pending.map(|job| job.source))
            .unwrap_or_else(Uuid::new_v4);
        let name = self.upload.name.clone();
        let mut job = new_job(
            scope.clone(),
            name,
            Some(external.clone()),
            source,
            bytes,
            current.is_some(),
        );
        job.import_hash = Some(hash);
        match provenance {
            Some(ImportProvenance::Url(url)) => job.url = Some(url),
            Some(ImportProvenance::Git(git)) => job.git = Some(git),
            None => {}
        }
        let job = self.publish_job_locked(job, None)?;
        Ok(ImportEvent {
            source: Some(job.source),
            revision: Some(job.revision),
            operation: Some(job.id),
            ..ImportEvent::new(&id, ImportState::Queued)
        })
    }

    pub(crate) fn reuse_git_import(&self, git: &GitProvenance) -> Result<Option<ImportEvent>> {
        let service = &self.service;
        let _reader = service
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        service.local_writes.run(&self.scope, || {
            let database = service.database(&self.scope)?;
            let (current, pending) = self.import_heads(&database)?;
            self.reuse_import(&current, &pending, |_, recorded| {
                recorded.is_some_and(|recorded| {
                    recorded.repository == git.repository
                        && recorded.reference == git.reference
                        && recorded.path == git.path
                        && recorded.blob == git.blob
                })
            })
        })
    }

    fn import_external(&self) -> &ExternalSource {
        self.upload
            .external
            .as_ref()
            .expect("imports have an external ID")
    }

    fn import_heads(
        &self,
        database: &CollectionDb,
    ) -> Result<(Option<SourceIdentity>, Option<crate::Job>)> {
        let external = self.import_external();
        let current = match database.find_source(None, Some(external)) {
            Ok(current) => Some(current),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        let pending = self
            .service
            .queue
            .pending_external(&self.scope, external)?
            .filter(|job| {
                job.import_hash.is_some()
                    && match &current {
                        Some(source) => source.source == job.source,
                        None => !job.replacement,
                    }
            });
        Ok((current, pending))
    }

    fn reuse_import(
        &self,
        current: &Option<SourceIdentity>,
        pending: &Option<crate::Job>,
        matches: impl Fn(Option<&str>, Option<&GitProvenance>) -> bool,
    ) -> Result<Option<ImportEvent>> {
        let id = &self.import_external().id;
        let retain_original = self.upload.retain_original;
        let extract_rtf = is_rtf(&self.upload.name);
        if let Some(job) = pending.as_ref().filter(|job| {
            matches(job.import_hash.as_deref(), job.git.as_ref())
                && job.retain_original == retain_original
                && job.extract_rtf == extract_rtf
        }) {
            return Ok(Some(ImportEvent {
                source: Some(job.source),
                revision: Some(job.revision),
                operation: Some(job.id),
                ..ImportEvent::new(id, ImportState::Pending)
            }));
        }
        if let Some(source) = current.as_ref().filter(|source| {
            matches(Some(&source.hash), source.git.as_ref())
                && source.extracted == extract_rtf
                && (!extract_rtf || source.retain_original == retain_original)
        }) {
            if let Some(pending) = pending {
                self.service.queue.supersede_import(&self.scope, pending)?;
            }
            return Ok(Some(ImportEvent {
                source: Some(source.source),
                revision: Some(source.revision),
                ..ImportEvent::new(id, ImportState::Unchanged)
            }));
        }
        Ok(None)
    }
}

impl Service {
    fn reconcile_import(
        &self,
        scope: &Scope,
        root: &Path,
        namespace: &str,
        select: &Selection,
        stopped: &impl Fn() -> bool,
        mut emit: impl FnMut(ImportEvent) -> bool,
    ) -> Result<()> {
        let mut cursor = String::new();
        loop {
            if stopped() {
                return Ok(());
            }
            if !fs::symlink_metadata(root)?.is_dir() {
                return Err(Error::Busy("import root is no longer a directory".into()));
            }
            let active = self.database(scope)?.imported_sources(namespace, &cursor)?;
            let pending = self.queue.pending_import_paths(scope, namespace, &cursor)?;
            let mut paths: Vec<_> = active.into_iter().chain(pending).collect();
            paths.sort_unstable();
            paths.dedup();
            paths.truncate(128);
            if paths.is_empty() {
                return Ok(());
            }
            for id in paths {
                if stopped() {
                    return Ok(());
                }
                let relative = PathBuf::from(&id);
                if relative_id(&relative).is_ok_and(|valid| valid == id)
                    && select.file_selected(&relative)
                {
                    match regular_import_path(root, &relative) {
                        Ok(true) => {}
                        Ok(false) => {
                            let event = match self.missing_import(
                                scope,
                                namespace,
                                &id,
                                select.prune_missing,
                            ) {
                                Ok(event) => event,
                                Err(error) => Some(ImportEvent::failed(&id, &error)),
                            };
                            if let Some(event) = event
                                && !emit(event)
                            {
                                return Ok(());
                            }
                        }
                        Err(error) => {
                            if !emit(ImportEvent::failed(&id, &error)) {
                                return Ok(());
                            }
                        }
                    }
                }
                cursor = id;
            }
        }
    }

    pub(crate) fn missing_import(
        &self,
        scope: &Scope,
        namespace: &str,
        id: &str,
        prune: bool,
    ) -> Result<Option<ImportEvent>> {
        let _reader = self
            .retrieval
            .read()
            .map_err(|_| Error::Busy("retrieval lock is unavailable".into()))?;
        self.local_writes.run(scope, || {
            self.missing_import_locked(scope, namespace, id, prune)
        })
    }

    fn missing_import_locked(
        &self,
        scope: &Scope,
        namespace: &str,
        id: &str,
        prune: bool,
    ) -> Result<Option<ImportEvent>> {
        let mut database = self.database(scope)?;
        let writer = database.write()?;
        let external = ExternalSource {
            namespace: namespace.into(),
            id: id.into(),
        };
        let active = match writer.find_source(None, Some(&external)) {
            Ok(source) => Some(source),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        let pending = self
            .queue
            .pending_external(scope, &external)?
            .filter(|job| job.import_hash.is_some());
        if active.is_none() && pending.is_none() {
            return Ok(None);
        }
        let mut event = ImportEvent::new(id, ImportState::Missing);
        if let Some(active) = active {
            event.source = Some(active.source);
            event.revision = Some(active.revision);
            if prune {
                writer.delete_source(active.source, Some(active.revision))?;
            }
        }
        if let Some(pending) = pending {
            event.source = event.source.or(Some(pending.source));
            event.revision = event.revision.or(Some(pending.revision));
            event.operation = Some(pending.id);
            if prune {
                self.queue.supersede_import(scope, &pending)?;
            }
        }
        if prune {
            writer.commit()?;
            event.state = ImportState::Deleted;
        }
        Ok(Some(event))
    }
}

fn regular_import_path(root: &Path, relative: &Path) -> std::io::Result<bool> {
    let mut path = root.to_path_buf();
    let mut components = relative.components().peekable();
    while let Some(component) = components.next() {
        path.push(component);
        let kind = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata.file_type(),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        if components.peek().is_none() {
            return Ok(kind.is_file());
        }
        if !kind.is_dir() {
            return Ok(false);
        }
    }
    Ok(false)
}

fn relative_id(path: &Path) -> Result<String> {
    let mut names = Vec::new();
    for part in path.components() {
        let Component::Normal(part) = part else {
            return Err(Error::Invalid(
                "import path must be relative to its scan root".into(),
            ));
        };
        names.push(
            part.to_str()
                .ok_or_else(|| Error::Invalid("import path is not UTF-8".into()))?,
        );
    }
    if names.is_empty() {
        return Err(Error::Invalid("empty import path".into()));
    }
    Ok(names.join("/"))
}

fn globs(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
            .map_err(|error| Error::Invalid(format!("invalid import glob: {error}")))?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|error| Error::Invalid(format!("invalid import globs: {error}")))
}

pub(crate) fn default_text(path: &Path) -> bool {
    if path.file_name().is_some_and(|name| {
        matches!(
            name.to_str(),
            Some("README" | "LICENSE" | "Makefile" | "Dockerfile" | ".gitignore")
        )
    }) {
        return true;
    }
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "txt"
                    | "rtf"
                    | "md"
                    | "markdown"
                    | "rst"
                    | "adoc"
                    | "org"
                    | "log"
                    | "csv"
                    | "tsv"
                    | "rs"
                    | "java"
                    | "kt"
                    | "scala"
                    | "go"
                    | "py"
                    | "rb"
                    | "php"
                    | "js"
                    | "jsx"
                    | "ts"
                    | "tsx"
                    | "mjs"
                    | "cjs"
                    | "c"
                    | "cc"
                    | "cpp"
                    | "h"
                    | "hpp"
                    | "cs"
                    | "swift"
                    | "sh"
                    | "bash"
                    | "zsh"
                    | "fish"
                    | "ps1"
                    | "sql"
                    | "proto"
                    | "html"
                    | "css"
                    | "scss"
                    | "xml"
                    | "svg"
                    | "toml"
                    | "yaml"
                    | "yml"
                    | "json"
                    | "jsonl"
                    | "jsonc"
                    | "ini"
                    | "cfg"
                    | "conf"
                    | "properties"
                    | "gradle"
                    | "tf"
                    | "hcl"
            )
        })
}
