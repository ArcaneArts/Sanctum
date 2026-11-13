use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use sanctum_core::{Error, ExternalSource, Result, Scope};
use uuid::Uuid;
use walkdir::WalkDir;

use crate::{Service, TextUpload, local_jobs::new_job};

#[derive(Default)]
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

    fn failed(path: &str, error: &impl std::fmt::Display) -> Self {
        Self {
            message: Some(error.to_string()),
            ..Self::new(path, ImportState::Failed)
        }
    }
}

struct Selection {
    include: GlobSet,
    exclude: GlobSet,
}

impl Selection {
    fn new(options: &ImportOptions) -> Result<Self> {
        Ok(Self {
            include: globs(&options.include)?,
            exclude: globs(&options.exclude)?,
        })
    }

    fn directory_excluded(&self, relative: &Path) -> bool {
        relative
            .file_name()
            .is_some_and(|name| matches!(name.to_str(), Some(".git" | "node_modules")))
            || self.exclude.is_match(relative)
    }

    fn file_selected(&self, relative: &Path) -> bool {
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
    /// Import a path on the local server's filesystem. Returning false stops the scan.
    pub fn import_local(
        &self,
        scope: &Scope,
        path: &Path,
        options: &ImportOptions,
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
        if file_type.is_dir() && !had_errors {
            self.reconcile_import(
                scope,
                &root,
                &namespace,
                &select,
                options.prune_missing,
                emit,
            )?;
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
            },
        )?;
        let mut input = File::open(path)?;
        let mut output = staged.reopen()?;
        let mut buffer = [0; 64 * 1024];
        let mut hasher = blake3::Hasher::new();
        let mut bytes = 0;
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            bytes += read as u64;
            if bytes > self.max_source_bytes {
                return Err(Error::Invalid(
                    "file exceeds the configured upload limit".into(),
                ));
            }
            hasher.update(&buffer[..read]);
            output.write_all(&buffer[..read])?;
        }
        drop(output);
        let hash = hasher.finalize().to_hex().to_string();
        let mut database = self.database(scope)?;
        let writer = database.write()?;
        let current = match writer.find_source(None, Some(&external)) {
            Ok(current) => Some(current),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        let pending = self
            .queue
            .pending_external(scope, &external)?
            .filter(|job| {
                job.import_hash.is_some()
                    && match &current {
                        Some(source) => source.source == job.source,
                        None => !job.replacement,
                    }
            });
        if let Some(pending) = pending.as_ref().filter(|job| {
            job.import_hash.as_ref() == Some(&hash) && job.retain_original == retain_original
        }) {
            return Ok(ImportEvent {
                source: Some(pending.source),
                revision: Some(pending.revision),
                operation: Some(pending.id),
                ..ImportEvent::new(id, ImportState::Pending)
            });
        }
        if let Some(current) = current.as_ref().filter(|source| {
            source.hash == hash && (!source.extracted || source.retain_original == retain_original)
        }) {
            if let Some(pending) = &pending {
                self.queue.supersede_import(scope, pending)?;
            }
            return Ok(ImportEvent {
                source: Some(current.source),
                revision: Some(current.revision),
                ..ImportEvent::new(id, ImportState::Unchanged)
            });
        }
        let source = current
            .as_ref()
            .map(|source| source.source)
            .or_else(|| pending.map(|job| job.source))
            .unwrap_or_else(Uuid::new_v4);
        let name = staged.upload.name.clone();
        let mut job = new_job(
            scope.clone(),
            name,
            Some(external),
            source,
            bytes,
            current.is_some(),
        );
        job.import_hash = Some(hash);
        let job = staged.publish_job(job)?;
        drop(writer);
        Ok(ImportEvent {
            source: Some(job.source),
            revision: Some(job.revision),
            operation: Some(job.id),
            ..ImportEvent::new(id, ImportState::Queued)
        })
    }

    fn reconcile_import(
        &self,
        scope: &Scope,
        root: &Path,
        namespace: &str,
        select: &Selection,
        prune: bool,
        mut emit: impl FnMut(ImportEvent) -> bool,
    ) -> Result<()> {
        let mut cursor = String::new();
        loop {
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
                let relative = PathBuf::from(&id);
                if relative_id(&relative).is_ok_and(|valid| valid == id)
                    && select.file_selected(&relative)
                {
                    match fs::symlink_metadata(root.join(&relative)) {
                        Ok(_) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            let event = match self.missing_import(scope, namespace, &id, prune) {
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

    fn missing_import(
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

fn default_text(path: &Path) -> bool {
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
