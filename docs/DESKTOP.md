# Sanctum desktop

Install Node.js, PNPM, the repository's Rust toolchain, and the native
[Tauri prerequisites](https://v2.tauri.app/start/prerequisites/) for your OS.
From the repository root, start the tray app and its managed server.

```sh
pnpm install --frozen-lockfile
pnpm devtop
```

The development command builds the server and CLI sidecars, starts Vite, and
launches Tauri. Tauri owns the server process. The desktop host prevents a
second window process and the server holds exclusive ownership of its data
directory. Changes to the Rust crates, Protobuf sources, or workspace
manifests rebuild the sidecars before Tauri restarts the app. Vite handles
frontend edits. [Tauri development file watching](https://v2.tauri.app/develop/).

## First use

1. In **Settings**, choose **Local folder**, select a folder, and save the changes. Select
   **Restart server to apply**. Sanctum defaults to a folder inside your OS
   application data directory.
2. To enable embeddings, select **Ollama** and enter the endpoint, model tag,
   model revision, and dimensions. Use the digest shown by `ollama list` as
   the revision for a local model. Save and restart the server.
3. In **Activity**, create and select a collection, then import a file or
   folder. Track accepted jobs and current embedding coverage. Select
   **Refresh** under Storage to measure the selected collection or the
   entire local store. Larger scans show how many files or objects have
   been counted. The last completed total shows its measurement time.
4. In **Connect**, copy the Streamable HTTP URL or stdio configuration.
   Add it to your MCP client and use `list_collections`, `search`, and
   `fetch_chunk` to retrieve cited text. [MCP tools and examples](MCP.md).

Sanctum saves settings as `config.json` in the OS application configuration
directory. A storage-folder change opens the selected location after restart.
The Rust host serializes the active configuration into the child process's
`SANCTUM_CONFIG_JSON`. Choose port zero to assign an ephemeral loopback port.
The Connect page always shows the active MCP address.

The embedding settings also support **OpenAI-compatible** and **Google Cloud**.
For a bearer-authenticated endpoint, enter the environment variable name
and launch Sanctum from an environment where that variable is set. For
Google, configure Application Default Credentials and enter the project and
location. The settings file stores the provider parameters and the variable
name. [Embedding provider configuration](LOCAL_API.md#openai-compatible-embeddings).

Closing the window leaves the server running. Use **Open Sanctum** in the
tray menu to show it again. **Quit Sanctum** closes the host's control pipe,
allows the server to stop, and exits. Accepted work resumes the next time
Sanctum opens.

## Object storage

In **Settings**, select **S3-compatible storage** or **Google Cloud Storage**.
Enter a bucket and an optional prefix dedicated to this server. For S3,
enter the region and, for an S3-compatible server, its endpoint. MinIO
uses path-style bucket URLs by default. Enable HTTP only for an endpoint
that uses it. Launch the app with the AWS credentials available to its
process or with Google Application Default Credentials for GCS.

Save the configuration and select **Restart server to apply**. The sidecar
runs the standalone object worker and serves gRPC and MCP on the same port.
The **Connect** page shows that active address. In **Activity**, use
**Upload file** to stream a selected text or RTF file and track indexing and
embedding. Storage refresh measures the selected collection or the configured
object store. The filesystem backend retains directory imports and queue pause
controls. [Standalone worker settings](STORAGE.md#hosted-server-configuration).

## Development validation

```sh
pnpm check
pnpm desktop:check
```

`desktop:check` builds the sidecars and frontend and checks the Tauri Rust
package. To exercise the UI with disposable settings and storage, set
`SANCTUM_DESKTOP_DEV_ROOT` to an absolute temporary directory before running
`pnpm devtop`.

## Native packages

Build on the target OS with its native prerequisites. [Tauri prerequisites](https://v2.tauri.app/start/prerequisites/).
Choose the corresponding command from the repository root after installing
PNPM dependencies.

| OS | Command | Output |
| --- | --- | --- |
| macOS | `pnpm --filter @sanctum/desktop tauri build --bundles app` | `apps/desktop/src-tauri/target/release/bundle/macos/Sanctum.app` |
| Debian-based Linux | `pnpm --filter @sanctum/desktop tauri build --bundles deb` | `apps/desktop/src-tauri/target/release/bundle/deb/*.deb` |
| Windows | `pnpm --filter @sanctum/desktop tauri build --bundles nsis` | `apps/desktop/src-tauri/target/release/bundle/nsis/*-setup.exe` |

To cross-build the unsigned Windows x64 NSIS installer from macOS, install
LLVM, LLD, NSIS, and `cargo-xwin`, and add LLVM's tools to `PATH`. The same
runner must build the desktop executable and both Rust sidecars.
[Tauri Windows cross-compilation](https://v2.tauri.app/distribute/windows-installer/#build-windows-apps-on-linux-and-macos).

```sh
rustup target add x86_64-pc-windows-msvc
SANCTUM_DESKTOP_CARGO_XWIN=1 pnpm --filter @sanctum/desktop tauri build \
  --runner cargo-xwin --target x86_64-pc-windows-msvc --bundles nsis --no-sign
```

The cross-build outputs the installer under
`apps/desktop/src-tauri/target/x86_64-pc-windows-msvc/release/bundle/nsis/`.

The package includes `sanctum-server` and `sanctum-cli` next to the desktop
executable. On Debian-based systems install the built package with
`sudo apt install ./path/to/the-built-package.deb` and launch **Sanctum** from a desktop
session with a tray host such as the Xfce Status Tray plugin. The Linux menu
provides **Open Sanctum** and **Quit Sanctum**. [Tauri Debian packaging](https://v2.tauri.app/distribute/debian/), [Tauri tray support](https://v2.tauri.app/learn/system-tray/).

For a package smoke test, launch Sanctum and create a collection. Close the
window, confirm the MCP server still serves the collection, and reopen
**Open Sanctum** from the tray. Launching the app again should focus the same
window. Select **Quit Sanctum**, verify the listener stops, and relaunch to
check the stored collection. Platform results and signing status are in the
[implementation status](IMPLEMENTATION_STATUS.md).
