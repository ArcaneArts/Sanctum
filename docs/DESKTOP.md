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

1. In **Settings**, choose a local storage folder and save the changes. Select
   **Restart server to apply**. Sanctum defaults to a folder inside your OS
   application data directory.
2. To enable embeddings, select **Ollama** and enter the endpoint, model tag,
   model revision, and dimensions. Use the digest shown by `ollama list` as
   the revision for a local model. Save and restart the server.
3. In **Activity**, create and select a collection, then import a file or
   folder. Track accepted jobs and current embedding coverage. Select
   **Refresh** under Storage to measure the selected collection or the
   entire local store.
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

## Development validation

```sh
pnpm check
pnpm desktop:check
```

`desktop:check` builds the sidecars and frontend and checks the Tauri Rust
package. To exercise the UI with disposable settings and storage, set
`SANCTUM_DESKTOP_DEV_ROOT` to an absolute temporary directory before running
`pnpm devtop`.

For a native package on the current host, run the Tauri build command.

```sh
pnpm --filter @sanctum/desktop tauri build
```

The build embeds the server and CLI for the target. Use native builds on each
platform to validate its installer, tray, and native dialogs. Release
signing and platform acceptance are tracked in the
[implementation status](IMPLEMENTATION_STATUS.md).
