export type EmbeddingProfile = {
  model: string
  revision: string
  dimensions: number
  document_prefix: string
  query_prefix: string
}

export type EmbeddingSettings = {
  profile: EmbeddingProfile
  timeout_seconds: number
} & (
  | { provider: "ollama", endpoint: string }
  | { provider: "openai", base_url: string, api_key_env: string | null, send_dimensions: boolean }
  | { provider: "google", project: string, location: string }
)

export type ObjectStorage = {
  bucket: string
  prefix: string | null
} & (
  | { backend: "s3", region: string, endpoint: string | null, allow_http: boolean, virtual_hosted_style: boolean }
  | { backend: "gcs" }
)

export type Settings = {
  storage_root: string
  object_storage: ObjectStorage | null
  grpc_port: number
  mcp_port: number
  local_worker_concurrency: number
  embedding: EmbeddingSettings | null
}

export type Job = {
  collection: string
  id: string
  name: string
  state: string
  total_bytes: number
  processed_bytes: number
  attempts: number
  error: string | null
  retryable: boolean
  cancelable: boolean
  embedding_batch: number | null
  embedding_retry_at: number | null
}

export type ActivityPage = { jobs: Job[], next: string | null }

export type Overview = {
  runtime: {
    phase: "starting" | "running" | "stopping" | "retrying"
    error: string | null
    mcp_url: string | null
    storage_backend: "filesystem" | ObjectStorage["backend"] | null
    generation: number
    stdio: { command: string, args: string[] } | null
    restart_pending: boolean
  }
  collections: string[]
  paused: boolean | null
  embedding: { enabled: boolean, total: number, embedded: number, failed: number, retrying: number, error: string | null } | null
  api_error: string | null
}

export type UsageProgress = { bytes: number, files: number }
export type Usage = UsageProgress & { finished_at_unix_ms: number }
export type ImportEvent = { path: string, state: string, message: string | null }

export function bytes(value: number) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"]
  let index = 0
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024
    index += 1
  }
  return `${value.toLocaleString(undefined, { maximumFractionDigits: index ? 1 : 0 })} ${units[index]}`
}
