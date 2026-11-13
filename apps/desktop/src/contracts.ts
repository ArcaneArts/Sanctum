export type EmbeddingSettings = {
  endpoint: string
  model: string
  revision: string
  dimensions: number
  timeout_seconds: number
  document_prefix: string
  query_prefix: string
}

export type Settings = {
  storage_root: string
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
}

export type Overview = {
  runtime: {
    phase: "starting" | "running" | "stopping" | "retrying"
    error: string | null
    mcp_url: string | null
    stdio: { command: string, args: string[] } | null
    restart_pending: boolean
  }
  collections: string[]
  paused: boolean
  jobs: Job[]
  embedding: { enabled: boolean, total: number, embedded: number, failed: number, retrying: number, error: string | null } | null
  api_error: string | null
}

export type Usage = { bytes: number, files: number }
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
