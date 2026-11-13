import { invoke } from "@tauri-apps/api/core"
import { FolderOpen, Save } from "lucide-react"
import { useEffect, useState, type ReactNode } from "react"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import type { EmbeddingProfile, EmbeddingSettings, Settings } from "@/contracts"

function defaultEmbedding(provider: string): EmbeddingSettings | null {
  const profile = { model: "", revision: "", dimensions: 768, document_prefix: "", query_prefix: "" }
  const common = { profile, timeout_seconds: 30 }
  switch (provider) {
    case "ollama":
      return { ...common, provider, endpoint: "http://127.0.0.1:11434" }
    case "openai":
      return {
        ...common, provider, base_url: "https://api.openai.com/v1", api_key_env: "OPENAI_API_KEY", send_dimensions: true,
        profile: { ...profile, model: "text-embedding-3-small", dimensions: 1536 },
      }
    case "google":
      return {
        ...common, provider, project: "", location: "us-central1",
        profile: { ...profile, model: "gemini-embedding-001" },
      }
    default:
      return null
  }
}

function Field({ label, children }: { label: string, children: ReactNode }) {
  return <label className="flex min-w-0 flex-col gap-1.5 text-sm"><span>{label}</span>{children}</label>
}

export function SettingsPanel({ run, pending, busy }: {
  run: (action: () => Promise<void>) => Promise<void>
  pending: boolean
  busy: boolean
}) {
  const [settings, setSettings] = useState<Settings | null>(null)
  const [dirty, setDirty] = useState(false)
  useEffect(() => {
    let active = true
    void run(async () => {
      const saved = await invoke<Settings>("settings")
      if (active) setSettings(saved)
    })
    return () => { active = false }
  }, [run])

  if (!settings) return <p className="text-sm text-muted-foreground">Loading settings…</p>
  const embedding = settings.embedding
  function update(change: Partial<Settings>) {
    setSettings(current => current && { ...current, ...change })
    setDirty(true)
  }
  function updateProfile(change: Partial<EmbeddingProfile>) {
    if (embedding) update({ embedding: { ...embedding, profile: { ...embedding.profile, ...change } } })
  }

  return <form className="space-y-4" onSubmit={event => {
    event.preventDefault()
    void run(async () => {
      await invoke("save_settings", { settings })
      setDirty(false)
    })
  }}>
    <Card className="space-y-4 p-5">
      <h2 className="font-semibold">Storage and server</h2>
      <div className="flex items-end gap-2">
        <div className="min-w-0 flex-1">
          <Field label="Local storage folder">
            <Input value={settings.storage_root} required onChange={event => update({ storage_root: event.currentTarget.value })} />
          </Field>
        </div>
        <Button variant="outline" disabled={busy} onClick={() => void run(async () => {
          const path = await invoke<string | null>("pick_path", { folder: true })
          if (path) update({ storage_root: path })
        })}><FolderOpen size={15} /> Browse</Button>
      </div>
      <div className="grid grid-cols-3 gap-3">
        <Field label="gRPC port"><Input type="number" min={0} max={65535} required value={settings.grpc_port}
          onChange={event => update({ grpc_port: event.currentTarget.valueAsNumber })} /></Field>
        <Field label="MCP port"><Input type="number" min={0} max={65535} required value={settings.mcp_port}
          onChange={event => update({ mcp_port: event.currentTarget.valueAsNumber })} /></Field>
        <Field label="Indexing workers"><Input type="number" min={1} max={32} required value={settings.local_worker_concurrency}
          onChange={event => update({ local_worker_concurrency: event.currentTarget.valueAsNumber })} /></Field>
      </div>
    </Card>
    <Card className="space-y-4 p-5">
      <div className="flex items-center justify-between gap-4">
        <h2 className="font-semibold">Embeddings</h2>
        <Field label="Provider">
          <select className="rounded-md border border-border bg-background px-3 py-2 text-sm" value={embedding?.provider ?? "disabled"}
            onChange={event => update({ embedding: defaultEmbedding(event.currentTarget.value) })}>
            <option value="disabled">Disabled</option><option value="ollama">Ollama</option>
            <option value="openai">OpenAI-compatible</option><option value="google">Google Cloud</option>
          </select>
        </Field>
      </div>
      {embedding && <>
        {embedding.provider === "ollama" && <Field label="Ollama URL"><Input type="url" required value={embedding.endpoint}
          onChange={event => update({ embedding: { ...embedding, endpoint: event.currentTarget.value } })} /></Field>}
        {embedding.provider === "openai" && <>
          <Field label="API base URL"><Input type="url" required value={embedding.base_url}
            onChange={event => update({ embedding: { ...embedding, base_url: event.currentTarget.value } })} /></Field>
          <Field label="API key environment variable"><Input value={embedding.api_key_env ?? ""} placeholder="OPENAI_API_KEY"
            onChange={event => update({ embedding: { ...embedding, api_key_env: event.currentTarget.value || null } })} /></Field>
          <label className="flex items-center gap-2 text-sm">
            <input type="checkbox" checked={embedding.send_dimensions}
              onChange={event => update({ embedding: { ...embedding, send_dimensions: event.currentTarget.checked } })} />
            Send dimensions in the request
          </label>
        </>}
        {embedding.provider === "google" && <>
          <div className="grid grid-cols-2 gap-3">
            <Field label="Google Cloud project"><Input required value={embedding.project}
              onChange={event => update({ embedding: { ...embedding, project: event.currentTarget.value } })} /></Field>
            <Field label="Vertex AI location"><Input required value={embedding.location}
              onChange={event => update({ embedding: { ...embedding, location: event.currentTarget.value } })} /></Field>
          </div>
          <p className="text-xs text-muted-foreground">Uses Google Application Default Credentials.</p>
        </>}
        <div className="grid grid-cols-2 gap-3">
          <Field label="Model"><Input required value={embedding.profile.model}
            onChange={event => updateProfile({ model: event.currentTarget.value })} /></Field>
          <Field label="Model revision"><Input required value={embedding.profile.revision}
            placeholder={embedding.provider === "ollama" ? "Digest from ollama list" : "Model version"}
            onChange={event => updateProfile({ revision: event.currentTarget.value })} /></Field>
          <Field label="Dimensions"><Input type="number" min={1} max={8192} required value={embedding.profile.dimensions}
            onChange={event => updateProfile({ dimensions: event.currentTarget.valueAsNumber })} /></Field>
          <Field label="Request timeout (seconds)"><Input type="number" min={1} max={300} required value={embedding.timeout_seconds}
            onChange={event => update({ embedding: { ...embedding, timeout_seconds: event.currentTarget.valueAsNumber } })} /></Field>
          <Field label="Document prefix"><Input value={embedding.profile.document_prefix}
            onChange={event => updateProfile({ document_prefix: event.currentTarget.value })} /></Field>
          <Field label="Query prefix"><Input value={embedding.profile.query_prefix}
            onChange={event => updateProfile({ query_prefix: event.currentTarget.value })} /></Field>
        </div>
      </>}
    </Card>
    <div className="flex flex-wrap items-center gap-2">
      <Button type="submit" disabled={!dirty || busy}><Save size={15} /> Save settings</Button>
      {pending && <Button variant="outline" disabled={dirty || busy} onClick={() => void run(() => invoke("apply_settings"))}>Restart server to apply</Button>}
      {pending && <span className="text-sm text-muted-foreground">Saved settings are waiting for a restart.</span>}
    </div>
  </form>
}
