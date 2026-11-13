import { invoke } from "@tauri-apps/api/core"
import { FolderOpen, Save } from "lucide-react"
import { useEffect, useState, type ReactNode } from "react"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import type { EmbeddingSettings, Settings } from "@/contracts"

const defaultEmbedding: EmbeddingSettings = {
  endpoint: "http://127.0.0.1:11434",
  model: "",
  revision: "",
  dimensions: 768,
  timeout_seconds: 30,
  document_prefix: "",
  query_prefix: "",
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
  function updateEmbedding(change: Partial<EmbeddingSettings>) {
    if (embedding) update({ embedding: { ...embedding, ...change } })
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
          <select className="rounded-md border border-border bg-background px-3 py-2 text-sm" value={embedding ? "ollama" : "disabled"}
            onChange={event => update({ embedding: event.currentTarget.value === "ollama" ? { ...defaultEmbedding } : null })}>
            <option value="disabled">Disabled</option><option value="ollama">Ollama</option>
          </select>
        </Field>
      </div>
      {embedding && <>
        <Field label="Ollama URL"><Input type="url" required value={embedding.endpoint}
          onChange={event => updateEmbedding({ endpoint: event.currentTarget.value })} /></Field>
        <div className="grid grid-cols-2 gap-3">
          <Field label="Model"><Input required value={embedding.model} placeholder="embeddinggemma-2:740m"
            onChange={event => updateEmbedding({ model: event.currentTarget.value })} /></Field>
          <Field label="Model revision"><Input required value={embedding.revision} placeholder="Digest from ollama list"
            onChange={event => updateEmbedding({ revision: event.currentTarget.value })} /></Field>
          <Field label="Dimensions"><Input type="number" min={1} max={8192} required value={embedding.dimensions}
            onChange={event => updateEmbedding({ dimensions: event.currentTarget.valueAsNumber })} /></Field>
          <Field label="Request timeout (seconds)"><Input type="number" min={1} max={300} required value={embedding.timeout_seconds}
            onChange={event => updateEmbedding({ timeout_seconds: event.currentTarget.valueAsNumber })} /></Field>
          <Field label="Document prefix"><Input value={embedding.document_prefix}
            onChange={event => updateEmbedding({ document_prefix: event.currentTarget.value })} /></Field>
          <Field label="Query prefix"><Input value={embedding.query_prefix}
            onChange={event => updateEmbedding({ query_prefix: event.currentTarget.value })} /></Field>
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
