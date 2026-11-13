import { Channel, invoke } from "@tauri-apps/api/core"
import { Activity, CircleAlert, Copy, Database, FilePlus, FolderOpen, Layers, Pause, Play, Plus, Radio, RefreshCw, Settings2 } from "lucide-react"
import { useCallback, useEffect, useState } from "react"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { bytes, type ImportEvent, type Job, type Overview, type Usage } from "@/contracts"
import { cn } from "@/lib/utils"
import { SettingsPanel } from "@/settings"

const navigation = [
  { id: "activity", label: "Activity", icon: Activity },
  { id: "settings", label: "Settings", icon: Settings2 },
  { id: "connect", label: "Connect", icon: Radio },
] as const

const phaseLabels: Record<Overview["runtime"]["phase"], string> = {
  running: "Server running",
  starting: "Starting server",
  stopping: "Stopping server",
  retrying: "Server unavailable",
}

export function App() {
  const [section, setSection] = useState<(typeof navigation)[number]["id"]>("activity")
  const [overview, setOverview] = useState<Overview | null>(null)
  const [collection, setCollection] = useState("")
  const [newCollection, setNewCollection] = useState("")
  const [usage, setUsage] = useState<Usage | null>(null)
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState<string | null>(null)
  const [operationError, setOperationError] = useState<string | null>(null)

  const run = useCallback(async (action: () => Promise<void>) => {
    setOperationError(null)
    setNotice(null)
    setBusy(true)
    try { await action() }
    catch (error) { setOperationError(String(error)) }
    finally { setBusy(false) }
  }, [])

  useEffect(() => {
    let active = true
    let timer: ReturnType<typeof setTimeout>
    async function poll() {
      try {
        const value = await invoke<Overview>("overview", { collection: collection || null })
        if (active) setOverview(value)
      } catch (error) {
        if (active) setOperationError(String(error))
      } finally {
        if (active) timer = setTimeout(poll, 1500)
      }
    }
    void poll()
    return () => { active = false; clearTimeout(timer) }
  }, [collection])

  const running = overview?.runtime.phase === "running"
  const target = collection || overview?.collections[0] || ""
  const phase = overview?.runtime.phase ?? "starting"
  const warning = operationError || overview?.runtime.error || overview?.api_error

  function importPath(folder: boolean) {
    void run(async () => {
      const path = await invoke<string | null>("pick_path", { folder })
      if (!path) return
      let queued = 0
      let unchanged = 0
      let failures = 0
      let sample = ""
      const events = new Channel<ImportEvent>()
      events.onmessage = event => {
        if (event.state === "queued" || event.state === "pending") queued++
        if (event.state === "unchanged") unchanged++
        if (event.state === "failed") {
          failures++
          sample ||= event.message ? `${event.path}: ${event.message}` : event.path
        }
        setNotice(`Import: ${queued} queued, ${unchanged} unchanged, ${failures} failed`)
      }
      await invoke("import_local", { collection: target, path, events })
      if (sample) setOperationError(sample)
      setCollection(target)
    })
  }

  function actOnJob(job: Job, operation: "retry_job" | "cancel_job") {
    void run(() => invoke(operation, { collection: job.collection, operationId: job.id }))
  }

  async function copy(value: string) {
    await invoke("copy_mcp", { value })
    setNotice("Copied to clipboard")
  }

  return <div className="grid min-h-screen grid-cols-[150px_minmax(0,1fr)] md:grid-cols-[188px_minmax(0,1fr)]">
    <aside className="sticky top-0 flex h-screen flex-col border-r border-border bg-card/45 p-3 md:p-4">
      <div className="flex items-center gap-2.5 px-2 pb-7 pt-2 text-lg font-semibold tracking-tight">
        <span className="grid size-7 place-items-center rounded-lg bg-primary/15 text-primary"><Database size={16} /></span>Sanctum
      </div>
      <nav className="space-y-1" aria-label="Sanctum">
        {navigation.map(({ id, icon: Icon, label }) => <button key={id} type="button" aria-current={section === id ? "page" : undefined}
          onClick={() => setSection(id)} className={cn("flex w-full items-center gap-2.5 rounded-lg px-3 py-2.5 text-left text-sm font-medium text-muted-foreground hover:bg-muted/80 hover:text-foreground", section === id && "bg-muted text-foreground")}>
          <Icon size={16} />{label}
        </button>)}
      </nav>
      <div className="mt-auto flex items-center gap-2 px-2 py-3 text-xs text-muted-foreground" role="status">
        <span className={cn("size-2 shrink-0 rounded-full", running ? "bg-primary" : "bg-amber-400")} />
        {phaseLabels[phase]}
      </div>
    </aside>
    <main className="mx-auto w-full max-w-[1080px] px-5 py-7 lg:px-8">
      <header className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">{navigation.find(item => item.id === section)?.label}</h1>
        {section === "activity" && <Button variant="outline" disabled={!running || busy}
          onClick={() => void run(() => invoke("pause_queue", { paused: !overview?.paused }))}>
          {overview?.paused ? <Play size={15} /> : <Pause size={15} />}
          {overview?.paused ? "Resume queue" : "Pause queue"}
        </Button>}
      </header>
      {warning && <div className="mb-4 flex gap-2 rounded-lg border border-amber-500/35 bg-amber-500/10 p-3 text-sm text-amber-200" role="alert">
        <CircleAlert size={17} className="mt-0.5 shrink-0" /><span className="break-all">{warning}</span>
      </div>}
      {notice && <p className="mb-4 text-sm text-muted-foreground" role="status">{notice}</p>}

      {section === "activity" && <div className="space-y-4">
        <Card className="space-y-3 p-4">
          <div className="flex items-center gap-2"><Layers size={17} className="text-primary" /><h2 className="font-semibold">Collections</h2></div>
          <div className="flex flex-wrap items-center gap-2">
            <select aria-label="Filter by collection" className="max-w-52 rounded-md border border-border bg-background px-3 py-2 text-sm"
              value={collection} onChange={event => { setCollection(event.currentTarget.value); setUsage(null) }}>
              <option value="">All activity</option>
              {overview?.collections.map(id => <option key={id} value={id}>{id}</option>)}
            </select>
            <Button variant="outline" disabled={!running || !target || busy} onClick={() => importPath(false)}><FilePlus size={15} /> Import file</Button>
            <Button variant="outline" disabled={!running || !target || busy} onClick={() => importPath(true)}><FolderOpen size={15} /> Import folder</Button>
          </div>
          {!collection && target && <p className="text-xs text-muted-foreground">Imports go to {target}. Select a collection to change the destination.</p>}
          <form className="flex max-w-md gap-2" onSubmit={event => {
            event.preventDefault()
            const id = newCollection.trim()
            if (!id) return
            void run(async () => {
              await invoke("create_collection", { collection: id })
              setNewCollection("")
              setCollection(id)
              setUsage(null)
            })
          }}>
            <Input aria-label="Collection ID" placeholder="New collection ID" value={newCollection}
              onChange={event => setNewCollection(event.currentTarget.value)} />
            <Button type="submit" disabled={!running || busy || !newCollection.trim()}><Plus size={15} /> Create</Button>
          </form>
        </Card>
        <div className="grid gap-4 sm:grid-cols-2">
          <Card className="p-4">
            <div className="flex items-center justify-between gap-2">
              <h2 className="font-semibold">Storage</h2>
              <Button variant="ghost" size="small" disabled={!running || busy}
                onClick={() => void run(async () => setUsage(await invoke<Usage>("storage_usage", { collection: collection || null })))}>
                <RefreshCw size={13} /> Refresh
              </Button>
            </div>
            <p className="mt-2 text-sm text-muted-foreground">
              {usage ? `${bytes(usage.bytes)} across ${usage.files.toLocaleString()} files` : `Measure ${collection ? collection : "all local storage"}`}
            </p>
          </Card>
          <Card className="p-4">
            <h2 className="font-semibold">Embeddings{target && <span className="ml-2 text-xs font-normal text-muted-foreground">{target}</span>}</h2>
            <p className="mt-2 text-sm text-muted-foreground">
              {overview?.embedding?.enabled ? `${overview.embedding.embedded.toLocaleString()} of ${overview.embedding.total.toLocaleString()} chunks ready` :
                overview?.embedding ? "Disabled" : "Choose a collection"}
            </p>
            {!!overview?.embedding?.failed && <p className="mt-1 break-words text-xs text-amber-200">
              {overview.embedding.failed} failed, {overview.embedding.retrying} retrying
              {overview.embedding.error && `. ${overview.embedding.error}`}
            </p>}
          </Card>
        </div>
        <Card className="p-4">
          <h2 className="mb-3 font-semibold">Recent ingestion</h2>
          {!overview?.jobs.length && <p className="text-sm text-muted-foreground">No ingestion activity yet.</p>}
          <ul className="divide-y divide-border">
            {overview?.jobs.map(job => <li key={job.id} className="flex items-start justify-between gap-3 py-3">
              <div className="min-w-0">
                <p className="break-all text-sm font-medium">{job.name}</p>
                <p className="mt-1 text-xs text-muted-foreground">{job.collection} · {job.state} · {bytes(job.processed_bytes)} / {bytes(job.total_bytes)}</p>
                {job.cancelable && job.total_bytes > 0 && <progress aria-label={`${job.name} ingestion`} className="mt-1 max-w-full" max={job.total_bytes} value={job.processed_bytes} />}
                {job.error && <p className="mt-1 break-all text-xs text-amber-200">{job.error}</p>}
              </div>
              {job.retryable && <Button variant="outline" size="small" disabled={busy} onClick={() => actOnJob(job, "retry_job")}>Retry</Button>}
              {job.cancelable && <Button variant="ghost" size="small" disabled={busy} onClick={() => actOnJob(job, "cancel_job")}>Cancel</Button>}
            </li>)}
          </ul>
        </Card>
      </div>}

      {section === "settings" && <SettingsPanel run={run} busy={busy} pending={overview?.runtime.restart_pending ?? false} />}

      {section === "connect" && <div className="space-y-4">
        <Card className="space-y-3 p-5">
          <h2 className="font-semibold">Streamable HTTP</h2>
          {overview?.runtime.mcp_url ? <div className="flex flex-wrap items-center gap-2">
            <code className="break-all rounded bg-background px-3 py-2 text-sm">{overview.runtime.mcp_url}</code>
            <Button variant="outline" onClick={() => void run(() => copy(overview.runtime.mcp_url!))}><Copy size={14} /> Copy URL</Button>
          </div> : <p className="text-sm text-muted-foreground">The server is starting or unavailable.</p>}
        </Card>
        {overview?.runtime.stdio && <Card className="space-y-3 p-5">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h2 className="font-semibold">Stdio configuration</h2>
            <Button variant="outline" onClick={() => void run(() => copy(JSON.stringify(overview.runtime.stdio, null, 2)))}><Copy size={14} /> Copy configuration</Button>
          </div>
          <pre className="whitespace-pre-wrap break-all rounded bg-background p-3 text-xs text-muted-foreground">{JSON.stringify(overview.runtime.stdio, null, 2)}</pre>
        </Card>}
      </div>}
    </main>
  </div>
}
