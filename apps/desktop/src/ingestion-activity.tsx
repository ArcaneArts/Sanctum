import { invoke } from "@tauri-apps/api/core"
import { useEffect, useState } from "react"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { bytes, type ActivityPage, type Job } from "@/contracts"

type Props = {
  collection: string
  running: boolean
  busy: boolean
  onJobAction: (job: Job, action: "retry_job" | "cancel_job") => void
}

export function IngestionActivity({ collection, running, busy, onJobAction }: Props) {
  const [cursors, setCursors] = useState<(string | null)[]>([null])
  const [page, setPage] = useState<ActivityPage | null>(null)
  const [error, setError] = useState<string | null>(null)
  const cursor = cursors[cursors.length - 1]

  useEffect(() => {
    if (!running) return
    let active = true
    let timer: ReturnType<typeof setTimeout>

    async function poll() {
      try {
        const response = await invoke<ActivityPage>("ingestion_activity", {
          collection: collection || null,
          cursor,
        })
        if (active) {
          setPage(response)
          setError(null)
        }
      } catch (error) {
        if (active) setError(String(error))
      } finally {
        if (active) timer = setTimeout(poll, 1500)
      }
    }

    void poll()
    return () => { active = false; clearTimeout(timer) }
  }, [collection, cursor, running])

  function showPage(nextCursors: (string | null)[]) {
    setError(null)
    setPage(null)
    setCursors(nextCursors)
  }

  const next = page?.next
  return <Card className="p-4">
    <h2 className="mb-3 font-semibold">Ingestion activity</h2>
    {error && <p className="break-words text-sm text-amber-200" role="alert">{error}</p>}
    {running && !page && !error && <p className="text-sm text-muted-foreground" role="status">Loading activity…</p>}
    {page?.jobs.length === 0 && <p className="text-sm text-muted-foreground">
      {cursor || next ? "No ingestion jobs on this page." : "No ingestion activity yet."}
    </p>}
    <ul className="divide-y divide-border">
      {page?.jobs.map(job => <li key={JSON.stringify([job.collection, job.id])} className="flex items-start justify-between gap-3 py-3">
        <div className="min-w-0">
          <p className="break-all text-sm font-medium">{job.name}</p>
          <p className="mt-1 text-xs text-muted-foreground">{job.collection} · {job.state} · {bytes(job.processed_bytes)} / {bytes(job.total_bytes)}</p>
          {job.cancelable && job.total_bytes > 0 && <progress aria-label={`${job.name} ingestion`} className="mt-1 max-w-full" max={job.total_bytes} value={job.processed_bytes} />}
          {job.embedding_batch != null && <p className="mt-1 text-xs text-muted-foreground">
            Embedding batch {job.embedding_batch + 1}{job.embedding_retry_at != null && ` · Retry ${new Date(job.embedding_retry_at * 1000).toLocaleString()}`}
          </p>}
          {job.error && <p className="mt-1 break-all text-xs text-amber-200">{job.error}</p>}
        </div>
        {job.retryable && <Button variant="outline" size="small" disabled={!running || busy} onClick={() => onJobAction(job, "retry_job")}>Retry</Button>}
        {job.cancelable && <Button variant="ghost" size="small" disabled={!running || busy} onClick={() => onJobAction(job, "cancel_job")}>Cancel</Button>}
      </li>)}
    </ul>
    {(cursor || next) && <nav aria-label="Ingestion activity pages" className="mt-3 flex gap-2">
      <Button variant="outline" size="small" disabled={!running || !cursor} onClick={() => showPage([null])}>First page</Button>
      <Button variant="outline" size="small" disabled={!running || !cursor} onClick={() => showPage(cursors.slice(0, -1))}>Previous page</Button>
      <Button variant="outline" size="small" disabled={!running || !next} onClick={() => next && showPage([...cursors, next])}>Next page</Button>
    </nav>}
  </Card>
}
