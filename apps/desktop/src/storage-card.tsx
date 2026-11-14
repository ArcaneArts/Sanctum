import { Channel, invoke } from "@tauri-apps/api/core"
import { RefreshCw } from "lucide-react"
import { useState } from "react"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { bytes, type Overview, type Usage, type UsageProgress } from "@/contracts"

const storageNames = { filesystem: "Local folder", s3: "S3", gcs: "Google Cloud" }

export function StorageCard({ collection, backend, running }: {
  collection: string
  backend: Overview["runtime"]["storage_backend"] | undefined
  running: boolean
}) {
  const [usage, setUsage] = useState<Usage | null>(null)
  const [progress, setProgress] = useState<UsageProgress | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const units = backend === "filesystem" ? "files" : "objects"
  const scope = collection || "all storage"

  async function refresh() {
    setRefreshing(true)
    setError(null)
    setProgress(null)
    const channel = new Channel<UsageProgress>()
    channel.onmessage = setProgress
    try {
      setUsage(await invoke<Usage>("storage_usage", { collection: collection || null, progress: channel }))
    } catch (failure) {
      setError(String(failure))
    } finally {
      setRefreshing(false)
    }
  }

  return <Card className="p-4">
    <div className="flex items-center justify-between gap-2">
      <h2 className="font-semibold">Storage
        {backend && <span className="ml-2 text-xs font-normal text-muted-foreground">{storageNames[backend]}</span>}
      </h2>
      <Button variant="ghost" size="small" disabled={!running || refreshing} onClick={() => void refresh()}>
        <RefreshCw size={13} /> Refresh
      </Button>
    </div>
    {usage && <div className="mt-2 text-sm text-muted-foreground">
      <p>{bytes(usage.bytes)} across {usage.files.toLocaleString()} {units}</p>
      <p className="text-xs">Measured {new Date(usage.finished_at_unix_ms).toLocaleString()}</p>
    </div>}
    {refreshing ? <p className="mt-2 text-sm text-muted-foreground" role="status">
      {progress ? `Scanned ${progress.files.toLocaleString()} ${units} · ${bytes(progress.bytes)}` : `Measuring ${scope}…`}
    </p> : !usage && !error && <p className="mt-2 text-sm text-muted-foreground">Measure {scope}</p>}
    {error && <p className="mt-2 break-words text-sm text-amber-200" role="alert">{error}</p>}
  </Card>
}
