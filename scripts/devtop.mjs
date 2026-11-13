import { spawn } from "node:child_process"

// PNPM supplies its own entry point, including on Windows where its shim
// is a batch file. Tauri owns the frontend and desktop process lifecycle.
const pnpm = process.env.npm_execpath
if (!pnpm) throw new Error("Run this command with pnpm devtop")
const tauri = spawn(process.execPath, [pnpm, "--filter", "@sanctum/desktop", "tauri", "dev", ...process.argv.slice(2)], {
  env: { ...process.env, SANCTUM_DEVTOP: "1" },
  stdio: "inherit",
})
tauri.on("exit", (code, signal) => {
  process.exitCode = code ?? (signal === "SIGINT" ? 130 : 1)
})
