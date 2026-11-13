import { execFileSync } from "node:child_process"
import { copyFileSync, mkdirSync } from "node:fs"
import { join } from "node:path"
import { fileURLToPath } from "node:url"

const root = fileURLToPath(new URL("../", import.meta.url))
const target = process.env.TAURI_ENV_TARGET_TRIPLE ??
  execFileSync("rustc", ["--print", "host-tuple"], { encoding: "utf8" }).trim()
const release = process.argv.includes("--release") && process.env.TAURI_ENV_DEBUG !== "true"
const targetDir = join(root, "target")
const output = join(root, "apps/desktop/src-tauri/binaries")
const programs = ["sanctum-server", "sanctum-cli"]
const build = ["build", "--locked", "--target", target, "--target-dir", targetDir]
if (release) build.push("--release")
for (const program of programs) build.push("-p", program)
execFileSync("cargo", build, { cwd: root, stdio: "inherit" })
mkdirSync(output, { recursive: true })
const extension = target.includes("windows") ? ".exe" : ""
for (const program of programs) {
  copyFileSync(
    join(targetDir, target, release ? "release" : "debug", program + extension),
    join(output, `${program}-${target}${extension}`),
  )
}
