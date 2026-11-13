import { execFileSync } from "node:child_process"
import { copyFileSync, mkdirSync, renameSync, rmSync } from "node:fs"
import { join } from "node:path"
import { fileURLToPath } from "node:url"

const root = fileURLToPath(new URL("../", import.meta.url))
const target = process.env.TAURI_ENV_TARGET_TRIPLE ??
  execFileSync("rustc", ["--print", "host-tuple"], { encoding: "utf8" }).trim()
const release = process.argv.includes("--release") && process.env.TAURI_ENV_DEBUG !== "true"
const targetDir = join(root, "target")
const output = join(root, "apps/desktop/src-tauri/binaries")
const programs = ["sanctum-server", "sanctum-cli"]
const devBuild = process.argv.includes("--dev-build")
const build = ["build", "--locked", "--target", target, "--target-dir", targetDir]
if (process.env.SANCTUM_DESKTOP_CARGO_XWIN === "1") build.unshift("xwin")
if (release) build.push("--release")
for (const program of programs) build.push("-p", program)
// The Tauri build script is itself running under Cargo. The sidecars use the
// repository target directory and their own jobserver to avoid nested locks.
const env = { ...process.env }
if (devBuild) {
  delete env.CARGO_MAKEFLAGS
  delete env.MAKEFLAGS
  delete env.MFLAGS
  delete env.RUSTC
  delete env.RUSTC_WRAPPER
  delete env.RUSTC_WORKSPACE_WRAPPER
  delete env.CARGO_ENCODED_RUSTFLAGS
  build.push("--jobs", "2")
}
execFileSync("cargo", build, { cwd: root, stdio: "inherit", env })
mkdirSync(output, { recursive: true })
const extension = target.includes("windows") ? ".exe" : ""
for (const program of programs) {
  const destination = join(output, `${program}-${target}${extension}`)
  const staged = `${destination}.${process.pid}.tmp`
  try {
    copyFileSync(join(targetDir, target, release ? "release" : "debug", program + extension), staged)
    renameSync(staged, destination)
  } finally {
    rmSync(staged, { force: true })
  }
}
