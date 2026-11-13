use std::{env, path::PathBuf, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=SANCTUM_DEVTOP");
    if env::var_os("SANCTUM_DEVTOP").is_some() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
        for path in [
            "crates",
            "proto",
            "Cargo.toml",
            "Cargo.lock",
            "scripts/desktop-sidecars.mjs",
        ] {
            println!("cargo:rerun-if-changed={}", root.join(path).display());
        }
        let target = env::var_os("TARGET").expect("Cargo sets TARGET");
        let status = Command::new("node")
            .arg(root.join("scripts/desktop-sidecars.mjs"))
            .arg("--dev-build")
            .env("TAURI_ENV_TARGET_TRIPLE", target)
            .current_dir(&root)
            .status()
            .expect("could not start the Sanctum sidecar build");
        assert!(status.success(), "could not build the Sanctum sidecars");
    }
    tauri_build::build();
}
