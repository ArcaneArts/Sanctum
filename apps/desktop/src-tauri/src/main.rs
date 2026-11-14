#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;
mod host;

use tauri::{
    AppHandle, Manager, RunEvent, WindowEvent,
    image::Image,
    menu::{Menu, MenuItem},
    tray::TrayIconBuilder,
};

fn show_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
    }
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            show_window(app)
        }))
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .setup(|app| {
            let host = host::Host::open(app.handle())?;
            app.manage(host.clone());
            let open = MenuItem::with_id(app, "open", "Open Sanctum", true, None::<&str>)?;
            let status = MenuItem::with_id(app, "status", "Starting…", false, None::<&str>)?;
            let quit_item = MenuItem::with_id(app, "quit", "Quit Sanctum", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&open, &status, &quit_item])?;
            TrayIconBuilder::with_id("sanctum")
                .icon(Image::from_bytes(include_bytes!("../icons/tray.png"))?)
                .icon_as_template(cfg!(target_os = "macos"))
                .tooltip("Sanctum")
                .menu(&menu)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "open" => show_window(app),
                    "quit" => quit(app),
                    _ => {}
                })
                .build(app)?;
            tauri::async_runtime::spawn(host.run(status));
            let app = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    quit(&app);
                }
            });
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .invoke_handler(tauri::generate_handler![
            commands::settings,
            commands::save_settings,
            commands::apply_settings,
            commands::overview,
            commands::ingestion_activity,
            commands::create_collection,
            commands::pick_path,
            commands::import_local,
            commands::upload_file,
            commands::pause_queue,
            commands::retry_job,
            commands::cancel_job,
            commands::storage_usage,
            commands::copy_mcp,
        ])
        .build(tauri::generate_context!())
        .expect("could not build Sanctum desktop")
        .run(|app, event| {
            if let RunEvent::ExitRequested {
                api, code: None, ..
            } = event
            {
                api.prevent_exit();
                quit(app);
            }
        });
}

fn quit(app: &AppHandle) {
    let host = app.state::<host::Host>().inner().clone();
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        if host.quit().await {
            app.exit(0);
        }
    });
}
