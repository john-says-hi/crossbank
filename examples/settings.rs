//! An eager locker, shaped exactly like a Hive `Box` of application settings.
//!
//! Run with `cargo run --example settings`.
//!
//! This is the shape crossbank's [`Locker`] exists for: a handful of small,
//! hot values that the UI reads from paths that cannot await. The values are
//! resident in RAM, so [`Locker::get`] is synchronous and infallible — the
//! same bargain Hive's `Box` makes, for the same reason.
//!
//! [`Locker`]: crossbank::Locker
//! [`Locker::get`]: crossbank::Locker::get

use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crossbank::{Bank, BankConfig, Event, Locker};

/// Settings are a struct per key here rather than one blob, so a change to one
/// setting is one small write and one `watch_key` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Theme {
    dark: bool,
    accent: String,
}

fn main() -> crossbank::Result<()> {
    // `block_on` only because an example needs *some* executor. crossbank
    // itself never depends on one.
    futures::executor::block_on(run())
}

async fn run() -> crossbank::Result<()> {
    // `BankConfig::memory()` for a runnable example. A real desktop app uses
    // `BankConfig::at("…/app.crossbank")`; a web app uses
    // `BankConfig::web("app")`.
    let bank = Bank::open(BankConfig::memory()).await?;
    let settings: Locker<Theme> = bank.locker("settings").await?;

    // Hive: `box.get('theme', defaultValue: …)`. The default is not written —
    // it is what you get back when nothing is stored.
    let fallback = Theme {
        dark: false,
        accent: "blue".into(),
    };
    println!(
        "before any write: {:?}",
        settings.get_or("theme", fallback.clone())
    );

    // Watch before writing, so the write below is observed. `watch_keys` is
    // the equivalent of Hive's `listenable(keys: […])`; the stream is bounded,
    // so a consumer that stops reading drops events rather than growing
    // without limit.
    let mut watched = settings.watch_keys(&["theme"]);

    // Hive: `box.put(…)`. The write is a durable commit by default — when this
    // resolves, the data is on disk.
    settings
        .put(
            "theme",
            Theme {
                dark: true,
                accent: "amber".into(),
            },
        )
        .await?;

    // And now the resident copy answers synchronously, with no await in sight.
    // This is the call the UI makes.
    let theme = settings.get("theme").expect("just written");
    println!("after the write: {theme:?}");
    println!(
        "get_or now returns the stored value: {:?}",
        settings.get_or("theme", fallback.clone())
    );

    match watched.next().await {
        Some(Event::Put { key }) => {
            println!("watch saw a write to {:?}", String::from_utf8_lossy(&key))
        }
        other => println!("watch saw {other:?}"),
    }

    // Bulk work is one atomic commit, not one per pair — Hive's `putAll`.
    settings
        .put_all(vec![
            (
                "theme_light".to_string(),
                Theme {
                    dark: false,
                    accent: "amber".into(),
                },
            ),
            (
                "theme_high_contrast".to_string(),
                Theme {
                    dark: true,
                    accent: "white".into(),
                },
            ),
        ])
        .await?;

    println!("keys: {:?}", settings.keys());
    println!("length: {}", settings.len());
    println!(
        "contains theme_light: {}",
        settings.contains_key("theme_light")
    );

    // Hive's `deleteAll`, also one commit.
    settings
        .delete_all(vec![
            "theme_light".to_string(),
            "theme_high_contrast".to_string(),
        ])
        .await?;
    println!("after delete_all: {:?}", settings.keys());

    // `close` is async: it flushes anything staged first, and a bare
    // `settings.close();` would be a dropped future that does nothing.
    settings.close().await?;
    bank.close().await?;
    Ok(())
}
