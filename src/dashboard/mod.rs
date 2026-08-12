mod app;
mod query;
mod ui;

use std::time::Duration;

use futures_util::StreamExt;
use ratatui::crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use serde::Deserialize;

use crate::{
    config::ClickhouseConfig,
    manifest::{Registry, Tenant},
};

use self::{app::App, query::Window};

#[derive(Deserialize, clickhouse::Row)]
struct HealthRow {
    value: u8,
}

pub async fn run(registry: &'static Registry, tenant: &'static Tenant) {
    let config = match ClickhouseConfig::from_env() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("invalid configuration: {message}");
            return;
        }
    };
    let timezone = crate::config::dashboard_timezone();
    // Every calendar-day boundary below resolves in this timezone. An unknown name makes
    // ClickHouse reject the query outright, so the health check doubles as its validation.
    let client = config
        .client()
        .with_option("session_timezone", timezone.clone());
    let health = tokio::time::timeout(
        Duration::from_secs(2),
        client.query("SELECT 1 AS value").fetch_one::<HealthRow>(),
    )
    .await;
    if !matches!(health, Ok(Ok(HealthRow { value: 1 }))) {
        let detail = match health {
            Ok(Ok(_)) => "unexpected response".to_owned(),
            Ok(Err(error)) => error.to_string(),
            Err(_) => "query timed out".to_owned(),
        };
        eprintln!(
            "dashboard could not reach ClickHouse at {} (DASHBOARD_TIMEZONE={timezone}): {detail}",
            config.url
        );
        return;
    }

    let mut terminal = match ratatui::try_init() {
        Ok(terminal) => terminal,
        Err(error) => {
            eprintln!("dashboard could not initialize the terminal: {error}");
            return;
        }
    };
    let result = event_loop(&mut terminal, client, registry, tenant, timezone).await;
    ratatui::restore();
    if let Err(error) = result {
        eprintln!("dashboard stopped: {error}");
    }
}

async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    client: clickhouse::Client,
    registry: &'static Registry,
    tenant: &'static Tenant,
    timezone: String,
) -> Result<(), String> {
    let mut app = App::new(tenant, timezone);
    let mut input = EventStream::new();
    let mut fast = tokio::time::interval(Duration::from_secs(1));
    let mut slow = tokio::time::interval(Duration::from_secs(10));
    let (fast_tx, mut fast_rx) = tokio::sync::mpsc::channel(1);
    let (slow_tx, mut slow_rx) = tokio::sync::mpsc::channel(1);
    let mut fast_in_flight = false;
    let mut slow_in_flight = false;

    loop {
        terminal
            .draw(|frame| ui::draw(frame, &app))
            .map_err(|error| error.to_string())?;
        tokio::select! {
            event = input.next() => {
                match event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        let quit = matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                            || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL));
                        if quit { return Ok(()); }
                        match key.code {
                            KeyCode::Char('1') => { app.window = Window::D1; slow.reset_immediately(); }
                            KeyCode::Char('2') => { app.window = Window::D7; slow.reset_immediately(); }
                            KeyCode::Char('3') => { app.window = Window::D30; slow.reset_immediately(); }
                            KeyCode::Char('p') => app.paused = !app.paused,
                            KeyCode::Char('r') => { fast.reset_immediately(); slow.reset_immediately(); }
                            KeyCode::Char('t') => { switch_tenant(&mut app, registry, true); fast.reset_immediately(); slow.reset_immediately(); }
                            KeyCode::Char('T') => { switch_tenant(&mut app, registry, false); fast.reset_immediately(); slow.reset_immediately(); }
                            _ => {}
                        }
                    }
                    Some(Err(error)) => app.fast_error = Some(format!("terminal input error: {error}")),
                    None => return Ok(()),
                    _ => {}
                }
            }
            _ = fast.tick(), if !app.paused && !fast_in_flight => {
                fast_in_flight = true;
                let client = client.clone();
                let watermark = app.watermark;
                let tenant = app.tenant;
                let sender = fast_tx.clone();
                tokio::spawn(async move {
                    let result = tokio::time::timeout(Duration::from_secs(2), query::fast(&client, tenant, watermark))
                        .await
                        .map_err(|_| "fast query timed out".to_owned())
                        .and_then(|result| result.map_err(|error| error.to_string()));
                    let _ = sender.send((tenant.name.clone(), result)).await;
                });
            }
            _ = slow.tick(), if !app.paused && !slow_in_flight => {
                slow_in_flight = true;
                let client = client.clone();
                let window = app.window;
                let tenant = app.tenant;
                let sender = slow_tx.clone();
                tokio::spawn(async move {
                    let result = tokio::time::timeout(Duration::from_secs(10), query::slow(&client, tenant, window))
                        .await
                        .map_err(|_| "slow query timed out".to_owned())
                        .and_then(|result| result.map_err(|error| error.to_string()));
                    let _ = sender.send((tenant.name.clone(), window, result)).await;
                });
            }
            result = fast_rx.recv(), if fast_in_flight => {
                fast_in_flight = false;
                match result {
                    Some((name, Ok((buckets, tail, connected)))) if name == app.tenant.name => app.apply_fast(buckets, tail, connected),
                    Some((name, Err(error))) if name == app.tenant.name => app.fast_error = Some(error),
                    Some(_) => fast.reset_immediately(),
                    None => app.fast_error = Some("fast query worker stopped".into()),
                }
            }
            result = slow_rx.recv(), if slow_in_flight => {
                slow_in_flight = false;
                match result {
                    Some((name, window, Ok((totals, breakdown, fleet)))) if name == app.tenant.name && window == app.window => app.apply_slow(totals, breakdown, fleet),
                    Some((name, window, Err(error))) if name == app.tenant.name && window == app.window => app.slow_error = Some(error),
                    Some(_) => slow.reset_immediately(),
                    None => app.slow_error = Some("slow query worker stopped".into()),
                }
            }
        }
    }
}

fn switch_tenant(app: &mut App, registry: &'static Registry, forward: bool) {
    let tenants = registry.iter().collect::<Vec<_>>();
    let current = tenants
        .iter()
        .position(|tenant| tenant.name == app.tenant.name)
        .unwrap_or(0);
    let next = if forward {
        (current + 1) % tenants.len()
    } else {
        (current + tenants.len() - 1) % tenants.len()
    };
    app.switch_tenant(tenants[next]);
}
