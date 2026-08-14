mod app;
mod query;
mod ui;

use std::{
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    time::Duration,
};

use futures_util::StreamExt;
use ratatui::crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use serde::Deserialize;

use crate::{
    config::ClickhouseConfig,
    manifest::{Registry, Tenant},
};

use self::{
    app::{App, IngestHealth},
    query::Window,
};

#[derive(Deserialize, clickhouse::Row)]
struct HealthRow {
    value: u8,
}

#[derive(Debug, Deserialize)]
struct IngestHealthResponse {
    ok: bool,
    pending_events: usize,
    #[serde(default = "default_batch_capacity")]
    batch_capacity: usize,
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
    let ingest_health_url = crate::config::ingest_health_url();
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
    let result = event_loop(
        &mut terminal,
        client,
        registry,
        tenant,
        timezone,
        ingest_health_url,
    )
    .await;
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
    ingest_health_url: String,
) -> Result<(), String> {
    let mut app = App::new(tenant, timezone);
    let mut input = EventStream::new();
    let mut fast = tokio::time::interval(Duration::from_secs(1));
    let mut slow = tokio::time::interval(Duration::from_secs(10));
    let (fast_tx, mut fast_rx) = tokio::sync::mpsc::channel(1);
    let (slow_tx, mut slow_rx) = tokio::sync::mpsc::channel(1);
    let mut ingest_health = tokio::time::interval(Duration::from_secs(1));
    let (ingest_health_tx, mut ingest_health_rx) = tokio::sync::mpsc::channel(1);
    let mut fast_in_flight = false;
    let mut slow_in_flight = false;
    let mut ingest_health_in_flight = false;

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
                            KeyCode::Char('r') => { fast.reset_immediately(); slow.reset_immediately(); ingest_health.reset_immediately(); }
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
            _ = ingest_health.tick(), if !app.paused && !ingest_health_in_flight => {
                ingest_health_in_flight = true;
                let url = ingest_health_url.clone();
                let sender = ingest_health_tx.clone();
                tokio::spawn(async move {
                    let result = tokio::time::timeout(Duration::from_secs(3), fetch_ingest_health(&url))
                        .await
                        .map_err(|_| "ingest health request timed out".to_owned())
                        .and_then(|result| result);
                    let _ = sender.send(result).await;
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
            result = ingest_health_rx.recv(), if ingest_health_in_flight => {
                ingest_health_in_flight = false;
                match result {
                    Some(result) => app.apply_ingest_health(result),
                    None => app.apply_ingest_health(Err("ingest health worker stopped".into())),
                }
            }
        }
    }
}

async fn fetch_ingest_health(url: &str) -> Result<IngestHealth, String> {
    let url = url.to_owned();
    tokio::task::spawn_blocking(move || fetch_ingest_health_blocking(&url))
        .await
        .map_err(|error| format!("ingest health worker failed: {error}"))?
}

fn fetch_ingest_health_blocking(url: &str) -> Result<IngestHealth, String> {
    let url = url
        .strip_prefix("http://")
        .ok_or_else(|| "INGEST_HEALTH_URL must use http://".to_owned())?;
    let (authority, path) = url.split_once('/').unwrap_or((url, ""));
    if authority.is_empty() {
        return Err("INGEST_HEALTH_URL has no host".into());
    }
    let (host, port) = host_and_port(authority)?;
    let address_text = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let address = address_text
        .to_socket_addrs()
        .map_err(|error| format!("could not resolve ingest health host: {error}"))?
        .next()
        .ok_or_else(|| "ingest health host resolved to no addresses".to_owned())?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .map_err(|error| format!("could not connect to ingest health endpoint: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| format!("could not set ingest health timeout: {error}"))?;
    let request_path = format!("/{path}");
    let request =
        format!("GET {request_path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("could not request ingest health: {error}"))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| format!("could not read ingest health: {error}"))?;
    let (headers, body) = response
        .split_once_bytes(b"\r\n\r\n")
        .ok_or_else(|| "ingest health returned an invalid HTTP response".to_owned())?;
    let status = headers
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| line.split(|byte| *byte == b' ').nth(1))
        .and_then(|status| std::str::from_utf8(status).ok())
        .ok_or_else(|| "ingest health returned no HTTP status".to_owned())?;
    if status != "200" {
        return Err(format!("ingest health returned HTTP {status}"));
    }
    let response = serde_json::from_slice::<IngestHealthResponse>(body)
        .map_err(|error| format!("could not decode ingest health response: {error}"))?;
    response
        .ok
        .then_some(())
        .ok_or_else(|| "ingest handler is unhealthy".to_owned())?;
    Ok(IngestHealth {
        pending_events: response.pending_events,
        batch_capacity: response.batch_capacity,
    })
}

fn host_and_port(authority: &str) -> Result<(&str, u16), String> {
    if let Some(host) = authority.strip_prefix('[') {
        let (host, port) = host
            .split_once(']')
            .ok_or_else(|| "INGEST_HEALTH_URL has an invalid IPv6 host".to_owned())?;
        let port = port
            .strip_prefix(':')
            .unwrap_or("8081")
            .parse()
            .map_err(|_| "INGEST_HEALTH_URL has an invalid port".to_owned())?;
        return Ok((host, port));
    }
    if let Some((host, port)) = authority.rsplit_once(':')
        && let Ok(port) = port.parse()
    {
        return Ok((host, port));
    }
    Ok((authority, 80))
}

fn default_batch_capacity() -> usize {
    200
}

trait SplitOnceBytes {
    fn split_once_bytes(&self, needle: &[u8]) -> Option<(&[u8], &[u8])>;
}

impl SplitOnceBytes for [u8] {
    fn split_once_bytes(&self, needle: &[u8]) -> Option<(&[u8], &[u8])> {
        self.windows(needle.len())
            .position(|window| window == needle)
            .map(|index| (&self[..index], &self[index + needle.len()..]))
    }
}

#[cfg(test)]
mod tests {
    use super::host_and_port;

    #[test]
    fn parses_ingest_health_authorities() {
        assert_eq!(
            host_and_port("127.0.0.1:8081").unwrap(),
            ("127.0.0.1", 8081)
        );
        assert_eq!(host_and_port("localhost").unwrap(), ("localhost", 80));
        assert_eq!(host_and_port("[::1]:8081").unwrap(), ("::1", 8081));
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
