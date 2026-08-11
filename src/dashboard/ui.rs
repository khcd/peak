use std::collections::BTreeMap;

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, Borders, List, ListItem, Paragraph, Row, Sparkline, Table, Widget},
};
use time::OffsetDateTime;

use super::app::App;

pub fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),
            Constraint::Length(4),
            Constraint::Length(8),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(area);
    draw_stats(frame, sections[0], app);
    let peak = app.buckets.iter().copied().max().unwrap_or(0);
    Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("incoming · events/sec · last 60s · peak {peak}")),
        )
        .data(app.buckets)
        .max(peak.max(1))
        .style(Style::default().fg(Color::Cyan))
        .render(sections[1], frame.buffer_mut());

    let lower = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(sections[2]);
    draw_events(frame, lower[0], app);
    draw_fleet(frame, lower[1], app);
    draw_tail(frame, sections[3], app);
    let footer = match app.error_message() {
        Some(message) => {
            format!("error: {message} · 1/2/3 window · t tenant · p pause · r refresh · q quit")
        }
        None if app.paused => {
            "paused · 1/2/3 window · t tenant · p resume · r refresh · q quit".into()
        }
        None => "1/2/3 window · t tenant · p pause · r refresh · q quit".into(),
    };
    Paragraph::new(footer).render(sections[4], frame.buffer_mut());
}

fn draw_stats(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let status = match app.error_message() {
        None => "clickhouse ok".into(),
        Some(_) => {
            let stale = app
                .last_ok()
                .map(|time| (OffsetDateTime::now_utc() - time).whole_seconds().max(0))
                .unwrap_or(0);
            format!("clickhouse error · stale {stale}s")
        }
    };
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(38),
            Constraint::Percentage(38),
            Constraint::Percentage(24),
        ])
        .split(area);
    let events = format!(
        "EVENTS\n today {:>14}\n 7d    {:>14}\n 30d   {:>14}",
        number(app.totals.events_today),
        number(app.totals.events_7d),
        number(app.totals.events_30d)
    );
    let installs = format!(
        "{}\n today {:>14}\n 7d    {:>14}\n 30d   {:>14}",
        app.tenant.dashboard.subject_label,
        number(app.totals.subjects_today),
        number(app.totals.subjects_7d),
        number(app.totals.subjects_30d)
    );
    Paragraph::new(events)
        .block(
            Block::default()
                .borders(Borders::LEFT | Borders::TOP | Borders::BOTTOM)
                .title(format!(
                    "{} · telemetry · window {} ({}) · {status}",
                    app.tenant.dashboard.title,
                    app.window.label(app.tenant),
                    app.timezone
                )),
        )
        .render(columns[0], frame.buffer_mut());
    Paragraph::new(installs)
        .block(Block::default().borders(Borders::TOP | Borders::BOTTOM))
        .render(columns[1], frame.buffer_mut());
    let connected = format!(
        "CONNECTED\n now   {:>8}\n\n ping <{}m",
        app.connected.map(number).unwrap_or_else(|| "--".into()),
        app.tenant
            .dashboard
            .offline_after_minutes()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "--".into())
    );
    Paragraph::new(connected)
        .block(Block::default().borders(Borders::RIGHT | Borders::TOP | Borders::BOTTOM))
        .render(columns[2], frame.buffer_mut());
}

fn draw_events(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let top = app.breakdown.first().map(|row| row.events).unwrap_or(1);
    let rows = app.breakdown.iter().map(|row| {
        Row::new(vec![
            row.event_name.clone(),
            number(row.events),
            bar(row.events, top, 8),
        ])
    });
    Table::new(
        rows,
        [
            Constraint::Min(18),
            Constraint::Length(10),
            Constraint::Length(9),
        ],
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("events · {}", app.window.label(app.tenant))),
    )
    .render(area, frame.buffer_mut());
}

fn draw_fleet(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let mut groups: BTreeMap<&str, Vec<_>> = BTreeMap::new();
    for row in &app.fleet {
        groups.entry(&row.dimension).or_default().push(row);
    }
    let rows = groups.into_iter().map(|(dimension, rows)| {
        let total: u64 = rows.iter().map(|row| row.events).sum();
        let summary = rows
            .iter()
            .take(3)
            .map(|row| format!("{} {}%", row.value, percent(row.events, total)))
            .collect::<Vec<_>>()
            .join("  ");
        Row::new(vec![dimension.to_owned(), summary])
    });
    Table::new(rows, [Constraint::Length(10), Constraint::Min(10)])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("fleet · {}", app.window.label(app.tenant))),
        )
        .render(area, frame.buffer_mut());
}

fn draw_tail(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let rows = app
        .tail
        .iter()
        .take(area.height.saturating_sub(2) as usize)
        .map(|row| {
            let time = row.received_at.time();
            ListItem::new(format!(
                "{:02}:{:02}:{:02}  {:<22}  {:.8}  {:<9} {:<10} {}",
                time.hour(),
                time.minute(),
                time.second(),
                row.event_name,
                row.subject_id,
                row.platform,
                row.service_version,
                row.country
            ))
        })
        .collect::<Vec<_>>();
    List::new(rows)
        .block(Block::default().borders(Borders::ALL).title("live tail"))
        .render(area, frame.buffer_mut());
}

fn number(value: u64) -> String {
    let text = value.to_string();
    let first = text.len() % 3;
    text.chars()
        .enumerate()
        .fold(String::new(), |mut output, (index, character)| {
            if index != 0 && (index - first).is_multiple_of(3) {
                output.push(',');
            }
            output.push(character);
            output
        })
}

fn bar(value: u64, top: u64, width: usize) -> String {
    let filled = ((value.saturating_mul(width as u64) + top.saturating_sub(1)) / top)
        .min(width as u64) as usize;
    "█".repeat(filled) + &"░".repeat(width - filled)
}

fn percent(value: u64, total: u64) -> u64 {
    if total == 0 { 0 } else { value * 100 / total }
}
