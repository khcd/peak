use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    config::Limits,
    contract::{IdShape, validate_attributes},
    error::EventError,
    manifest::Tenant,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncomingEvent {
    pub event_id: Uuid,
    pub event_name: String,
    pub schema_version: u16,
    pub occurred_at: String,
    pub subject: IncomingSubject,
    pub session_id: Option<String>,
    pub resource: IncomingResource,
    pub attributes: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncomingSubject {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncomingResource {
    pub service_name: String,
    pub service_version: String,
    pub platform: Option<String>,
    pub platform_version: Option<String>,
}

#[derive(Debug, Serialize, Row)]
pub struct EventRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    pub producer: String,
    pub event_name: String,
    pub schema_version: u16,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub occurred_at: OffsetDateTime,
    pub subject_kind: String,
    pub subject_id: String,
    pub session_id: String,
    pub service_name: String,
    pub service_version: String,
    pub platform: String,
    pub platform_version: String,
    pub attributes: String,
    pub country: String,
    pub ingest_version: String,
}

impl IncomingEvent {
    pub fn validate(
        self,
        producer: &'static Tenant,
        limits: &Limits,
        country: &str,
        ingest_version: &str,
    ) -> Result<EventRow, EventError> {
        if self.event_name.is_empty() || self.event_name.len() > 128 {
            return Err(EventError::invalid_envelope(
                "event_name must be between 1 and 128 bytes",
            ));
        }
        let occurred_at = OffsetDateTime::parse(&self.occurred_at, &Rfc3339).map_err(|_| {
            EventError::invalid_envelope("occurred_at must be an RFC 3339 timestamp")
        })?;
        let now = OffsetDateTime::now_utc();
        if occurred_at > now + time::Duration::seconds(limits.max_future_skew_seconds) {
            return Err(EventError::invalid_envelope(
                "occurred_at is too far in the future",
            ));
        }
        if occurred_at < now - time::Duration::days(limits.max_event_age_days) {
            return Err(EventError::invalid_envelope(
                "occurred_at is older than the accepted offline-delivery window",
            ));
        }
        let subject_kind = producer
            .subject_kinds
            .iter()
            .find(|kind| kind.kind == self.subject.kind)
            .ok_or_else(|| {
                EventError::invalid_envelope("subject.kind is not allowed for this producer")
            })?;
        match subject_kind.id_shape {
            IdShape::Uuid if Uuid::parse_str(&self.subject.id).is_err() => {
                return Err(EventError::invalid_envelope(
                    "subject.id must be a UUID for this subject kind",
                ));
            }
            IdShape::Opaque { max_bytes }
                if self.subject.id.is_empty() || self.subject.id.len() > max_bytes =>
            {
                return Err(EventError::invalid_envelope(format!(
                    "subject.id must contain 1 to {max_bytes} bytes"
                )));
            }
            _ => {}
        }
        validate_string("service_name", &self.resource.service_name, 64, true)?;
        if let Some(services) = &producer.services
            && !services
                .iter()
                .any(|service| service == &self.resource.service_name)
        {
            return Err(EventError::invalid_envelope(
                "resource.service_name is not allowed for this tenant",
            ));
        }
        validate_string("service_version", &self.resource.service_version, 64, true)?;
        let platform = self.resource.platform.unwrap_or_default();
        let platform_version = self.resource.platform_version.unwrap_or_default();
        validate_string("platform", &platform, 64, false)?;
        validate_string("platform_version", &platform_version, 128, false)?;
        let session_id = self.session_id.unwrap_or_default();
        if session_id.len() > 128 {
            return Err(EventError::invalid_envelope(
                "session_id must be at most 128 bytes",
            ));
        }
        let contract = producer
            .contract(&self.event_name, self.schema_version)
            .ok_or_else(|| {
                EventError::unknown_contract(format!(
                    "no contract for producer '{}', event '{}', schema version {}",
                    producer.name, self.event_name, self.schema_version
                ))
            })?;
        validate_attributes(contract, &self.attributes)?;
        let attributes = serde_json::to_string(&self.attributes)
            .expect("serializing serde_json::Value cannot fail");
        if attributes.len() > limits.max_attributes_bytes {
            return Err(EventError::too_large(format!(
                "attributes is {} bytes; the maximum is {}",
                attributes.len(),
                limits.max_attributes_bytes
            )));
        }
        Ok(EventRow {
            event_id: self.event_id,
            producer: producer.name.clone(),
            event_name: self.event_name,
            schema_version: self.schema_version,
            occurred_at,
            subject_kind: self.subject.kind,
            subject_id: self.subject.id,
            session_id,
            service_name: self.resource.service_name,
            service_version: self.resource.service_version,
            platform,
            platform_version,
            attributes,
            country: country.into(),
            ingest_version: ingest_version.into(),
        })
    }
}

fn validate_string(
    name: &str,
    value: &str,
    maximum: usize,
    required: bool,
) -> Result<(), EventError> {
    if value.len() > maximum || (required && value.is_empty()) {
        return Err(EventError::invalid_envelope(format!(
            "{name} must contain {} to {maximum} bytes",
            if required { 1 } else { 0 }
        )));
    }
    Ok(())
}
