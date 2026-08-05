use serde_json::Value;

use crate::error::EventError;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum FieldType {
    Bool,
    U64,
    I64,
    F64,
    Str { max_bytes: usize },
    Enum(&'static [&'static str]),
}

#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    pub name: &'static str,
    pub ty: FieldType,
    pub required: bool,
    pub nullable: bool,
}

pub const fn required(name: &'static str, ty: FieldType) -> FieldSpec {
    FieldSpec {
        name,
        ty,
        required: true,
        nullable: false,
    }
}

pub const fn optional(name: &'static str, ty: FieldType) -> FieldSpec {
    FieldSpec {
        name,
        ty,
        required: false,
        nullable: false,
    }
}

pub const fn optional_null(name: &'static str, ty: FieldType) -> FieldSpec {
    FieldSpec {
        name,
        ty,
        required: false,
        nullable: true,
    }
}

#[derive(Debug)]
pub struct EventContract {
    pub producer: &'static str,
    pub event_name: &'static str,
    pub schema_version: u16,
    pub fields: &'static [FieldSpec],
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum IdShape {
    Uuid,
    Opaque { max_bytes: usize },
}

#[derive(Debug)]
pub struct SubjectKind {
    pub kind: &'static str,
    pub id_shape: IdShape,
}

#[derive(Debug)]
pub struct ProducerSpec {
    pub name: &'static str,
    pub subject_kinds: &'static [SubjectKind],
}

const PLANAR_SUBJECT_KINDS: &[SubjectKind] = &[SubjectKind {
    kind: "install",
    id_shape: IdShape::Uuid,
}];

pub static PRODUCERS: &[ProducerSpec] = &[ProducerSpec {
    name: "planar",
    subject_kinds: PLANAR_SUBJECT_KINDS,
}];

const SESSION_END: &[FieldSpec] = &[required("duration_ms", FieldType::U64)];
const GENERATION_REQUESTED: &[FieldSpec] = &[
    required("backend", FieldType::Enum(&["sdcpp", "diffusers"])),
    required("model", FieldType::Str { max_bytes: 256 }),
    required("steps", FieldType::U64),
    required("width", FieldType::U64),
    required("height", FieldType::U64),
    required("sampler", FieldType::Str { max_bytes: 128 }),
];
const GENERATION_COMPLETED: &[FieldSpec] = &[
    required("duration_ms", FieldType::U64),
    required("success", FieldType::Bool),
    optional_null(
        "error_kind",
        FieldType::Enum(&["oom", "model_load", "other"]),
    ),
    optional("backend", FieldType::Enum(&["sdcpp", "diffusers"])),
];
const MODEL_LOADED: &[FieldSpec] = &[
    required("model", FieldType::Str { max_bytes: 256 }),
    required("load_ms", FieldType::U64),
    required("size_mb", FieldType::U64),
];
const FEATURE_USED: &[FieldSpec] = &[required("feature", FieldType::Str { max_bytes: 128 })];

pub static CONTRACTS: &[EventContract] = &[
    EventContract {
        producer: "planar",
        event_name: "session_start",
        schema_version: 1,
        fields: &[],
    },
    EventContract {
        producer: "planar",
        event_name: "session_end",
        schema_version: 1,
        fields: SESSION_END,
    },
    EventContract {
        producer: "planar",
        event_name: "generation_requested",
        schema_version: 1,
        fields: GENERATION_REQUESTED,
    },
    EventContract {
        producer: "planar",
        event_name: "generation_completed",
        schema_version: 1,
        fields: GENERATION_COMPLETED,
    },
    EventContract {
        producer: "planar",
        event_name: "model_loaded",
        schema_version: 1,
        fields: MODEL_LOADED,
    },
    EventContract {
        producer: "planar",
        event_name: "feature_used",
        schema_version: 1,
        fields: FEATURE_USED,
    },
];

pub fn lookup(
    producer: &str,
    event_name: &str,
    schema_version: u16,
) -> Option<&'static EventContract> {
    CONTRACTS.iter().find(|contract| {
        contract.producer == producer
            && contract.event_name == event_name
            && contract.schema_version == schema_version
    })
}

pub fn validate_attributes(contract: &EventContract, attributes: &Value) -> Result<(), EventError> {
    let Some(attributes) = attributes.as_object() else {
        return Err(EventError::invalid_attributes(
            "attributes must be a JSON object",
        ));
    };
    for key in attributes.keys() {
        if !contract.fields.iter().any(|field| field.name == key) {
            return Err(EventError::invalid_attributes(format!(
                "attribute '{key}' is not permitted for this event"
            )));
        }
    }
    for field in contract.fields {
        match attributes.get(field.name) {
            None if field.required => {
                return Err(EventError::invalid_attributes(format!(
                    "attribute '{}' is required",
                    field.name
                )));
            }
            None => {}
            Some(value) if value.is_null() && field.nullable => {}
            Some(value) if value.is_null() => {
                return Err(EventError::invalid_attributes(format!(
                    "attribute '{}' may not be null",
                    field.name
                )));
            }
            Some(value) => validate_value(field, value)?,
        }
    }
    Ok(())
}

fn validate_value(field: &FieldSpec, value: &Value) -> Result<(), EventError> {
    let valid = match field.ty {
        FieldType::Bool => value.is_boolean(),
        FieldType::U64 => value.as_u64().is_some(),
        FieldType::I64 => value.as_i64().is_some(),
        FieldType::F64 => value.as_f64().is_some(),
        FieldType::Str { max_bytes } => value
            .as_str()
            .is_some_and(|value| !value.is_empty() && value.len() <= max_bytes),
        FieldType::Enum(values) => value.as_str().is_some_and(|value| values.contains(&value)),
    };
    valid.then_some(()).ok_or_else(|| {
        EventError::invalid_attributes(format!("attribute '{}' has an invalid value", field.name))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn registry_is_consistent() {
        let producers = PRODUCERS
            .iter()
            .map(|producer| producer.name)
            .collect::<HashSet<_>>();
        assert_eq!(producers.len(), PRODUCERS.len());
        let mut contracts = HashSet::new();
        for contract in CONTRACTS {
            assert!(producers.contains(contract.producer));
            assert!(contracts.insert((
                contract.producer,
                contract.event_name,
                contract.schema_version
            )));
        }
    }
}
