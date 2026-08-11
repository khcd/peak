use serde_json::Value;

use crate::error::EventError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Bool,
    U64,
    I64,
    F64,
    Str { max_bytes: usize },
    Enum(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSpec {
    pub name: String,
    pub ty: FieldType,
    pub required: bool,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventContract {
    pub event_name: String,
    pub schema_version: u16,
    pub fields: Vec<FieldSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdShape {
    Uuid,
    Opaque { max_bytes: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectKind {
    pub kind: String,
    pub id_shape: IdShape,
}

pub fn validate_attributes(contract: &EventContract, attributes: &Value) -> Result<(), EventError> {
    let Some(attributes) = attributes.as_object() else {
        return Err(EventError::invalid_attributes(
            "attributes must be a JSON object",
        ));
    };
    for key in attributes.keys() {
        if !contract.fields.iter().any(|field| field.name == *key) {
            return Err(EventError::invalid_attributes(format!(
                "attribute '{key}' is not permitted for this event"
            )));
        }
    }
    for field in &contract.fields {
        match attributes.get(&field.name) {
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
    let valid = match &field.ty {
        FieldType::Bool => value.is_boolean(),
        FieldType::U64 => value.as_u64().is_some(),
        FieldType::I64 => value.as_i64().is_some(),
        FieldType::F64 => value.as_f64().is_some(),
        FieldType::Str { max_bytes } => value
            .as_str()
            .is_some_and(|value| !value.is_empty() && value.len() <= *max_bytes),
        FieldType::Enum(values) => value
            .as_str()
            .is_some_and(|value| values.iter().any(|item| item == value)),
    };
    valid.then_some(()).ok_or_else(|| {
        EventError::invalid_attributes(format!("attribute '{}' has an invalid value", field.name))
    })
}
