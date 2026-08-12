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
    Struct { fields: Vec<FieldSpec> },
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
    validate_object(&contract.fields, attributes, None)
}

fn validate_object(
    fields: &[FieldSpec],
    attributes: &Value,
    parent_name: Option<&str>,
) -> Result<(), EventError> {
    let Some(attributes) = attributes.as_object() else {
        return Err(EventError::invalid_attributes(match parent_name {
            Some(name) => format!("attribute '{name}' must be a JSON object"),
            None => "attributes must be a JSON object".into(),
        }));
    };
    for key in attributes.keys() {
        if !fields.iter().any(|field| field.name == *key) {
            let name = parent_name
                .map(|parent| format!("{parent}.{key}"))
                .unwrap_or_else(|| key.clone());
            return Err(EventError::invalid_attributes(format!(
                "attribute '{name}' is not permitted for this event"
            )));
        }
    }
    for field in fields {
        let name = parent_name
            .map(|parent| format!("{parent}.{}", field.name))
            .unwrap_or_else(|| field.name.clone());
        match attributes.get(&field.name) {
            None if field.required => {
                return Err(EventError::invalid_attributes(format!(
                    "attribute '{name}' is required"
                )));
            }
            None => {}
            Some(value) if value.is_null() && field.nullable => {}
            Some(value) if value.is_null() => {
                return Err(EventError::invalid_attributes(format!(
                    "attribute '{name}' may not be null"
                )));
            }
            Some(value) => validate_value(field, value, &name)?,
        }
    }
    Ok(())
}

fn validate_value(field: &FieldSpec, value: &Value, name: &str) -> Result<(), EventError> {
    if let FieldType::Struct { fields } = &field.ty {
        return validate_object(fields, value, Some(name));
    }
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
        FieldType::Struct { .. } => unreachable!("struct fields are validated recursively"),
    };
    valid.then_some(()).ok_or_else(|| {
        EventError::invalid_attributes(format!("attribute '{name}' has an invalid value"))
    })
}

#[cfg(test)]
mod tests {
    use super::{EventContract, FieldSpec, FieldType, validate_attributes};

    fn contract() -> EventContract {
        EventContract {
            event_name: "model_loaded".into(),
            schema_version: 1,
            fields: vec![FieldSpec {
                name: "model".into(),
                ty: FieldType::Struct {
                    fields: vec![
                        FieldSpec {
                            name: "name".into(),
                            ty: FieldType::Str { max_bytes: 32 },
                            required: true,
                            nullable: false,
                        },
                        FieldSpec {
                            name: "load_ms".into(),
                            ty: FieldType::U64,
                            required: true,
                            nullable: false,
                        },
                    ],
                },
                required: true,
                nullable: false,
            }],
        }
    }

    #[test]
    fn nested_structs_validate_recursively() {
        let contract = contract();
        assert!(
            validate_attributes(
                &contract,
                &serde_json::json!({"model": {"name": "sd", "load_ms": 12}})
            )
            .is_ok()
        );
        assert!(
            validate_attributes(
                &contract,
                &serde_json::json!({"model": {"name": "sd", "extra": true}})
            )
            .is_err()
        );
        assert!(
            validate_attributes(&contract, &serde_json::json!({"model": {"name": "sd"}})).is_err()
        );
    }
}
