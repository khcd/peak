use std::collections::{HashMap, HashSet};

use axum::http::{HeaderMap, header};
use sha2::{Digest, Sha256};

use crate::{
    contract::{PRODUCERS, ProducerSpec},
    error::ApiError,
};

#[derive(Clone)]
pub struct ProducerRegistry {
    by_digest: HashMap<[u8; 32], &'static ProducerSpec>,
}

impl ProducerRegistry {
    pub fn from_pairs(value: &str) -> Result<Self, String> {
        let mut by_digest = HashMap::new();
        let mut names = HashSet::new();
        let mut found = false;
        for pair in value.split(',') {
            let (name, secret) = pair.split_once(':').ok_or_else(|| {
                "INGEST_KEYS entries must have the form producer:secret".to_string()
            })?;
            found = true;
            if !valid_producer_name(name) {
                return Err(format!("invalid producer name '{name}'"));
            }
            if !names.insert(name) {
                return Err(format!("duplicate producer name '{name}'"));
            }
            if secret.len() < 16 {
                return Err(format!(
                    "secret for producer '{name}' must be at least 16 bytes"
                ));
            }
            let producer = PRODUCERS
                .iter()
                .find(|producer| producer.name == name)
                .ok_or_else(|| format!("unknown producer '{name}'"))?;
            let digest: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
            if by_digest.insert(digest, producer).is_some() {
                return Err("duplicate ingest secret".into());
            }
        }
        found
            .then_some(Self { by_digest })
            .ok_or_else(|| "INGEST_KEYS must not be empty".into())
    }

    pub fn authenticate(&self, headers: &HeaderMap) -> Result<&'static ProducerSpec, ApiError> {
        let Some(value) = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        else {
            return Err(ApiError::unauthorized());
        };
        let Some(token) = value.strip_prefix("Bearer ") else {
            return Err(ApiError::unauthorized());
        };
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        self.by_digest
            .get(&digest)
            .copied()
            .ok_or_else(ApiError::unauthorized)
    }
}

fn valid_producer_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::ProducerRegistry;

    #[test]
    fn rejects_bad_registry_entries() {
        assert!(ProducerRegistry::from_pairs("unknown:1234567890123456").is_err());
        assert!(
            ProducerRegistry::from_pairs("planar:1234567890123456,planar:abcdefghijklmnop")
                .is_err()
        );
        assert!(ProducerRegistry::from_pairs("planar:short").is_err());
    }
}
