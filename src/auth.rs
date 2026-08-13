use std::collections::{HashMap, HashSet};

use axum::http::{HeaderMap, header};
use sha2::{Digest, Sha256};

use crate::{
    error::ApiError,
    manifest::{Registry, Tenant},
};

#[derive(Clone)]
pub struct ProducerRegistry {
    by_digest: HashMap<[u8; 32], &'static Tenant>,
}

impl ProducerRegistry {
    pub fn from_pairs(value: &str, registry: &'static Registry) -> Result<Self, String> {
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
            let producer = registry
                .get(name)
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

    pub fn authenticate(&self, headers: &HeaderMap) -> Result<&'static Tenant, ApiError> {
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

pub(crate) fn valid_producer_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Mints a secret that satisfies `from_pairs` by construction: hex is well over the 16-byte
/// minimum, and it contains neither of the `,` and `:` delimiters the parser splits on.
pub(crate) fn generate_secret() -> String {
    use std::fmt::Write;
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("operating system random number generator unavailable");
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use super::{ProducerRegistry, generate_secret, valid_producer_name};
    use crate::manifest::Registry;
    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
    use std::path::Path;
    #[test]
    fn validates_producer_names() {
        assert!(valid_producer_name("producer"));
        assert!(!valid_producer_name("Producer"));
    }

    #[test]
    fn generated_secrets_are_parser_safe_and_random() {
        let first = generate_secret();
        let second = generate_secret();
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!first.contains(',') && !first.contains(':'));
        assert!(first.len() >= 16);
        assert_ne!(first, second);
    }

    #[test]
    fn generated_secret_authenticates_for_the_right_tenant() {
        let registry = Box::leak(Box::new(Registry::load(Path::new("tenants")).unwrap()));
        let tenant_name = registry.first().unwrap().name.clone();
        let secret = generate_secret();
        let producers =
            ProducerRegistry::from_pairs(&format!("{tenant_name}:{secret}"), registry).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {secret}")).unwrap(),
        );
        assert_eq!(producers.authenticate(&headers).unwrap().name, tenant_name);
    }
}
