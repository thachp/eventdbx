use std::fmt;

use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce, aead::Aead};
use base64::{Engine, engine::general_purpose::STANDARD};
use rand_core::{OsRng, RngCore};
use serde_json::{Map, Value};

use crate::error::{EventError, Result};

const PREFIX: &str = "ENCv1:";
pub const ENCRYPTED_FIELD: &str = "__eventdbx_encrypted";

#[derive(Clone)]
pub struct Encryptor {
    cipher: Aes256Gcm,
}

impl fmt::Debug for Encryptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Encryptor(..)")
    }
}

impl Encryptor {
    pub fn new_from_base64(key_b64: &str) -> Result<Self> {
        let trimmed = key_b64.trim();
        if trimmed.is_empty() {
            return Err(EventError::Config(
                "data encryption key cannot be empty".to_string(),
            ));
        }
        let bytes = STANDARD
            .decode(trimmed)
            .map_err(|err| EventError::Config(format!("invalid data encryption key: {err}")))?;
        if bytes.len() != 32 {
            return Err(EventError::Config(
                "data encryption key must decode to 32 bytes (256 bits)".to_string(),
            ));
        }
        #[allow(deprecated)]
        let key = Key::<Aes256Gcm>::from_slice(&bytes);
        Ok(Self {
            cipher: Aes256Gcm::new(key),
        })
    }

    pub fn encrypt_to_string(&self, plaintext: &[u8]) -> Result<String> {
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        #[allow(deprecated)]
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|err| EventError::Storage(format!("encryption failure: {err}")))?;

        let mut combined = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        combined.extend_from_slice(&nonce_bytes);
        combined.extend_from_slice(&ciphertext);
        Ok(format!("{PREFIX}{}", STANDARD.encode(combined)))
    }

    pub fn decrypt_from_str(&self, data: &str) -> Result<Vec<u8>> {
        if !data.starts_with(PREFIX) {
            return Err(EventError::Storage(
                "encrypted payload missing expected prefix".to_string(),
            ));
        }
        let encoded = &data[PREFIX.len()..];
        let combined = STANDARD
            .decode(encoded)
            .map_err(|err| EventError::Storage(format!("invalid encrypted payload: {err}")))?;
        if combined.len() < 13 {
            return Err(EventError::Storage(
                "encrypted payload too short".to_string(),
            ));
        }
        let (nonce_bytes, ciphertext) = combined.split_at(12);
        #[allow(deprecated)]
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|err| EventError::Storage(format!("failed to decrypt payload: {err}")))?;
        Ok(plaintext)
    }
}

pub fn wrap_encrypted_value(ciphertext: String) -> Value {
    let mut map = Map::new();
    map.insert(ENCRYPTED_FIELD.to_string(), Value::String(ciphertext));
    Value::Object(map)
}

pub fn extract_encrypted_value(value: &Value) -> Option<&str> {
    value
        .as_object()
        .and_then(|map| map.get(ENCRYPTED_FIELD))
        .and_then(Value::as_str)
        .filter(|val| val.starts_with(PREFIX))
}

pub fn is_encrypted_blob(data: &str) -> bool {
    data.starts_with(PREFIX)
}
