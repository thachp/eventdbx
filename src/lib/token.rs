use std::{
    collections::HashSet,
    fmt, fs, io,
    path::{Path, PathBuf},
};

use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    encryption::{self, Encryptor},
    error::{EventError, Result},
    store::ActorClaims,
};

pub const ROOT_ACTION: &str = "*.*";
pub const ROOT_RESOURCE: &str = "*";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TokenStatus {
    Active,
    Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct JwtLimits {
    pub write_events: Option<u64>,
    pub keep_alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub jti: String,
    pub iat: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nbf: Option<i64>,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub actions: Vec<String>,
    #[serde(default)]
    pub resources: Vec<String>,
    #[serde(default)]
    pub issued_by: String,
    #[serde(default)]
    pub limits: JwtLimits,
}

impl JwtClaims {
    pub fn actor_claims(&self) -> Option<ActorClaims> {
        if self.group.is_empty() || self.user.is_empty() {
            return None;
        }
        Some(ActorClaims {
            group: self.group.clone(),
            user: self.user.clone(),
        })
    }

    pub fn allows_action(&self, action: &str) -> bool {
        if self.actions.is_empty() {
            return false;
        }
        self.actions
            .iter()
            .any(|pattern| wildcard_matches(pattern, action))
    }

    pub fn allows_resource(&self, resource: Option<&str>) -> bool {
        let Some(resource) = resource else {
            // No resource provided means action-level authorization only.
            return true;
        };
        if self.resources.is_empty() {
            return true;
        }
        self.resources
            .iter()
            .any(|pattern| wildcard_matches(pattern, resource))
    }

    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.exp.and_then(|ts| DateTime::from_timestamp(ts, 0))
    }
}

#[derive(Debug, Clone)]
pub struct IssueTokenInput {
    pub subject: String,
    pub group: String,
    pub user: String,
    pub actions: Vec<String>,
    pub resources: Vec<String>,
    pub ttl_secs: Option<u64>,
    pub not_before: Option<DateTime<Utc>>,
    pub issued_by: String,
    pub limits: JwtLimits,
}

impl IssueTokenInput {
    pub fn ensure_defaults(mut self) -> Self {
        if self.subject.trim().is_empty() {
            self.subject = format!("{}:{}", self.group, self.user);
        }
        if self.resources.is_empty() {
            self.resources = vec![ROOT_RESOURCE.to_string()];
        }
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub token: String,
    pub jti: String,
    pub subject: String,
    pub group: String,
    pub user: String,
    pub actions: Vec<String>,
    pub resources: Vec<String>,
    pub issued_at: DateTime<Utc>,
    #[serde(default)]
    pub not_before: Option<DateTime<Utc>>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    pub issued_by: String,
    pub status: TokenStatus,
    #[serde(default)]
    pub limits: JwtLimits,
}

impl TokenRecord {
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        if !matches!(self.status, TokenStatus::Active) {
            return false;
        }
        if let Some(exp) = self.expires_at {
            if exp < now {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct RevokeTokenInput {
    pub token_or_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokedTokenRecord {
    pub jti: String,
    pub revoked_at: DateTime<Utc>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct JwtManagerConfig {
    pub issuer: String,
    pub audience: String,
    pub private_key: Vec<u8>,
    pub public_key: Vec<u8>,
    pub key_id: Option<String>,
    pub default_ttl: Duration,
    pub clock_skew: Duration,
}

pub struct TokenManager {
    ledger_path: PathBuf,
    encryptor: Option<Encryptor>,
    records: RwLock<Vec<TokenRecord>>,
    revocations: RevocationStore,
    header: Header,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    public_key: Vec<u8>,
    issuer: String,
    audience: String,
    default_ttl: Duration,
    clock_skew: Duration,
}

impl fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenManager")
            .field("ledger_path", &self.ledger_path)
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .finish()
    }
}

#[derive(Debug)]
pub struct AccessRequest<'a> {
    pub action: &'a str,
    pub resource: Option<&'a str>,
}

#[derive(Debug)]
struct RevocationStore {
    path: PathBuf,
    encryptor: Option<Encryptor>,
    records: RwLock<Vec<RevokedTokenRecord>>,
}

impl TokenManager {
    pub fn load(
        config: JwtManagerConfig,
        ledger_path: PathBuf,
        revocation_path: PathBuf,
        encryptor: Option<Encryptor>,
    ) -> Result<Self> {
        if let Some(parent) = ledger_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = revocation_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let records = RwLock::new(read_token_records(&ledger_path, encryptor.as_ref())?);
        let revocations = RevocationStore::load(revocation_path, encryptor.clone())?;

        let mut header = Header::new(Algorithm::EdDSA);
        if let Some(kid) = config.key_id {
            header.kid = Some(kid);
        }

        let encoding_key = EncodingKey::from_ed_der(&config.private_key);
        let public_key_b64 =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&config.public_key);
        let decoding_key = DecodingKey::from_ed_components(&public_key_b64)
            .map_err(|err| EventError::Config(format!("invalid jwt public key material: {err}")))?;

        Ok(Self {
            ledger_path,
            encryptor,
            records,
            revocations,
            header,
            encoding_key,
            decoding_key,
            public_key: config.public_key.clone(),
            issuer: config.issuer,
            audience: config.audience,
            default_ttl: config.default_ttl,
            clock_skew: config.clock_skew,
        })
    }

    pub fn public_key(&self) -> &[u8] {
        &self.public_key
    }

    pub fn issue(&self, input: IssueTokenInput) -> Result<TokenRecord> {
        let payload = input.ensure_defaults();
        let now = Utc::now();
        let ttl = payload
            .ttl_secs
            .map(|secs| {
                let clamped = secs.min(i64::MAX as u64);
                Duration::seconds(clamped as i64)
            })
            .unwrap_or(self.default_ttl);
        let expires_at = if ttl.is_zero() { None } else { Some(now + ttl) };
        if payload.actions.is_empty() {
            return Err(EventError::Config(
                "token actions must contain at least one entry".to_string(),
            ));
        }

        let claims = JwtClaims {
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            sub: payload.subject.clone(),
            jti: Uuid::new_v4().to_string(),
            iat: now.timestamp(),
            exp: expires_at.map(|ts| ts.timestamp()),
            nbf: payload.not_before.map(|ts| ts.timestamp()),
            group: payload.group.clone(),
            user: payload.user.clone(),
            actions: payload.actions.clone(),
            resources: payload.resources.clone(),
            issued_by: payload.issued_by.clone(),
            limits: payload.limits.clone(),
        };

        let token = encode(&self.header, &claims, &self.encoding_key)
            .map_err(|err| EventError::Storage(format!("failed to encode jwt: {err}")))?;

        let record = TokenRecord {
            token,
            jti: claims.jti.clone(),
            subject: payload.subject,
            group: payload.group,
            user: payload.user,
            actions: claims.actions.clone(),
            resources: claims.resources.clone(),
            issued_at: now,
            not_before: payload.not_before,
            expires_at,
            issued_by: payload.issued_by,
            status: TokenStatus::Active,
            limits: payload.limits,
        };

        self.persist_with(|records| {
            records.push(record.clone());
        })?;

        Ok(record)
    }

    pub fn authorize(&self, token: &str, request: AccessRequest<'_>) -> Result<JwtClaims> {
        match self.decode(token) {
            Ok(claims) => {
                self.ensure_active(&claims)?;
                self.check_permissions(&claims, request)?;
                Ok(claims)
            }
            Err(_) => self.authorize_legacy(token, request),
        }
    }

    pub fn authorize_action(
        &self,
        token: &str,
        action: &str,
        resource: Option<&str>,
    ) -> Result<JwtClaims> {
        self.authorize(token, AccessRequest { action, resource })
    }

    pub fn verify(&self, token: &str) -> Result<JwtClaims> {
        let claims = self.decode(token)?;
        self.ensure_active(&claims)?;
        Ok(claims)
    }

    fn authorize_legacy(&self, token: &str, request: AccessRequest<'_>) -> Result<JwtClaims> {
        self.reload_from_disk()?;
        let now = Utc::now();
        let record = {
            let records = self.records.read();
            records.iter().find(|record| record.token == token).cloned()
        };
        let Some(record) = record else {
            return Err(EventError::InvalidToken);
        };

        if !record.is_active(now) {
            return Err(EventError::InvalidToken);
        }

        if self.revocations.is_revoked(&record.jti)? {
            return Err(EventError::InvalidToken);
        }

        let actions = if record.actions.is_empty() {
            vec![ROOT_ACTION.to_string()]
        } else {
            record.actions.clone()
        };
        let resources = if record.resources.is_empty() {
            vec![ROOT_RESOURCE.to_string()]
        } else {
            record.resources.clone()
        };

        let claims = JwtClaims {
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            sub: record.subject.clone(),
            jti: record.jti.clone(),
            iat: record.issued_at.timestamp(),
            exp: record.expires_at.map(|ts| ts.timestamp()),
            nbf: record.not_before.map(|ts| ts.timestamp()),
            group: record.group.clone(),
            user: record.user.clone(),
            actions,
            resources,
            issued_by: record.issued_by.clone(),
            limits: record.limits.clone(),
        };

        self.check_permissions(&claims, request)?;
        Ok(claims)
    }

    fn check_permissions(&self, claims: &JwtClaims, request: AccessRequest<'_>) -> Result<()> {
        if !claims.allows_action(request.action) {
            return Err(EventError::Unauthorized);
        }
        if !claims.allows_resource(request.resource) {
            return Err(EventError::Unauthorized);
        }
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<TokenRecord>> {
        self.reload_from_disk()?;
        let now = Utc::now();
        let records = self.records.read();
        let mut filtered = records.clone();
        filtered.retain(|record| {
            record.status == TokenStatus::Active
                || record.expires_at.map(|ts| ts > now).unwrap_or(true)
        });
        Ok(filtered)
    }

    pub fn revoke(&self, input: RevokeTokenInput) -> Result<RevokedTokenRecord> {
        let now = Utc::now();
        let (jti, expires_at) = {
            let mut target = None;
            self.persist_with(|records| {
                for record in records.iter_mut() {
                    if record.token == input.token_or_id || record.jti == input.token_or_id {
                        record.status = TokenStatus::Revoked;
                        target = Some((record.jti.clone(), record.expires_at));
                        break;
                    }
                }
            })?;

            target.ok_or(EventError::InvalidToken)?
        };

        let revoked = RevokedTokenRecord {
            jti,
            revoked_at: now,
            reason: input.reason,
            expires_at,
        };
        self.revocations.insert(revoked.clone())?;
        Ok(revoked)
    }

    pub fn refresh(&self, token_or_id: &str, ttl_secs: Option<u64>) -> Result<TokenRecord> {
        self.reload_from_disk()?;
        let existing = {
            let records = self.records.read();
            records
                .iter()
                .find(|record| record.token == token_or_id || record.jti == token_or_id)
                .cloned()
        };
        let Some(record) = existing else {
            return Err(EventError::InvalidToken);
        };

        self.revocations.insert(RevokedTokenRecord {
            jti: record.jti.clone(),
            revoked_at: Utc::now(),
            reason: Some("refreshed".to_string()),
            expires_at: record.expires_at,
        })?;

        self.persist_with(|records| {
            records
                .iter_mut()
                .filter(|entry| entry.jti == record.jti)
                .for_each(|entry| entry.status = TokenStatus::Revoked);
        })?;

        let issued_by = if record.issued_by.is_empty() {
            record.subject.clone()
        } else {
            record.issued_by.clone()
        };

        let mut input = IssueTokenInput {
            subject: record.subject.clone(),
            group: record.group.clone(),
            user: record.user.clone(),
            actions: if record.actions.is_empty() {
                vec![ROOT_ACTION.to_string()]
            } else {
                record.actions.clone()
            },
            resources: if record.resources.is_empty() {
                vec![ROOT_RESOURCE.to_string()]
            } else {
                record.resources.clone()
            },
            ttl_secs,
            not_before: None,
            issued_by,
            limits: record.limits.clone(),
        };
        if input.ttl_secs.is_none() && record.expires_at.is_some() {
            let ttl = record
                .expires_at
                .map(|exp| exp - record.issued_at)
                .unwrap_or_else(|| self.default_ttl);
            input.ttl_secs = Some(ttl.num_seconds().max(0) as u64);
        }
        self.issue(input)
    }

    fn decode(&self, token: &str) -> Result<JwtClaims> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.required_spec_claims = HashSet::new();

        let decoded = decode::<JwtClaims>(token, &self.decoding_key, &validation)
            .map_err(|_| EventError::InvalidToken)?;
        Ok(decoded.claims)
    }

    fn ensure_active(&self, claims: &JwtClaims) -> Result<()> {
        if claims.iss != self.issuer || claims.aud != self.audience {
            return Err(EventError::Unauthorized);
        }

        let now = Utc::now();
        if let Some(exp) = claims.exp {
            let exp = DateTime::from_timestamp(exp, 0).ok_or_else(|| EventError::InvalidToken)?;
            if now - self.clock_skew > exp {
                return Err(EventError::TokenExpired);
            }
        }
        if let Some(nbf) = claims.nbf {
            let nbf = DateTime::from_timestamp(nbf, 0).ok_or_else(|| EventError::InvalidToken)?;
            if now + self.clock_skew < nbf {
                return Err(EventError::Unauthorized);
            }
        }
        let iat =
            DateTime::from_timestamp(claims.iat, 0).ok_or_else(|| EventError::InvalidToken)?;
        if iat - self.clock_skew > now {
            return Err(EventError::Unauthorized);
        }

        if self.revocations.is_revoked(&claims.jti)? {
            return Err(EventError::InvalidToken);
        }

        Ok(())
    }

    fn persist_with<F>(&self, mut mutator: F) -> Result<()>
    where
        F: FnMut(&mut Vec<TokenRecord>),
    {
        let mut records = self.records.write();
        self.refresh_records_internal(&mut records)?;
        mutator(&mut records);
        write_token_records(&self.ledger_path, &records, self.encryptor.as_ref())?;
        Ok(())
    }

    fn refresh_records_internal(&self, records: &mut Vec<TokenRecord>) -> Result<()> {
        let disk = read_token_records(&self.ledger_path, self.encryptor.as_ref())?;
        *records = disk;
        Ok(())
    }

    fn reload_from_disk(&self) -> Result<()> {
        let mut records = self.records.write();
        self.refresh_records_internal(&mut records)
    }
}

impl RevocationStore {
    fn load(path: PathBuf, encryptor: Option<Encryptor>) -> Result<Self> {
        let records = RwLock::new(read_revocations(&path, encryptor.as_ref())?);
        Ok(Self {
            path,
            encryptor,
            records,
        })
    }

    fn insert(&self, record: RevokedTokenRecord) -> Result<()> {
        let mut records = self.records.write();
        let mut replaced = false;
        for existing in records.iter_mut() {
            if existing.jti == record.jti {
                *existing = record.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            records.push(record);
        }
        self.persist(&records)
    }

    fn is_revoked(&self, jti: &str) -> Result<bool> {
        let mut records = self.records.write();
        let now = Utc::now();

        records.retain(|record| match record.expires_at {
            Some(exp) if exp < now => false,
            _ => true,
        });

        let revoked = records.iter().any(|record| record.jti == jti);
        if revoked {
            return Ok(true);
        }

        // Refresh from disk in case another process updated it.
        *records = read_revocations(&self.path, self.encryptor.as_ref())?;
        let revoked_on_disk = records.iter().any(|record| record.jti == jti);
        Ok(revoked_on_disk)
    }

    fn persist(&self, records: &[RevokedTokenRecord]) -> Result<()> {
        write_revocations(&self.path, records, self.encryptor.as_ref())
    }
}

fn wildcard_matches(pattern: &str, candidate: &str) -> bool {
    if pattern == ROOT_ACTION || pattern == ROOT_RESOURCE {
        return true;
    }
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let text_chars = candidate.chars().collect::<Vec<_>>();
    let (mut p_idx, mut t_idx) = (0usize, 0usize);
    let mut star_idx: Option<usize> = None;
    let mut match_idx = 0usize;

    while t_idx < text_chars.len() {
        if p_idx < pattern_chars.len()
            && (pattern_chars[p_idx] == text_chars[t_idx] || pattern_chars[p_idx] == '?')
        {
            p_idx += 1;
            t_idx += 1;
        } else if p_idx < pattern_chars.len() && pattern_chars[p_idx] == '*' {
            star_idx = Some(p_idx);
            match_idx = t_idx;
            p_idx += 1;
        } else if let Some(star) = star_idx {
            p_idx = star + 1;
            match_idx += 1;
            t_idx = match_idx;
        } else {
            return false;
        }
    }

    while p_idx < pattern_chars.len() && pattern_chars[p_idx] == '*' {
        p_idx += 1;
    }

    p_idx == pattern_chars.len()
}

fn read_token_records(path: &Path, encryptor: Option<&Encryptor>) -> Result<Vec<TokenRecord>> {
    let contents = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(EventError::Io(err)),
    };

    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if let Some(enc) = encryptor {
        if encryption::is_encrypted_blob(trimmed) {
            let bytes = enc.decrypt_from_str(trimmed)?;
            if bytes.is_empty() {
                return Ok(Vec::new());
            }
            let records: Vec<TokenRecord> = serde_json::from_slice(&bytes)?;
            Ok(records)
        } else {
            let records: Vec<TokenRecord> = serde_json::from_str(trimmed)?;
            write_token_records(path, &records, Some(enc))?;
            Ok(records)
        }
    } else {
        if encryption::is_encrypted_blob(trimmed) {
            return Err(EventError::Config(
                "data encryption key must be configured to read encrypted tokens".to_string(),
            ));
        }
        let records: Vec<TokenRecord> = serde_json::from_str(trimmed)?;
        Ok(records)
    }
}

fn write_token_records(
    path: &Path,
    records: &[TokenRecord],
    encryptor: Option<&Encryptor>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec(records)?;
    if let Some(enc) = encryptor {
        let encrypted = enc.encrypt_to_string(&payload)?;
        fs::write(path, encrypted)?;
    } else {
        fs::write(path, payload)?;
    }
    Ok(())
}

fn read_revocations(path: &Path, encryptor: Option<&Encryptor>) -> Result<Vec<RevokedTokenRecord>> {
    let contents = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(EventError::Io(err)),
    };

    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if let Some(enc) = encryptor {
        if encryption::is_encrypted_blob(trimmed) {
            let bytes = enc.decrypt_from_str(trimmed)?;
            if bytes.is_empty() {
                return Ok(Vec::new());
            }
            let records: Vec<RevokedTokenRecord> = serde_json::from_slice(&bytes)?;
            Ok(records)
        } else {
            let records: Vec<RevokedTokenRecord> = serde_json::from_str(trimmed)?;
            write_revocations(path, &records, Some(enc))?;
            Ok(records)
        }
    } else {
        if encryption::is_encrypted_blob(trimmed) {
            return Err(EventError::Config(
                "data encryption key must be configured to read encrypted revocations".to_string(),
            ));
        }
        let records: Vec<RevokedTokenRecord> = serde_json::from_str(trimmed)?;
        Ok(records)
    }
}

fn write_revocations(
    path: &Path,
    records: &[RevokedTokenRecord],
    encryptor: Option<&Encryptor>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec(records)?;
    if let Some(enc) = encryptor {
        let encrypted = enc.encrypt_to_string(&payload)?;
        fs::write(path, encrypted)?;
    } else {
        fs::write(path, payload)?;
    }
    Ok(())
}
