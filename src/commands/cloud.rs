use std::{
    env,
    io::{self, Write},
    process::Command,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Local, SecondsFormat, Utc};
use clap::{ArgGroup, Args, Subcommand, ValueEnum};
use eventdbx::cloud::{
    CloudSessionStore, StoredCloudSession, decode_cloud_token, extract_email_from_claims,
};
use reqwest::{Client, Method, StatusCode};
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, Deserializer},
};
use validator::validate_email;

const CLOUD_TOP_UP_URL: &str = "https://pay.eventdbx.com/b/28EeV71dw7Y53pWcCxgYU00";
const DEFAULT_CLOUD_API_BASE: &str = "https://api.eventdbx.com";
const ENV_EVENTDBX_CLOUD_API_URL: &str = "EVENTDBX_CLOUD_API_URL";

const SUPPORTED_REGIONS: &[&str] = &[
    "us-central1",
    "us-east1",
    "us-west1",
    "europe-west1",
    "asia-southeast1",
];

#[derive(Subcommand)]
pub enum CloudCommands {
    /// Link or unlink this CLI to EventDBX Cloud
    Auth(CloudAuthCommand),
    /// View or manage your EventDBX Cloud balance
    Balance(CloudBalanceArgs),
    /// Provision a new tenant in EventDBX Cloud
    Create(CloudCreateArgs),
    /// Scale the plan or capacity for an existing tenant
    Scale(CloudScaleArgs),
    /// Issue or revoke tenant tokens
    Token(CloudTokenArgs),
    /// Schedule a tenant for retirement
    Retire(CloudRetireArgs),
    /// List the tenants linked to your account
    Status(CloudStatusArgs),
}

#[derive(Args, Debug)]
pub struct CloudAuthArgs {
    #[command(flatten)]
    pub api: CloudApiArgs,

    /// Email address used for your EventDBX Cloud account
    #[arg(long)]
    pub email: Option<String>,

    /// Explicitly confirm you accept the EventDBX Cloud terms of service (required for new accounts)
    #[arg(long = "accept")]
    pub accept_terms: bool,

    /// First name to use when creating a new EventDBX Cloud account
    #[arg(long = "first-name")]
    pub first_name: Option<String>,

    /// Last name to use when creating a new EventDBX Cloud account
    #[arg(long = "last-name")]
    pub last_name: Option<String>,
}

#[derive(Args)]
pub struct CloudBalanceArgs {
    /// Open the EventDBX Cloud checkout to add funds using your default browser
    #[arg(long = "top-up")]
    pub top_up: bool,

    /// Email address to lock in the EventDBX Cloud checkout (overrides the token email)
    #[arg(long, requires = "top_up")]
    pub email: Option<String>,
}

#[derive(Args, Debug)]
pub struct CloudAuthCommand {
    #[command(flatten)]
    pub login: CloudAuthArgs,

    #[command(subcommand)]
    pub action: Option<CloudAuthAction>,
}

#[derive(Subcommand, Debug)]
pub enum CloudAuthAction {
    /// Remove the local EventDBX Cloud session token
    Logout,
    /// Display details about the stored EventDBX Cloud session token
    Status,
}

#[derive(Args, Debug, Clone)]
pub struct CloudApiArgs {
    /// Override the EventDBX Cloud API base URL (defaults to EVENTDBX_CLOUD_API_URL or the public endpoint)
    #[arg(long = "api-url")]
    pub api_url: Option<String>,
}

#[derive(Args, Debug)]
pub struct CloudCreateArgs {
    #[command(flatten)]
    pub api: CloudApiArgs,

    /// DNS-style tenant identifier to create
    #[arg(value_name = "tenant")]
    pub tenant: String,

    /// Region where the tenant should be provisioned
    #[arg(long, value_name = "region")]
    pub region: String,

    /// Initial plan tier for the tenant
    #[arg(long, value_enum, default_value_t = PlanTierArg::Free)]
    pub plan: PlanTierArg,
}

#[derive(Args, Debug)]
pub struct CloudScaleArgs {
    #[command(flatten)]
    pub api: CloudApiArgs,

    /// Tenant identifier to scale
    #[arg(value_name = "tenant")]
    pub tenant: String,

    /// Desired plan tier
    #[arg(long, value_enum)]
    pub plan: PlanTierArg,

    /// Disk allocation (GB). Required for flex/flow plans.
    #[arg(long = "disk-gb")]
    pub disk_gb: Option<u32>,

    /// Virtual CPUs for dedicated plans.
    #[arg(long = "vcpu")]
    pub vcpu: Option<u32>,

    /// Memory in GB for dedicated plans.
    #[arg(long = "ram-gb")]
    pub ram_gb: Option<u32>,
}

#[derive(Args, Debug)]
#[command(group(
    ArgGroup::new("token_mode")
        .required(true)
        .multiple(false)
        .args(&["generate", "revoke"])
))]
pub struct CloudTokenArgs {
    #[command(flatten)]
    pub api: CloudApiArgs,

    /// Tenant identifier to scope the token
    #[arg(value_name = "tenant")]
    pub tenant: String,

    /// Issue a fresh tenant token (default).
    #[arg(long)]
    pub generate: bool,

    /// Revoke previously issued tenant token(s).
    #[arg(long)]
    pub revoke: bool,

    /// Override default token actions (comma separated)
    #[arg(long, value_delimiter = ',')]
    pub actions: Option<Vec<String>>,

    /// Override default token resources (comma separated)
    #[arg(long, value_delimiter = ',')]
    pub resources: Option<Vec<String>>,

    /// Explicit TTL for generated tokens
    #[arg(long = "ttl-secs")]
    pub ttl_secs: Option<u64>,
}

#[derive(Args, Debug)]
pub struct CloudRetireArgs {
    #[command(flatten)]
    pub api: CloudApiArgs,

    /// Tenant identifier to retire
    #[arg(value_name = "tenant")]
    pub tenant: String,

    /// Number of days from today before retirement is executed
    #[arg(long = "in-days", default_value_t = 30)]
    pub in_days: u32,
}

#[derive(Args, Debug)]
pub struct CloudStatusArgs {
    #[command(flatten)]
    pub api: CloudApiArgs,

    /// Explicit user identifier whose tenants should be listed
    #[arg(long = "user")]
    pub user_id: Option<String>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum PlanTierArg {
    Free,
    #[value(alias = "flow")]
    Flex,
    Dedicated,
}

impl PlanTierArg {
    fn api_value(self) -> &'static str {
        match self {
            PlanTierArg::Free => "free",
            PlanTierArg::Flex => "flex",
            PlanTierArg::Dedicated => "dedicated",
        }
    }
}

#[derive(Debug, Serialize)]
struct ApiCreateTenantRequest {
    tenant_id: String,
    region: String,
    plan: String,
}

#[derive(Debug, Deserialize)]
struct ApiCreateTenantResponse {
    tenant_id: String,
    region: String,
    plan: String,
    status: String,
    tenants: Vec<String>,
    token: ApiTenantTokenResponse,
}

#[derive(Debug, Serialize)]
struct ApiScaleTenantRequest {
    plan: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiScaleTenantResponse {
    tenant_id: String,
    plan: String,
    #[serde(default)]
    size: Option<String>,
    checkout_url: String,
}

#[derive(Debug, Serialize)]
struct ApiGenerateTenantTokenRequest {
    tenant_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    actions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ApiRevokeTenantTokenRequest {
    tenant_id: String,
}

#[derive(Debug, Serialize, Default)]
struct ApiGetTenantsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApiRetireTenantRequest {
    in_days: u32,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ApiTenantTokenResponse {
    value: String,
    jti: String,
    subject: String,
    group: String,
    user: String,
    region: String,
    status: String,
    issued_by: String,
    issued_at: String,
    expires_at: String,
    actions: Vec<String>,
    resources: Vec<String>,
    tenants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ApiGetTenantsResponse {
    user_id: String,
    tenants: Vec<ApiTenantSummary>,
}

#[derive(Debug, Deserialize)]
struct ApiTenantSummary {
    tenant_id: String,
    plan: String,
    status: String,
    region: String,
    usage_percent: u8,
    #[serde(default)]
    expires_at: Option<String>,
    #[serde(default)]
    vcpu: f32,
    #[serde(default)]
    memory_gb: f32,
    #[serde(default)]
    disk_gb: f32,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    created_by: Option<String>,
}

struct CloudApiClient {
    http: Client,
    base_url: String,
    token: String,
}

impl CloudApiClient {
    fn new(base_url: String, token: String) -> Result<Self> {
        let http = Client::builder()
            .user_agent(format!("eventdbx-cli/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to construct EventDBX Cloud HTTP client")?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
        })
    }

    async fn create_tenant(
        &self,
        payload: ApiCreateTenantRequest,
    ) -> Result<ApiCreateTenantResponse> {
        self.post_json("/v1/tenants", &payload, "create tenant")
            .await
    }

    async fn scale_tenant(
        &self,
        tenant_id: &str,
        payload: ApiScaleTenantRequest,
    ) -> Result<ApiScaleTenantResponse> {
        let path = format!("/v1/tenants/{tenant_id}/upgrade");
        self.post_json(&path, &payload, "scale tenant").await
    }

    async fn generate_token(
        &self,
        payload: ApiGenerateTenantTokenRequest,
    ) -> Result<ApiTenantTokenResponse> {
        self.post_json("/v1/tokens", &payload, "generate tenant token")
            .await
    }

    async fn revoke_token(&self, payload: ApiRevokeTenantTokenRequest) -> Result<()> {
        self.post_unit("/v1/tokens/revoke", &payload, "revoke tenant tokens")
            .await
    }

    async fn retire_tenant(&self, tenant_id: &str, in_days: u32) -> Result<()> {
        let path = format!("/v1/tenants/{tenant_id}/retire");
        let payload = ApiRetireTenantRequest { in_days };
        self.post_unit(&path, &payload, "schedule tenant retirement")
            .await
    }

    async fn list_tenants(&self, query: ApiGetTenantsQuery) -> Result<ApiGetTenantsResponse> {
        self.get_json("/v1/tenants", &query, "list tenants").await
    }

    async fn get_json<Q: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        path: &str,
        query: &Q,
        context: &str,
    ) -> Result<R> {
        let url = self.endpoint(path);
        let request = self
            .http
            .request(Method::GET, url)
            .bearer_auth(&self.token)
            .query(query);
        let response = self.execute(request, context).await?;
        response
            .json::<R>()
            .await
            .with_context(|| format!("failed to {context}: could not decode response body"))
    }

    async fn post_json<B: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        context: &str,
    ) -> Result<R> {
        let url = self.endpoint(path);
        let request = self
            .http
            .request(Method::POST, url)
            .bearer_auth(&self.token)
            .json(body);
        let response = self.execute(request, context).await?;
        response
            .json::<R>()
            .await
            .with_context(|| format!("failed to {context}: could not decode response body"))
    }

    async fn post_unit<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
        context: &str,
    ) -> Result<()> {
        let url = self.endpoint(path);
        let request = self
            .http
            .request(Method::POST, url)
            .bearer_auth(&self.token)
            .json(body);
        self.execute(request, context).await.map(|_| ())
    }

    fn endpoint(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/').trim_start_matches('/')
        )
    }

    async fn execute(
        &self,
        request: reqwest::RequestBuilder,
        context: &str,
    ) -> Result<reqwest::Response> {
        let response = request
            .send()
            .await
            .with_context(|| format!("failed to {context}: request error"))?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let message = format_error_body(body);
            bail!(
                "failed to {context}: EventDBX Cloud API responded with {} – {}",
                status,
                message
            );
        }
    }
}

async fn build_api_client(args: &CloudApiArgs) -> Result<CloudApiClient> {
    let base_url = resolve_api_base(args.api_url.clone())?;
    let session_store = cloud_session_store()?;
    let mut session = session_store.read()?;
    align_session_api_base(&session_store, &mut session, &base_url)?;
    ensure_fresh_cloud_token(&session_store, &mut session, &base_url).await?;
    CloudApiClient::new(base_url, session.token.clone())
}

struct CloudAuthClient {
    http: Client,
    base_url: String,
}

impl CloudAuthClient {
    fn new(base_url: String) -> Result<Self> {
        let http = Client::builder()
            .user_agent(format!("eventdbx-cli/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to construct EventDBX Cloud HTTP client")?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    async fn start_email_sign_in(
        &self,
        email: &str,
    ) -> std::result::Result<ApiEmailAuthChallenge, StartSignInError> {
        let payload = ApiStartEmailSignInRequest {
            email: email.to_string(),
        };
        match self
            .post_json("/v1/auth/sign-in/email", &payload, "start sign-in")
            .await
        {
            Ok(response) => Ok(response),
            Err(ApiClientError::Response { status, .. }) if status == StatusCode::NOT_FOUND => {
                Err(StartSignInError::MissingIdentifier)
            }
            Err(err) => Err(StartSignInError::Other(err.into_anyhow())),
        }
    }

    async fn verify_sign_in_code(&self, challenge_id: &str, code: &str) -> Result<ApiAuthTokens> {
        let payload = ApiVerifyEmailCodeRequest {
            challenge_id: challenge_id.to_string(),
            code: code.to_string(),
        };
        self.post_json("/v1/auth/sign-in/email/verify", &payload, "verify sign-in")
            .await
            .map_err(|err| err.into_anyhow())
    }

    async fn start_email_sign_up(
        &self,
        email: &str,
        first_name: &str,
        last_name: &str,
        accept_terms: bool,
    ) -> Result<ApiEmailAuthChallenge> {
        let payload = ApiStartEmailSignUpRequest {
            email: email.to_string(),
            first_name: first_name.to_string(),
            last_name: last_name.to_string(),
            accept_terms,
        };
        self.post_json("/v1/auth/sign-up/email", &payload, "start sign-up")
            .await
            .map_err(|err| err.into_anyhow())
    }

    async fn verify_sign_up_code(&self, challenge_id: &str, code: &str) -> Result<ApiAuthTokens> {
        let payload = ApiVerifyEmailCodeRequest {
            challenge_id: challenge_id.to_string(),
            code: code.to_string(),
        };
        self.post_json("/v1/auth/sign-up/email/verify", &payload, "verify sign-up")
            .await
            .map_err(|err| err.into_anyhow())
    }

    async fn refresh_session(
        &self,
        refresh_token: &str,
    ) -> std::result::Result<ApiAuthTokens, ApiClientError> {
        let payload = ApiRefreshSessionRequest {
            refresh_token: refresh_token.to_string(),
        };
        self.post_json("/v1/auth/token/refresh", &payload, "refresh cloud token")
            .await
    }

    fn endpoint(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/').trim_start_matches('/')
        )
    }

    async fn post_json<B: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        context: &str,
    ) -> Result<R, ApiClientError> {
        let url = self.endpoint(path);
        let request = self.http.request(Method::POST, url).json(body);
        let response = request
            .send()
            .await
            .map_err(|err| ApiClientError::Transport(err.into()))?;
        if response.status().is_success() {
            response.json::<R>().await.map_err(|err| {
                ApiClientError::Transport(anyhow!(err).context(format!(
                    "failed to {context}: could not decode response body"
                )))
            })
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let message = format!(
                "failed to {context}: EventDBX Cloud API responded with {} – {}",
                status,
                format_error_body(body)
            );
            Err(ApiClientError::Response { status, message })
        }
    }
}

#[derive(Debug, Deserialize)]
struct ApiEmailAuthChallenge {
    status: ApiEmailAuthStatus,
    #[serde(default)]
    challenge_id: Option<String>,
    #[serde(default)]
    session: Option<ApiAuthTokens>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiAuthTokens {
    #[serde(rename = "access_token")]
    access_token: String,
    #[serde(rename = "refresh_token", default)]
    refresh_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ApiEmailAuthStatus {
    PendingVerification,
    Complete,
    Unknown(String),
}

impl<'de> Deserialize<'de> for ApiEmailAuthStatus {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let status = match value.as_str() {
            "pending_verification" => Self::PendingVerification,
            "complete" => Self::Complete,
            other => Self::Unknown(other.to_string()),
        };
        Ok(status)
    }
}

#[derive(Debug, Serialize)]
struct ApiStartEmailSignInRequest {
    email: String,
}

#[derive(Debug, Serialize)]
struct ApiStartEmailSignUpRequest {
    email: String,
    first_name: String,
    last_name: String,
    accept_terms: bool,
}

#[derive(Debug, Serialize)]
struct ApiVerifyEmailCodeRequest {
    challenge_id: String,
    code: String,
}

#[derive(Debug, Serialize)]
struct ApiRefreshSessionRequest {
    refresh_token: String,
}

#[derive(Debug)]
enum ApiClientError {
    Transport(anyhow::Error),
    Response { status: StatusCode, message: String },
}

impl std::fmt::Display for ApiClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(err) => write!(f, "{err}"),
            Self::Response { message, .. } => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ApiClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(err) => err.source(),
            Self::Response { .. } => None,
        }
    }
}

impl ApiClientError {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            ApiClientError::Transport(err) => err,
            ApiClientError::Response { message, .. } => anyhow!(message),
        }
    }
}

fn resolve_api_base(override_value: Option<String>) -> Result<String> {
    if let Some(value) = override_value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return normalize_api_base(value);
    }

    if let Ok(from_env) = env::var(ENV_EVENTDBX_CLOUD_API_URL) {
        let trimmed = from_env.trim();
        if !trimmed.is_empty() {
            return normalize_api_base(trimmed);
        }
    }

    normalize_api_base(DEFAULT_CLOUD_API_BASE)
}

fn normalize_api_base(value: &str) -> Result<String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        bail!("EventDBX Cloud API base URL cannot be empty");
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("https://{trimmed}"))
    }
}

fn format_error_body(body: String) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        "<empty body>".to_string()
    } else if trimmed.len() > 512 {
        format!("{}…", &trimmed[..512])
    } else {
        trimmed.to_string()
    }
}

fn validate_tenant_id(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("tenant identifier must not be empty");
    }
    Ok(trimmed.to_string())
}

fn format_jwt_timestamp(epoch_secs: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(epoch_secs, 0).map(|dt| {
        let local = dt.with_timezone(&Local);
        local.to_rfc3339_opts(SecondsFormat::Secs, false)
    })
}

fn normalize_region_choice(value: &str) -> Result<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        bail!("--region must not be empty");
    }
    if SUPPORTED_REGIONS
        .iter()
        .any(|candidate| candidate == &normalized)
    {
        Ok(normalized)
    } else {
        bail!(
            "unsupported region '{}'. supported regions: {}",
            value,
            SUPPORTED_REGIONS.join(", ")
        );
    }
}

fn sanitize_string_list(values: Option<Vec<String>>) -> Option<Vec<String>> {
    let cleaned = values?
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn cloud_session_store() -> Result<CloudSessionStore> {
    CloudSessionStore::new_default()
}

fn build_size_descriptor(args: &CloudScaleArgs) -> Result<Option<String>> {
    match args.plan {
        PlanTierArg::Free => {
            if args.disk_gb.is_some() || args.vcpu.is_some() || args.ram_gb.is_some() {
                bail!("resource overrides are not supported on the free plan");
            }
            Ok(None)
        }
        PlanTierArg::Flex => {
            if args.vcpu.is_some() || args.ram_gb.is_some() {
                bail!("--vcpu/--ram-gb are only valid for dedicated plans");
            }
            let disk = args
                .disk_gb
                .ok_or_else(|| anyhow!("--disk-gb is required for flex/flow plans"))?;
            let allowed = [1, 2, 4, 8, 16];
            if !allowed.contains(&disk) {
                bail!(
                    "--disk-gb must be one of {} for flex/flow plans",
                    allowed
                        .iter()
                        .map(|value| format!("{value}GB"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            Ok(Some(format!("{disk} GB disk")))
        }
        PlanTierArg::Dedicated => {
            let disk = args
                .disk_gb
                .ok_or_else(|| anyhow!("--disk-gb is required for dedicated plans"))?;
            if !(10..=100).contains(&disk) {
                bail!("--disk-gb must be between 10 and 100 for dedicated plans");
            }
            let vcpu = args
                .vcpu
                .ok_or_else(|| anyhow!("--vcpu is required for dedicated plans"))?;
            if !(1..=32).contains(&vcpu) {
                bail!("--vcpu must be between 1 and 32 for dedicated plans");
            }
            let ram = args
                .ram_gb
                .ok_or_else(|| anyhow!("--ram-gb is required for dedicated plans"))?;
            if !(1..=32).contains(&ram) {
                bail!("--ram-gb must be between 1 and 32 for dedicated plans");
            }
            Ok(Some(format!("{vcpu} vCPU / {ram} GB RAM / {disk} GB disk")))
        }
    }
}

pub async fn execute(command: CloudCommands) -> Result<()> {
    match command {
        CloudCommands::Auth(command) => match command.action {
            Some(CloudAuthAction::Logout) => auth_logout(),
            Some(CloudAuthAction::Status) => auth_status().await,
            None => auth(command.login).await,
        },
        CloudCommands::Balance(args) => balance(args),
        CloudCommands::Create(args) => create_tenant_command(args).await,
        CloudCommands::Scale(args) => scale_tenant_command(args).await,
        CloudCommands::Token(args) => token_command(args).await,
        CloudCommands::Retire(args) => retire_tenant_command(args).await,
        CloudCommands::Status(args) => status_command(args).await,
    }
}

async fn create_tenant_command(args: CloudCreateArgs) -> Result<()> {
    let tenant_id = validate_tenant_id(&args.tenant)?;
    let region = normalize_region_choice(&args.region)?;
    let client = build_api_client(&args.api).await?;
    let payload = ApiCreateTenantRequest {
        tenant_id: tenant_id.clone(),
        region,
        plan: args.plan.api_value().to_string(),
    };
    let response = client.create_tenant(payload).await?;

    println!(
        "Tenant '{}' queued for provisioning (plan: {}, region: {}).",
        response.tenant_id, response.plan, response.region
    );
    println!("Status: {}", response.status);
    if !response.tenants.is_empty() {
        println!("Membership snapshot: {}", response.tenants.join(", "));
    }
    println!(
        "Bootstrap token (expires {}):\n{}",
        response.token.expires_at, response.token.value
    );

    Ok(())
}

async fn scale_tenant_command(args: CloudScaleArgs) -> Result<()> {
    let tenant_id = validate_tenant_id(&args.tenant)?;
    let client = build_api_client(&args.api).await?;
    let size = build_size_descriptor(&args)?;
    let payload = ApiScaleTenantRequest {
        plan: args.plan.api_value().to_string(),
        size,
    };
    let response = client.scale_tenant(&tenant_id, payload).await?;

    println!(
        "Tenant '{}' scaled to {} plan.",
        response.tenant_id, response.plan
    );
    if let Some(size) = response.size.as_deref() {
        println!("Requested capacity: {size}");
    }
    println!("Checkout URL: {}", response.checkout_url);

    Ok(())
}

async fn token_command(args: CloudTokenArgs) -> Result<()> {
    let tenant_id = validate_tenant_id(&args.tenant)?;
    let client = build_api_client(&args.api).await?;

    if args.revoke {
        client
            .revoke_token(ApiRevokeTenantTokenRequest {
                tenant_id: tenant_id.clone(),
            })
            .await?;
        println!("Requested token revocation for tenant '{tenant_id}'.");
        return Ok(());
    }

    let payload = ApiGenerateTenantTokenRequest {
        tenant_id: tenant_id.clone(),
        actions: sanitize_string_list(args.actions),
        resources: sanitize_string_list(args.resources),
        ttl_secs: args.ttl_secs,
    };
    let token = client.generate_token(payload).await?;

    println!(
        "Issued tenant token for '{}' (expires {}).",
        tenant_id, token.expires_at
    );
    println!("{}", token.value);

    Ok(())
}

async fn retire_tenant_command(args: CloudRetireArgs) -> Result<()> {
    if args.in_days == 0 {
        bail!("--in-days must be at least 1");
    }
    let tenant_id = validate_tenant_id(&args.tenant)?;
    let client = build_api_client(&args.api).await?;
    client.retire_tenant(&tenant_id, args.in_days).await?;
    println!(
        "Scheduled tenant '{}' for retirement in {} day(s).",
        tenant_id, args.in_days
    );
    Ok(())
}

async fn status_command(args: CloudStatusArgs) -> Result<()> {
    let client = build_api_client(&args.api).await?;
    let query = ApiGetTenantsQuery {
        user_id: args
            .user_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string()),
    };
    let snapshot = client.list_tenants(query).await?;

    if snapshot.tenants.is_empty() {
        println!("No tenants found for {}.", snapshot.user_id);
        return Ok(());
    }

    println!("Tenants for {}:", snapshot.user_id);
    for tenant in snapshot.tenants {
        println!(
            "- {} ({}, status: {}, region: {})",
            tenant.tenant_id, tenant.plan, tenant.status, tenant.region
        );
        println!(
            "    usage: {}% | resources: {:.1} vCPU / {:.1} GB RAM / {:.1} GB disk",
            tenant.usage_percent, tenant.vcpu, tenant.memory_gb, tenant.disk_gb
        );
        if let Some(expires) = tenant
            .expires_at
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            println!("    expires at: {}", expires);
        }
        if let Some(created_at) = tenant
            .created_at
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let created_by = tenant
                .created_by
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("system");
            println!("    created {} by {}", created_at, created_by);
        }
    }

    Ok(())
}

enum StartSignInError {
    MissingIdentifier,
    Other(anyhow::Error),
}

fn resolve_auth_email(store: &CloudSessionStore, args: &CloudAuthArgs) -> Result<String> {
    let stored_email = store.last_known_email()?;
    choose_auth_email(args.email.as_deref(), stored_email.as_deref())
}

fn choose_auth_email(provided: Option<&str>, stored: Option<&str>) -> Result<String> {
    if let Some(value) = provided {
        return normalize_email(value);
    }
    if let Some(value) = stored {
        return normalize_email(value);
    }
    bail!(
        "--email must be provided the first time you run 'dbx cloud auth'. Re-run with '--email <address>' to link the CLI."
    );
}

fn normalize_email(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || !validate_email(trimmed) {
        bail!("--email must be a valid email address");
    }
    Ok(trimmed.to_string())
}

async fn auth(args: CloudAuthArgs) -> Result<()> {
    let session_store = cloud_session_store()?;
    let email = resolve_auth_email(&session_store, &args)?;
    let api_base = resolve_api_base(args.api.api_url.clone())?;
    let client = CloudAuthClient::new(api_base.clone())?;
    let tokens = match client.start_email_sign_in(&email).await {
        Ok(challenge) => {
            complete_email_challenge(&client, &email, challenge, EmailChallengeKind::SignIn).await?
        }
        Err(StartSignInError::MissingIdentifier) => {
            if args.accept_terms {
                println!(
                    "No existing EventDBX Cloud account found for {email}. Creating one now..."
                );
                let (first_name, last_name) = registration_names(&args)?;
                run_sign_up_flow(&client, &email, args.accept_terms, &first_name, &last_name)
                    .await?
            } else {
                bail!(
                    "No EventDBX Cloud account found for {email}. Re-run with --accept to create one."
                );
            }
        }
        Err(StartSignInError::Other(err)) => return Err(err),
    };
    let stored = StoredCloudSession {
        token: tokens.access_token.clone(),
        refresh_token: tokens.refresh_token,
        api_base: Some(api_base),
    };
    let token_path = session_store.persist(&stored)?;
    println!(
        "Linked CLI to EventDBX Cloud. Token stored at {}",
        token_path.display()
    );

    Ok(())
}

async fn complete_email_challenge(
    client: &CloudAuthClient,
    email: &str,
    challenge: ApiEmailAuthChallenge,
    kind: EmailChallengeKind,
) -> Result<ApiAuthTokens> {
    match challenge.status {
        ApiEmailAuthStatus::Complete => challenge
            .session
            .ok_or_else(|| anyhow!("{} flow did not return a session token", kind.context())),
        ApiEmailAuthStatus::PendingVerification => {
            let challenge_id = challenge.challenge_id.ok_or_else(|| {
                anyhow!(
                    "{} flow did not include a challenge identifier",
                    kind.context()
                )
            })?;
            println!("Verification code sent to {email}. Check your inbox.");
            let code = prompt_for_code()?;
            match kind {
                EmailChallengeKind::SignIn => {
                    client.verify_sign_in_code(&challenge_id, &code).await
                }
                EmailChallengeKind::SignUp => {
                    client.verify_sign_up_code(&challenge_id, &code).await
                }
            }
        }
        ApiEmailAuthStatus::Unknown(status) => {
            bail!(
                "{} flow returned unsupported status '{status}'",
                kind.context()
            );
        }
    }
}

async fn run_sign_up_flow(
    client: &CloudAuthClient,
    email: &str,
    accept_terms: bool,
    first_name: &str,
    last_name: &str,
) -> Result<ApiAuthTokens> {
    if !accept_terms {
        bail!("--accept is required when creating a new EventDBX Cloud account");
    }
    let challenge = client
        .start_email_sign_up(email, first_name, last_name, accept_terms)
        .await?;
    complete_email_challenge(client, email, challenge, EmailChallengeKind::SignUp).await
}

enum EmailChallengeKind {
    SignIn,
    SignUp,
}

impl EmailChallengeKind {
    fn context(&self) -> &'static str {
        match self {
            Self::SignIn => "sign-in",
            Self::SignUp => "sign-up",
        }
    }
}

fn balance(args: CloudBalanceArgs) -> Result<()> {
    if args.top_up {
        let locked_prefill_email = parse_locked_prefill_email(args.email.as_deref())?;
        open_top_up_checkout(locked_prefill_email.as_deref())
    } else {
        println!("Use --top-up to open the EventDBX Cloud checkout and add funds to your balance.");
        Ok(())
    }
}

fn auth_logout() -> Result<()> {
    cloud_session_store()?.logout()?;
    println!("Logged out of EventDBX Cloud.");
    Ok(())
}

async fn auth_status() -> Result<()> {
    let session_store = cloud_session_store()?;
    let mut session = session_store.read()?;
    let api_base = session_api_base(&session)?;
    ensure_fresh_cloud_token(&session_store, &mut session, &api_base).await?;
    let claims = decode_cloud_token(&session.token)?;
    let subject = claims.subject()?;
    println!("Logged in as: {}", subject);
    if let Some(email) = extract_email_from_claims(&claims) {
        println!("Email: {}", email);
    }
    match claims.expires_at_epoch().and_then(format_jwt_timestamp) {
        Some(expiry) => println!("Token expires at: {}", expiry),
        None => println!("Token expiration: unavailable"),
    }
    Ok(())
}

fn parse_locked_prefill_email(value: Option<&str>) -> Result<Option<String>> {
    match value {
        Some(input) => {
            let trimmed = input.trim();
            if trimmed.is_empty() {
                bail!("--email cannot be empty when provided");
            }
            if !validate_email(trimmed) {
                bail!("--email must be a valid email address");
            }
            Ok(Some(trimmed.to_string()))
        }
        None => Ok(None),
    }
}

fn open_top_up_checkout(locked_prefill_email: Option<&str>) -> Result<()> {
    let session_store = cloud_session_store()?;
    let session = session_store.read()?;
    let claims = decode_cloud_token(&session.token)?;
    let subject = claims.subject()?;
    let mut url = format!(
        "{CLOUD_TOP_UP_URL}?client_reference_id={}",
        percent_encode(subject)
    );
    if let Some(email) = locked_prefill_email.or_else(|| extract_email_from_claims(&claims)) {
        url.push_str("&locked_prefilled_email=");
        url.push_str(&percent_encode(email));
    }
    println!("Opening EventDBX Cloud checkout in your default browser...");
    match open_in_browser(&url) {
        Ok(()) => {
            println!("If the browser does not open automatically, open this link manually:\n{url}");
            Ok(())
        }
        Err(err) => {
            println!("Open this link manually to top up your balance:\n{url}");
            Err(err)
        }
    }
}

fn session_api_base(session: &StoredCloudSession) -> Result<String> {
    if let Some(value) = session
        .api_base
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Ok(value.to_string())
    } else {
        resolve_api_base(None)
    }
}

fn align_session_api_base(
    store: &CloudSessionStore,
    session: &mut StoredCloudSession,
    desired_base: &str,
) -> Result<()> {
    if let Some(current) = session
        .api_base
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if current != desired_base {
            bail!(
                "Stored EventDBX Cloud session targets {current} but this command targets {desired_base}. Re-run 'dbx cloud auth --api-url {desired_base}' to switch contexts."
            );
        }
        Ok(())
    } else {
        session.api_base = Some(desired_base.to_string());
        store.persist(session)?;
        Ok(())
    }
}

fn prompt_for_code() -> Result<String> {
    print!("Enter verification code: ");
    io::stdout().flush().ok();
    let mut code = String::new();
    io::stdin()
        .read_line(&mut code)
        .context("failed to read verification code")?;
    let trimmed = code.trim();
    if trimmed.is_empty() {
        bail!("verification code cannot be empty");
    }
    Ok(trimmed.to_string())
}

async fn ensure_fresh_cloud_token(
    store: &CloudSessionStore,
    session: &mut StoredCloudSession,
    api_base: &str,
) -> Result<()> {
    if !token_needs_refresh(&session.token) {
        return Ok(());
    }

    let refresh_token = session
        .refresh_token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "Stored EventDBX Cloud session is missing its refresh token; run 'dbx cloud auth --email <email>' again."
            )
        })?;
    let client = CloudAuthClient::new(api_base.to_string())?;
    match client.refresh_session(refresh_token).await {
        Ok(tokens) => {
            session.token = tokens.access_token;
            session.refresh_token = tokens.refresh_token;
            store.persist(session)?;
            Ok(())
        }
        Err(ApiClientError::Response { .. }) => {
            bail!(
                "Stored EventDBX Cloud session has expired. Re-run 'dbx cloud auth --email <email>' to link the CLI again."
            );
        }
        Err(ApiClientError::Transport(err)) => Err(err),
    }
}

fn token_needs_refresh(token: &str) -> bool {
    match decode_cloud_token(token) {
        Ok(claims) => claims
            .expires_at_epoch()
            .map(|exp| exp <= Utc::now().timestamp() + 30)
            .unwrap_or(false),
        Err(_) => true,
    }
}

fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => {
                encoded.push('%');
                encoded.push_str(&format!("{byte:02X}"));
            }
        }
    }
    encoded
}

#[cfg(target_os = "windows")]
fn open_in_browser(url: &str) -> Result<()> {
    run_browser_command(
        "cmd",
        &["/C", "start", "", url],
        "launch the default browser via 'start'",
    )
}

#[cfg(target_os = "macos")]
fn open_in_browser(url: &str) -> Result<()> {
    run_browser_command("open", &[url], "launch the default browser via 'open'")
}

#[cfg(all(target_family = "unix", not(target_os = "macos")))]
fn open_in_browser(url: &str) -> Result<()> {
    run_browser_command(
        "xdg-open",
        &[url],
        "launch the default browser via 'xdg-open'",
    )
}

#[cfg(not(any(target_os = "windows", target_family = "unix")))]
fn open_in_browser(_url: &str) -> Result<()> {
    bail!("opening a browser is not supported on this platform");
}

#[cfg(any(target_os = "windows", target_family = "unix"))]
fn run_browser_command(command: &str, args: &[&str], context: &str) -> Result<()> {
    let status = Command::new(command)
        .args(args)
        .status()
        .with_context(|| format!("failed to {context}"))?;
    if status.success() {
        Ok(())
    } else {
        bail!("{context} exited with status {status}");
    }
}

fn registration_names(args: &CloudAuthArgs) -> Result<(String, String)> {
    let first_name = required_name(args.first_name.as_deref(), "--first-name")?;
    let last_name = required_name(args.last_name.as_deref(), "--last-name")?;
    Ok((first_name, last_name))
}

fn required_name(value: Option<&str>, flag: &str) -> Result<String> {
    let trimmed = value
        .map(|input| input.trim())
        .filter(|input| !input.is_empty())
        .ok_or_else(|| anyhow!("{flag} is required when creating a new EventDBX Cloud account"))?;
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_api_base_with_scheme() {
        assert_eq!(
            normalize_api_base("https://example.com/foo").unwrap(),
            "https://example.com/foo"
        );
        assert_eq!(
            normalize_api_base("example.com/").unwrap(),
            "https://example.com"
        );
    }

    #[test]
    fn percent_encode_handles_reserved_characters() {
        assert_eq!(percent_encode("abc123-_.~"), "abc123-_.~");
        assert_eq!(percent_encode("user@example.com"), "user%40example.com");
    }

    #[test]
    fn region_normalization_accepts_known_values() {
        assert_eq!(
            normalize_region_choice("US-WEST1").unwrap(),
            "us-west1".to_string()
        );
    }

    #[test]
    fn region_normalization_rejects_unknown_values() {
        assert!(normalize_region_choice("antarctica-1").is_err());
    }

    #[test]
    fn sanitize_string_list_filters_empty_entries() {
        let cleaned = sanitize_string_list(Some(vec![
            "read.*".into(),
            "".into(),
            "  ".into(),
            "write.all".into(),
        ]))
        .unwrap();
        assert_eq!(cleaned, vec!["read.*", "write.all"]);
    }

    #[test]
    fn flex_plan_requires_supported_disk_sizes() {
        let mut args = CloudScaleArgs {
            api: CloudApiArgs { api_url: None },
            tenant: "tenant".into(),
            plan: PlanTierArg::Flex,
            disk_gb: Some(4),
            vcpu: None,
            ram_gb: None,
        };
        assert!(build_size_descriptor(&args).is_ok());
        args.disk_gb = Some(3);
        assert!(build_size_descriptor(&args).is_err());
    }

    #[test]
    fn dedicated_plan_rejects_missing_resources() {
        let args = CloudScaleArgs {
            api: CloudApiArgs { api_url: None },
            tenant: "tenant".into(),
            plan: PlanTierArg::Dedicated,
            disk_gb: Some(20),
            vcpu: Some(4),
            ram_gb: None,
        };
        assert!(build_size_descriptor(&args).is_err());
    }

    #[test]
    fn formats_jwt_timestamp_produces_rfc3339() {
        let ts = 1_700_000_000;
        let formatted = format_jwt_timestamp(ts).expect("timestamp should format");
        assert!(
            chrono::DateTime::parse_from_rfc3339(&formatted).is_ok(),
            "formatted timestamp should be RFC3339"
        );
    }

    #[test]
    fn choose_auth_email_prefers_cli_argument() {
        let result = choose_auth_email(Some(" user@example.com "), Some("stored@example.com"))
            .expect("email should resolve");
        assert_eq!(result, "user@example.com");
    }

    #[test]
    fn choose_auth_email_falls_back_to_stored_value() {
        let result = choose_auth_email(None, Some("stored@example.com"))
            .expect("stored email should be used");
        assert_eq!(result, "stored@example.com");
    }

    #[test]
    fn choose_auth_email_errors_when_missing() {
        assert!(choose_auth_email(None, None).is_err());
    }
}
