use std::{
    env, fs,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Subcommand};
use clerk_fapi_rs::{
    apis,
    clerk_fapi::ClerkFapiClient,
    clerk_state::ClerkState,
    configuration::ClerkFapiConfiguration,
    models::{
        ClerkErrors, ClientSignIn, ClientSignUp, StubsSignInFactor, StubsSignUpVerification,
        client_sign_in::Status as SignInStatus, client_sign_up::Status as SignUpStatus,
        stubs_sign_in_factor::Strategy as SignInStrategy,
        stubs_sign_up_verification::NextAction as SignUpNextAction,
    },
};
use dirs::home_dir;
use parking_lot::RwLock;
use serde_json;
use tracing::debug;
use validator::validate_email;

const DEFAULT_FRONTEND_API: &str = "https://clerk.eventdbx.com";
const DEFAULT_PUBLISHABLE_KEY: &str = "pk_live_Y2xlcmsuZXZlbnRkYnguY29tJA";
const ENV_EVENTDBX_CLOUD_FRONTEND_API: &str = "EVENTDBX_CLOUD_FRONTEND_API";
const ENV_CLERK_FRONTEND_API: &str = "CLERK_FRONTEND_API";
const ENV_EVENTDBX_CLOUD_PUBLISHABLE_KEY: &str = "EVENTDBX_CLOUD_PUBLISHABLE_KEY";
const ENV_CLERK_PUBLISHABLE_KEY: &str = "CLERK_PUBLISHABLE_KEY";

#[derive(Subcommand)]
pub enum CloudCommands {
    /// Link or unlink this CLI to EventDBX Cloud
    Auth(CloudAuthArgs),
}

#[derive(Args)]
pub struct CloudAuthArgs {
    /// Email address used for your EventDBX Cloud account
    #[arg(long, required_unless_present = "logout")]
    pub email: Option<String>,

    /// Override the Clerk Frontend API base URL (defaults to EVENTDBX_CLOUD_FRONTEND_API)
    #[arg(long)]
    pub frontend_api: Option<String>,

    /// Override the Clerk publishable key (defaults to built-in EventDBX key or EVENTDBX_CLOUD_PUBLISHABLE_KEY)
    #[arg(long)]
    pub publishable_key: Option<String>,

    /// Explicitly confirm you accept the EventDBX Cloud terms of service (required for new accounts)
    #[arg(long = "accept")]
    pub accept_terms: bool,

    /// First name to use when creating a new EventDBX Cloud account
    #[arg(long = "first-name")]
    pub first_name: Option<String>,

    /// Last name to use when creating a new EventDBX Cloud account
    #[arg(long = "last-name")]
    pub last_name: Option<String>,

    /// Remove the local EventDBX Cloud session token
    #[arg(long)]
    pub logout: bool,
}

pub async fn execute(command: CloudCommands) -> Result<()> {
    match command {
        CloudCommands::Auth(args) => auth(args).await,
    }
}

enum StartSignInError {
    MissingIdentifier,
    Other(anyhow::Error),
}

async fn auth(args: CloudAuthArgs) -> Result<()> {
    if args.logout {
        logout_cloud_session()?;
        println!("Logged out of EventDBX Cloud.");
        return Ok(());
    }
    let email = args.email.as_deref().unwrap_or("").trim().to_string();
    if email.is_empty() || !validate_email(&email) {
        bail!("--email must be a valid email address");
    }

    let frontend_api = resolve_frontend_api(args.frontend_api.clone())?;
    let publishable_key = resolve_publishable_key(args.publishable_key.clone())?;
    let client = ClerkClient::new(frontend_api, publishable_key.clone()).await?;

    let token = match client.start_email_sign_in(&email).await {
        Ok(sign_in) => match sign_in.status {
            SignInStatus::NeedsFirstFactor => {
                ensure_email_code_supported(sign_in.supported_first_factors.as_deref(), "sign-in")?;
                println!("Verification code sent to {email}. Check your inbox.");
                let code = prompt_for_code()?;
                let verified = client.verify_email_code(&sign_in.id, &code).await?;
                ensure_sign_in_complete(&verified)?;
                client
                    .session_token("sign-in", verified.created_session_id.as_deref())
                    .await?
            }
            SignInStatus::Complete => {
                client
                    .session_token("sign-in", sign_in.created_session_id.as_deref())
                    .await?
            }
            SignInStatus::NeedsSecondFactor => {
                bail!(
                    "Clerk requires an additional factor for this account; supported factors: {}",
                    format_sign_in_strategies(sign_in.supported_first_factors.as_deref())
                );
            }
            SignInStatus::NeedsIdentifier => {
                println!(
                    "No existing EventDBX Cloud account found for {email}. Creating one now..."
                );
                let (first_name, last_name) = registration_names(&args)?;
                run_sign_up_flow(&client, &email, args.accept_terms, &first_name, &last_name)
                    .await?
            }
            other => {
                bail!(
                    "Clerk returned unsupported status '{:?}' when starting sign-in",
                    other
                )
            }
        },
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

    let token_path = persist_token(&token)?;
    println!(
        "Linked CLI to EventDBX Cloud. Token stored at {}",
        token_path.display()
    );

    Ok(())
}

struct ClerkClient {
    api: ClerkFapiClient,
    state: Arc<RwLock<ClerkState>>,
}

impl ClerkClient {
    async fn new(frontend_api: String, publishable_key: String) -> Result<Self> {
        let config = ClerkFapiConfiguration::new(publishable_key.clone(), Some(frontend_api), None)
            .map_err(|err| anyhow!("failed to configure Clerk client: {err}"))?;
        let state = Arc::new(RwLock::new(ClerkState::new(
            config.clone(),
            |_client, _, _, _| {},
        )));
        state
            .write()
            .set_authorization_header(Some(format!("Bearer {publishable_key}")));
        let api = ClerkFapiClient::new(config, state.clone())
            .map_err(|err| anyhow!("failed to create Clerk client: {err}"))?;
        let client = Self { api, state };
        if let Err(err) = client.bootstrap().await {
            debug!("failed to initialize Clerk client state (continuing): {err}");
        }
        Ok(client)
    }

    async fn bootstrap(&self) -> Result<()> {
        self.api
            .post_client()
            .await
            .map_err(|err| anyhow!("failed to initialize Clerk client state: {err}"))?;
        Ok(())
    }

    async fn start_email_sign_in(&self, email: &str) -> Result<ClientSignIn, StartSignInError> {
        match self
            .api
            .create_sign_in(
                None,
                Some("email_code"),
                Some(email),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        {
            Ok(response) => Ok(response),
            Err(apis::Error::ResponseError(resp)) => {
                if is_form_identifier_not_found(resp.entity.as_ref(), &resp.content) {
                    return Err(StartSignInError::MissingIdentifier);
                }
                Err(StartSignInError::Other(map_response_error(
                    resp,
                    "start sign-in",
                )))
            }
            Err(other) => Err(StartSignInError::Other(map_api_error(
                other,
                "start sign-in",
            ))),
        }
    }

    async fn verify_email_code(&self, sign_in_id: &str, code: &str) -> Result<ClientSignIn> {
        self.api
            .attempt_sign_in_factor_one(
                sign_in_id,
                "email_code",
                None,
                Some(code),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .map_err(|err| map_api_error(err, "verify email code"))
    }

    async fn start_email_sign_up(
        &self,
        email: &str,
        accept_terms: bool,
        first_name: &str,
        last_name: &str,
    ) -> Result<ClientSignUp> {
        self.api
            .create_sign_ups(
                None, // origin
                None, // transfer
                None, // password
                Some(first_name),
                Some(last_name),
                None,               // username
                Some(email),        // email_address
                None,               // phone_number
                None,               // email_address_or_phone_number
                None,               // unsafe_metadata
                Some("email_code"), // strategy
                None,               // action_complete_redirect_url
                None,               // redirect_url
                None,               // ticket
                None,               // web3_wallet
                None,               // token
                None,               // code
                None,               // captcha_token
                None,               // captcha_error
                None,               // captcha_widget_type
                Some(accept_terms), // legal_accepted
                None,               // oidc_login_hint
                None,               // oidc_prompt
            )
            .await
            .map_err(|err| map_api_error(err, "start sign-up"))
    }

    async fn prepare_sign_up_email(&self, sign_up_id: &str) -> Result<ClientSignUp> {
        self.api
            .prepare_sign_ups_verification(
                sign_up_id,
                None,
                Some("email_code"),
                None,
                None,
                None,
                None,
            )
            .await
            .map_err(|err| map_api_error(err, "send verification email"))
    }

    async fn verify_sign_up_code(&self, sign_up_id: &str, code: &str) -> Result<ClientSignUp> {
        self.api
            .attempt_sign_ups_verification(
                sign_up_id,
                None,
                Some("email_code"),
                Some(code),
                None,
                None,
            )
            .await
            .map_err(|err| map_api_error(err, "verify sign-up code"))
    }

    async fn session_token(&self, context: &str, session_id: Option<&str>) -> Result<String> {
        if let Some(token) = self.read_authorization_token() {
            return Ok(token);
        }

        let session_id = session_id.ok_or_else(|| {
            anyhow!("Clerk {context} flow did not return a session token or session identifier")
        })?;

        let response = self
            .api
            .create_session_token(session_id, None)
            .await
            .map_err(|err| map_api_error(err, "create session token"))?;

        response.jwt.ok_or_else(|| {
            anyhow!(
                "Clerk {context} flow did not provide a session token even after requesting one"
            )
        })
    }

    fn read_authorization_token(&self) -> Option<String> {
        let mut guard = self.state.write();
        guard.authorization_header().and_then(|header| {
            header
                .strip_prefix("Bearer ")
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
    }
}

fn resolve_frontend_api(override_value: Option<String>) -> Result<String> {
    if let Some(value) = override_value.filter(|value| !value.trim().is_empty()) {
        return normalize_base_url(&value);
    }

    let from_env = env::var(ENV_EVENTDBX_CLOUD_FRONTEND_API)
        .or_else(|_| env::var(ENV_CLERK_FRONTEND_API))
        .ok();

    if let Some(value) = from_env.filter(|value| !value.trim().is_empty()) {
        return normalize_base_url(&value);
    }

    normalize_base_url(DEFAULT_FRONTEND_API)
}

fn resolve_publishable_key(override_value: Option<String>) -> Result<String> {
    if let Some(value) = override_value.filter(|value| !value.trim().is_empty()) {
        return Ok(value.trim().to_string());
    }

    let from_env = env::var(ENV_EVENTDBX_CLOUD_PUBLISHABLE_KEY)
        .or_else(|_| env::var(ENV_CLERK_PUBLISHABLE_KEY))
        .ok();

    if let Some(value) = from_env.filter(|value| !value.trim().is_empty()) {
        return Ok(value.trim().to_string());
    }

    Ok(DEFAULT_PUBLISHABLE_KEY.to_string())
}

fn normalize_base_url(value: &str) -> Result<String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        bail!("Clerk Frontend API base URL cannot be empty");
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("https://{trimmed}"))
    }
}

fn persist_token(token: &str) -> Result<PathBuf> {
    if token.trim().is_empty() {
        bail!("refusing to persist an empty token");
    }

    let token_path = cloud_token_path()?;
    if let Some(parent) = token_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&token_path, format!("{token}\n"))
        .with_context(|| format!("failed to write {}", token_path.display()))?;
    Ok(token_path)
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

fn cloud_token_path() -> Result<PathBuf> {
    let config_dir = home_dir()
        .map(|path| path.join(".eventdbx"))
        .context("failed to resolve home directory for ~/.eventdbx")?;
    Ok(config_dir.join("cloud.token"))
}

fn logout_cloud_session() -> Result<()> {
    let token_path = cloud_token_path()?;
    if token_path.exists() {
        fs::remove_file(&token_path)
            .with_context(|| format!("failed to remove {}", token_path.display()))?;
    }
    Ok(())
}

async fn run_sign_up_flow(
    client: &ClerkClient,
    email: &str,
    accept_terms: bool,
    first_name: &str,
    last_name: &str,
) -> Result<String> {
    if !accept_terms {
        bail!(
            "Clerk requires accepting the EventDBX Cloud terms of service. Re-run with --accept to proceed."
        );
    }

    let mut sign_up = client
        .start_email_sign_up(email, accept_terms, first_name, last_name)
        .await?;
    ensure_sign_up_ready(&sign_up)?;
    ensure_sign_up_email_supported(&sign_up)?;
    if needs_prepare(&sign_up) {
        sign_up = client.prepare_sign_up_email(&sign_up.id).await?;
    }
    println!("Verification code sent to {email}. Check your inbox.");
    let code = prompt_for_code()?;
    let verified = client.verify_sign_up_code(&sign_up.id, &code).await?;
    ensure_sign_up_complete(&verified)?;
    client
        .session_token("sign-up", verified.created_session_id.as_deref())
        .await
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

fn is_form_identifier_not_found(
    entity: Option<&apis::sign_ins_api::CreateSignInError>,
    body: &str,
) -> bool {
    if let Some(apis::sign_ins_api::CreateSignInError::Status422(errors)) = entity {
        if errors
            .errors
            .iter()
            .any(|error| error.code == "form_identifier_not_found")
        {
            return true;
        }
    }

    serde_json::from_str::<ClerkErrors>(body)
        .map(|payload| {
            payload
                .errors
                .iter()
                .any(|error| error.code == "form_identifier_not_found")
        })
        .unwrap_or(false)
}

fn ensure_email_code_supported(factors: Option<&[StubsSignInFactor]>, context: &str) -> Result<()> {
    let supported = factors.map(|items| {
        items
            .iter()
            .any(|factor| matches!(factor.strategy, SignInStrategy::EmailCode))
    });
    if supported.unwrap_or(false) {
        Ok(())
    } else {
        let strategies = format_sign_in_strategies(factors);
        bail!(
            "Clerk {context} flow is not configured for email verification. Supported factors: {strategies}"
        );
    }
}

fn ensure_sign_up_ready(sign_up: &ClientSignUp) -> Result<()> {
    match sign_up.status {
        SignUpStatus::MissingRequirements | SignUpStatus::Complete => Ok(()),
        _ => bail!(
            "Clerk returned unsupported status '{:?}' when starting sign-up",
            sign_up.status
        ),
    }
}

fn ensure_sign_up_email_supported(sign_up: &ClientSignUp) -> Result<()> {
    let verification = sign_up_email_verification(sign_up)?;
    if verification
        .supported_strategies
        .iter()
        .any(|strategy| strategy == "email_code")
    {
        Ok(())
    } else {
        let strategies = if verification.supported_strategies.is_empty() {
            "none".to_string()
        } else {
            verification.supported_strategies.join(", ")
        };
        bail!(
            "Clerk sign-up flow is not configured for email verification. Supported factors: {strategies}"
        );
    }
}

fn needs_prepare(sign_up: &ClientSignUp) -> bool {
    matches!(
        sign_up_email_verification(sign_up)
            .map(|verification| verification.next_action)
            .unwrap_or(SignUpNextAction::Empty),
        SignUpNextAction::NeedsPrepare
    )
}

fn sign_up_email_verification(sign_up: &ClientSignUp) -> Result<&StubsSignUpVerification> {
    sign_up
        .verifications
        .email_address
        .as_deref()
        .ok_or_else(|| anyhow!("Clerk sign-up response missing email verification state"))
}

fn format_sign_in_strategies(factors: Option<&[StubsSignInFactor]>) -> String {
    factors
        .map(|list| {
            if list.is_empty() {
                "none".to_string()
            } else {
                list.iter()
                    .map(|factor| strategy_label(&factor.strategy))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn strategy_label(strategy: &SignInStrategy) -> String {
    serde_json::to_string(strategy)
        .map(|value| value.trim_matches('"').to_string())
        .unwrap_or_else(|_| format!("{strategy:?}"))
}

fn ensure_sign_in_complete(sign_in: &ClientSignIn) -> Result<()> {
    if matches!(sign_in.status, SignInStatus::Complete) {
        Ok(())
    } else {
        bail!(
            "Clerk sign-in flow did not complete; status '{:?}'",
            sign_in.status
        );
    }
}

fn ensure_sign_up_complete(sign_up: &ClientSignUp) -> Result<()> {
    if matches!(sign_up.status, SignUpStatus::Complete) {
        Ok(())
    } else {
        let missing = if sign_up.missing_fields.is_empty() {
            "unspecified fields".to_string()
        } else {
            sign_up.missing_fields.join(", ")
        };
        let unverified = if sign_up.unverified_fields.is_empty() {
            "none".to_string()
        } else {
            sign_up.unverified_fields.join(", ")
        };
        bail!(
            "Clerk sign-up flow did not complete; status '{:?}'. Missing fields: {}. Unverified fields: {}",
            sign_up.status,
            missing,
            unverified
        );
    }
}

fn map_api_error<T: std::fmt::Debug>(err: apis::Error<T>, action: &str) -> anyhow::Error {
    match err {
        apis::Error::ResponseError(resp) => map_response_error(resp, action),
        other => anyhow!("failed to {action}: {other}"),
    }
}

fn map_response_error<T>(resp: apis::ResponseContent<T>, action: &str) -> anyhow::Error {
    let message =
        parse_clerk_error_message(&resp.content).unwrap_or_else(|| truncate_body(&resp.content));
    anyhow!(
        "failed to {action}: Clerk responded with {} – {}",
        resp.status,
        message
    )
}

fn parse_clerk_error_message(body: &str) -> Option<String> {
    let payload: ClerkErrors = serde_json::from_str(body).ok()?;
    if payload.errors.is_empty() {
        None
    } else {
        Some(
            payload
                .errors
                .iter()
                .map(|error| {
                    if !error.long_message.is_empty() {
                        error.long_message.clone()
                    } else {
                        error.message.clone()
                    }
                })
                .collect::<Vec<_>>()
                .join("; "),
        )
    }
}

fn truncate_body(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return "<empty response>".to_string();
    }

    const MAX_LEN: usize = 512;
    let mut snippet = trimmed.chars().take(MAX_LEN).collect::<String>();
    if trimmed.chars().count() > MAX_LEN {
        snippet.push('…');
    }
    snippet
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_base_url_with_scheme() {
        assert_eq!(
            normalize_base_url("https://example.com/foo").unwrap(),
            "https://example.com/foo"
        );
        assert_eq!(
            normalize_base_url("example.com/").unwrap(),
            "https://example.com"
        );
    }
}
