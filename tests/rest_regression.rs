use std::{net::TcpListener, path::PathBuf, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use eventdbx::{
    config::Config,
    plugin::PluginManager,
    server,
    token::{IssueTokenInput, TokenManager},
};
use fake::{
    Fake,
    faker::{
        address::en::{CityName, StateAbbr},
        company::en::{Buzzword, CompanyName},
        internet::en::{DomainSuffix, SafeEmail},
        lorem::en::{Paragraph, Words},
        name::en::{FirstName, LastName},
        number::raw::NumberWithFormat,
    },
    locales::EN,
};
use rand::{seq::SliceRandom, thread_rng};
use reqwest::Client;
use serde_json::{Value, json};
use std::io;
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default, Copy, Clone)]
struct SeedCounts {
    people: usize,
    companies: usize,
    animals: usize,
}

impl SeedCounts {
    fn max_take(self) -> usize {
        self.people + self.companies + self.animals + 10
    }
}

fn parse_seed_counts() -> SeedCounts {
    let mut counts = SeedCounts {
        people: 32,
        companies: 16,
        animals: 24,
    };

    for arg in std::env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--people=") {
            counts.people = value.parse().expect("--people must be numeric");
        } else if let Some(value) = arg.strip_prefix("--companies=") {
            counts.companies = value.parse().expect("--companies must be numeric");
        } else if let Some(value) = arg.strip_prefix("--animals=") {
            counts.animals = value.parse().expect("--animals must be numeric");
        }
    }

    counts
}

fn random_species() -> &'static str {
    const OPTIONS: &[&str] = &[
        "Canis lupus",
        "Panthera leo",
        "Elephas maximus",
        "Gorilla gorilla",
        "Delphinus delphis",
        "Loxodonta africana",
        "Cervus elaphus",
        "Equus zebra",
    ];
    OPTIONS
        .choose(&mut thread_rng())
        .copied()
        .unwrap_or("Canis lupus")
}

fn random_language() -> &'static str {
    const LANGS: &[&str] = &["en", "es", "fr", "de", "ja", "zh", "pt", "vi"];
    LANGS.choose(&mut thread_rng()).copied().unwrap_or("en")
}

fn allocate_port() -> io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

#[tokio::test(flavor = "multi_thread")]
async fn rest_person_company_flow() -> TestResult<()> {
    let counts = parse_seed_counts();
    let temp = TempDir::new()?;
    let mut config = Config::default();
    config.data_dir = temp.path().join("data");
    let port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping rest regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    config.port = port;
    config.restrict = false;
    config.data_encryption_key = Some(STANDARD.encode([1u8; 32]));
    config.ensure_data_dir()?;
    let config_path = temp.path().join("config.toml");
    config.save(&config_path)?;

    // Prepare token
    let encryptor = config
        .encryption_key()?
        .expect("encryption key should be configured");
    let token_manager = TokenManager::load(config.tokens_path(), Some(encryptor.clone()))?;
    let token = token_manager
        .issue(IssueTokenInput {
            group: "testers".into(),
            user: "regression".into(),
            expiration_secs: Some(3600),
            limit: None,
            keep_alive: true,
        })?
        .token;
    drop(token_manager);

    let plugins = PluginManager::from_config(&config)?;
    let server_handle = spawn_server(config.clone(), config_path.clone(), plugins)?;

    let base_url = format!("http://127.0.0.1:{}", config.port);
    wait_for_health(&base_url).await?;

    let client = Client::new();

    // Seed sample aggregates
    seed_people(&client, &base_url, &token, counts.people).await?;
    seed_companies(&client, &base_url, &token, counts.companies).await?;
    seed_animals(&client, &base_url, &token, counts.animals).await?;

    // Verify aggregate listing
    let aggregates: Value = client
        .get(format!(
            "{base_url}/v1/aggregates?take={}",
            counts.max_take()
        ))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let entries = aggregates
        .as_array()
        .expect("aggregates response should be array");
    assert!(
        entries
            .iter()
            .any(|entry| entry["aggregate_type"] == "person"),
        "person aggregate should be present"
    );
    if counts.companies > 0 {
        assert!(
            entries
                .iter()
                .any(|entry| entry["aggregate_type"] == "company"),
            "company aggregate should be present"
        );
    }
    if counts.animals > 0 {
        assert!(
            entries
                .iter()
                .any(|entry| entry["aggregate_type"] == "animal"),
            "animal aggregate should be present"
        );
    }

    // Verify person aggregate state
    if counts.people > 0 {
        let person_index = counts.people - 1;
        let target_person = format!("person-{person_index:05}");
        let person_state: Value = client
            .get(format!("{base_url}/v1/aggregates/person/{target_person}"))
            .bearer_auth(&token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        assert_eq!(
            person_state["aggregate_id"], target_person,
            "aggregate id should match"
        );
        assert!(
            person_state["state"].get("first_name").is_some(),
            "person state must include first_name"
        );
        assert!(
            person_state["state"].get("preferences").is_some(),
            "person state should include nested preferences"
        );

        let events: Value = client
            .get(format!(
                "{base_url}/v1/aggregates/person/{target_person}/events?take=5"
            ))
            .bearer_auth(&token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        assert!(
            events
                .as_array()
                .expect("events response must be array")
                .first()
                .is_some(),
            "events array should not be empty"
        );
    }

    if counts.animals > 0 {
        let animal_index = counts.animals - 1;
        let target_animal = format!("animal-{animal_index:05}");
        let animal_state: Value = client
            .get(format!("{base_url}/v1/aggregates/animal/{target_animal}"))
            .bearer_auth(&token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        assert_eq!(
            animal_state["aggregate_id"], target_animal,
            "animal aggregate id should match"
        );
        let animal_fields = animal_state["state"]
            .as_object()
            .expect("animal state should be an object");
        assert!(
            animal_fields.len() >= 9,
            "animal aggregate should have a rich attribute set"
        );
        assert!(
            animal_fields.contains_key("species"),
            "animal state should include species"
        );
    }

    // Shutdown server
    server_handle.abort();
    let _ = server_handle.await;

    Ok(())
}

fn spawn_server(
    config: Config,
    config_path: PathBuf,
    plugins: PluginManager,
) -> TestResult<JoinHandle<eventdbx::error::Result<()>>> {
    Ok(tokio::spawn(async move {
        server::run(config, config_path, plugins).await
    }))
}

async fn wait_for_health(base_url: &str) -> TestResult<()> {
    let client = Client::new();
    for _ in 0..40 {
        if let Ok(resp) = client.get(format!("{base_url}/health")).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err("server did not become healthy in time".into())
}

async fn seed_people(client: &Client, base_url: &str, token: &str, count: usize) -> TestResult<()> {
    for index in 0..count {
        let aggregate_id = format!("person-{index:05}");
        let payload = json!({
            "first_name": FirstName().fake::<String>(),
            "last_name": LastName().fake::<String>(),
            "email": SafeEmail().fake::<String>(),
            "phone": format!("+1-{}", NumberWithFormat(EN, "###-###-####").fake::<String>()),
            "street": format!(
                "{} {}",
                NumberWithFormat(EN, "#####").fake::<String>(),
                Words(1..3).fake::<Vec<String>>().join(" ")
            ),
            "city": CityName().fake::<String>(),
            "state": StateAbbr().fake::<String>(),
            "postal_code": NumberWithFormat(EN, "#####").fake::<String>(),
            "company": CompanyName().fake::<String>(),
            "job_title": Buzzword().fake::<String>(),
            "bio": Paragraph(1..2).fake::<String>(),
            "preferences": {
                "language": random_language(),
                "email_opt_in": index % 2 == 0
            }
        });
        send_event(
            client,
            base_url,
            token,
            "person",
            &aggregate_id,
            "person-upserted",
            payload,
        )
        .await?;
    }
    Ok(())
}

async fn seed_companies(
    client: &Client,
    base_url: &str,
    token: &str,
    count: usize,
) -> TestResult<()> {
    for index in 0..count {
        let aggregate_id = format!("company-{index:05}");
        let payload = json!({
            "legal_name": CompanyName().fake::<String>(),
            "industry": Buzzword().fake::<String>(),
            "domain": DomainSuffix().fake::<String>(),
            "description": Paragraph(1..2).fake::<String>()
        });
        send_event(
            client,
            base_url,
            token,
            "company",
            &aggregate_id,
            "company-upserted",
            payload,
        )
        .await?;
    }
    Ok(())
}

async fn seed_animals(
    client: &Client,
    base_url: &str,
    token: &str,
    count: usize,
) -> TestResult<()> {
    for index in 0..count {
        let aggregate_id = format!("animal-{index:05}");
        let payload = json!({
            "species": random_species(),
            "common_name": Words(1..2).fake::<Vec<String>>().join(" "),
            "habitat": Paragraph(1..2).fake::<String>(),
            "diet": Words(3..6).fake::<Vec<String>>().join(", "),
            "age_years": NumberWithFormat(EN, "##").fake::<String>(),
            "caretaker": format!("{} {}", FirstName().fake::<String>(), LastName().fake::<String>()),
            "enclosure": format!("Zone-{}", NumberWithFormat(EN, "##").fake::<String>()),
            "health_status": Words(2..4).fake::<Vec<String>>().join(" "),
            "tracking_tag": format!("TAG-{}", NumberWithFormat(EN, "#####").fake::<String>()),
            "notes": Paragraph(2..3).fake::<String>()
        });
        send_event(
            client,
            base_url,
            token,
            "animal",
            &aggregate_id,
            "animal-upserted",
            payload,
        )
        .await?;
    }
    Ok(())
}

async fn send_event(
    client: &Client,
    base_url: &str,
    token: &str,
    aggregate_type: &str,
    aggregate_id: &str,
    event_type: &str,
    payload: Value,
) -> TestResult<()> {
    client
        .post(format!("{base_url}/v1/events"))
        .bearer_auth(token)
        .json(&json!({
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "payload": payload
        }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
