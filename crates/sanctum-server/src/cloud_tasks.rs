use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use reqwest::{Client, StatusCode, Url};
use sanctum_config::CloudTasksConfig;
use sanctum_core::{Error, Result};
use sanctum_service::{Dispatch, Task, TaskStage};
use serde_json::{Value, json};
use tokio::sync::OnceCell;

const ENDPOINT: &str = "https://cloudtasks.googleapis.com/v2/";
const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

pub(crate) struct CloudTasks {
    config: CloudTasksConfig,
    worker: Url,
    url: Url,
    client: Client,
    credentials: OnceCell<AccessTokenCredentials>,
}

impl CloudTasks {
    pub(crate) fn new(config: CloudTasksConfig) -> Result<Self> {
        Self::with_endpoint(config, ENDPOINT)
    }

    fn with_endpoint(config: CloudTasksConfig, endpoint: &str) -> Result<Self> {
        let queue: Vec<_> = config.queue.split('/').collect();
        if !matches!(queue.as_slice(), ["projects", project, "locations", region, "queues", queue]
        if [project, region, queue].into_iter().all(|part| {
            !part.is_empty() && part.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || b"_-".contains(&byte)
            })
        })) {
            return Err(Error::Invalid(
                "Cloud Tasks queue must be projects/PROJECT/locations/REGION/queues/QUEUE".into(),
            ));
        }
        let worker = Url::parse(&config.worker_url)
            .map_err(|_| Error::Invalid("invalid Cloud Tasks worker URL".into()))?;
        if worker.scheme() != "https"
            || worker.host_str().is_none()
            || !worker.username().is_empty()
            || worker.password().is_some()
            || worker.path() != "/"
            || worker.query().is_some()
            || worker.fragment().is_some()
        {
            return Err(Error::Invalid(
                "Cloud Tasks worker URL must be an HTTPS origin".into(),
            ));
        }
        if !config
            .service_account
            .split_once('@')
            .is_some_and(|(account, domain)| {
                !account.is_empty()
                    && domain.ends_with(".iam.gserviceaccount.com")
                    && config
                        .service_account
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.@".contains(&byte))
            })
        {
            return Err(Error::Invalid(
                "invalid Cloud Tasks service account email".into(),
            ));
        }
        let url = Url::parse(&format!("{endpoint}{}/tasks", config.queue))
            .map_err(|error| Error::Internal(Box::new(error)))?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(12))
            .build()
            .map_err(|error| Error::Internal(Box::new(error)))?;
        Ok(Self {
            config,
            worker,
            url,
            client,
            credentials: OnceCell::new(),
        })
    }

    async fn token(&self) -> Result<String> {
        let credentials = self
            .credentials
            .get_or_try_init(|| async {
                tokio::task::spawn_blocking(|| {
                    Builder::default()
                        .with_scopes([SCOPE])
                        .build_access_token_credentials()
                })
                .await
                .map_err(|error| Error::Internal(Box::new(error)))?
                .map_err(|_| {
                    Error::Busy(
                        "Cloud Tasks Application Default Credentials are unavailable".into(),
                    )
                })
            })
            .await?;
        credentials
            .access_token()
            .await
            .map(|value| value.token)
            .map_err(|_| Error::Busy("could not refresh Cloud Tasks credentials".into()))
    }

    fn payload(&self, task: Task<'_>) -> Result<Value> {
        let (stage, step) = match task.stage {
            TaskStage::Register => ("register-upload", None),
            TaskStage::Index => ("index-upload", None),
            TaskStage::Embed { step } => ("embed-upload", Some(step)),
            TaskStage::Rebuild { step } => ("rebuild-collection", Some(step)),
        };
        let mut worker = self.worker.clone();
        worker.set_path(&format!("/internal/tasks/{stage}"));
        let identity = format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            self.worker,
            task.scope.tenant,
            task.scope.collection,
            task.operation,
            stage,
            step.unwrap_or(0),
            task.window,
        );
        let id = blake3::hash(identity.as_bytes());
        let mut body = json!({
            "tenant": task.scope.tenant.as_str(),
            "collection": task.scope.collection.as_str(),
            "operation_id": task.operation,
        });
        if let Some(step) = step {
            body["step"] = json!(step);
        }
        let body = serde_json::to_vec(&body).map_err(|error| Error::Internal(Box::new(error)))?;
        let mut payload = json!({
            "task": {
                "name": format!("{}/tasks/{id}", self.config.queue),
                "dispatchDeadline": "600s",
                "httpRequest": {
                    "httpMethod": "POST",
                    "url": worker.as_str(),
                    "headers": { "Content-Type": "application/json" },
                    "body": STANDARD.encode(body),
                    "oidcToken": {
                        "serviceAccountEmail": self.config.service_account,
                        "audience": self.worker.as_str().trim_end_matches('/'),
                    }
                }
            }
        });
        if let Some(seconds) = task.not_before {
            let timestamp = i64::try_from(seconds)
                .ok()
                .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
                .ok_or_else(|| {
                    Error::Internal("task schedule time is outside its supported range".into())
                })?;
            payload["task"]["scheduleTime"] =
                json!(timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
        }
        Ok(payload)
    }

    async fn send_with_token(&self, task: Task<'_>, token: &str) -> Result<()> {
        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(token)
            .json(&self.payload(task)?)
            .send()
            .await
            .map_err(|_| Error::Busy("Cloud Tasks create request did not complete".into()))?;
        if response.status().is_success() || response.status() == StatusCode::CONFLICT {
            Ok(())
        } else {
            Err(Error::Busy(format!(
                "Cloud Tasks create returned HTTP {}",
                response.status().as_u16()
            )))
        }
    }
}

#[async_trait::async_trait]
impl Dispatch for CloudTasks {
    async fn send(&self, task: Task<'_>) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            self.send_with_token(task, &self.token().await?).await
        })
        .await
        .map_err(|_| Error::Busy("Cloud Tasks dispatch timed out".into()))?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use sanctum_core::Scope;
    use tokio::net::TcpListener;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn sends_oidc_tasks_and_treats_same_name_as_replayed() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let fixture = Router::new()
            .route(
                "/v2/projects/project/locations/region/queues/queue/tasks",
                post(accept),
            )
            .with_state(calls.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v2/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, fixture).await.unwrap() });
        let dispatcher = CloudTasks::with_endpoint(
            CloudTasksConfig {
                queue: "projects/project/locations/region/queues/queue".into(),
                worker_url: "https://sanctum-worker.example".into(),
                service_account: "tasks@project.iam.gserviceaccount.com".into(),
            },
            &endpoint,
        )
        .unwrap();
        let scope = Scope {
            tenant: "tenant".parse().unwrap(),
            collection: "collection".parse().unwrap(),
        };
        let operation = Uuid::new_v4();
        let task = |stage, window| Task {
            scope: &scope,
            operation,
            stage,
            window,
            not_before: None,
        };
        dispatcher
            .send_with_token(task(TaskStage::Register, 1), "fixture")
            .await
            .unwrap();
        dispatcher
            .send_with_token(task(TaskStage::Register, 1), "fixture")
            .await
            .unwrap();
        assert!(matches!(
            dispatcher.send_with_token(task(TaskStage::Index, 1), "fixture").await,
            Err(Error::Busy(message)) if message.contains("503")
        ));
        dispatcher
            .send_with_token(task(TaskStage::Register, 2), "fixture")
            .await
            .unwrap();

        let scheduled = dispatcher
            .payload(Task {
                not_before: Some(1_800_000_000),
                ..task(TaskStage::Embed { step: 2 }, 2)
            })
            .unwrap();
        let http = &scheduled["task"]["httpRequest"];
        assert_eq!(
            http["url"],
            "https://sanctum-worker.example/internal/tasks/embed-upload"
        );
        assert_eq!(scheduled["task"]["scheduleTime"], "2027-01-15T08:00:00Z");
        let body = STANDARD.decode(http["body"].as_str().unwrap()).unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["step"], 2);
        assert_eq!(body["operation_id"], operation.to_string());

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0], calls[1]);
        assert_ne!(calls[0]["task"]["name"], calls[2]["task"]["name"]);
        assert_ne!(calls[0]["task"]["name"], calls[3]["task"]["name"]);
        let http = &calls[0]["task"]["httpRequest"];
        assert_eq!(
            http["url"],
            "https://sanctum-worker.example/internal/tasks/register-upload"
        );
        assert_eq!(
            http["oidcToken"]["audience"],
            "https://sanctum-worker.example"
        );
        assert_eq!(
            http["oidcToken"]["serviceAccountEmail"],
            "tasks@project.iam.gserviceaccount.com"
        );
        let decoded = STANDARD.decode(http["body"].as_str().unwrap()).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&decoded).unwrap(),
            json!({
                "tenant": "tenant", "collection": "collection", "operation_id": operation
            })
        );
        server.abort();
    }

    type Calls = Arc<Mutex<Vec<Value>>>;

    async fn accept(
        State(calls): State<Calls>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> StatusCode {
        assert_eq!(headers["authorization"], "Bearer fixture");
        let mut calls = calls.lock().unwrap();
        calls.push(body);
        match calls.len() {
            2 => StatusCode::CONFLICT,
            3 => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::OK,
        }
    }
}
