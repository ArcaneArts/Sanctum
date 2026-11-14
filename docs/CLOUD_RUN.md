# Deploy on Google Cloud Run

Sanctum uses one Linux image for two Cloud Run services. The API accepts
gRPC calls over HTTP/2. The worker accepts private Cloud Tasks and Cloud
Scheduler HTTP requests. Both use the same GCS bucket and prefix. Cloud Run
injects PORT and the configured role binds to that port on all interfaces.
The services handle SIGTERM and recover from the last durable checkpoint
if shutdown interrupts work. [Cloud Run container contract](https://docs.cloud.google.com/run/docs/container-contract), [HTTP/2 configuration](https://docs.cloud.google.com/run/docs/configuring/http2).

## Build and prepare Google Cloud resources

Set the variables below for an authorized project. The examples assume
the bucket, Artifact Registry repository, and service accounts already exist.
Use a bucket without retention rules that prevent Sanctum from deleting
reclaimed objects. Keep the bucket private.

| Variable | Value |
| --- | --- |
| PROJECT | Google Cloud project ID |
| REGION | Cloud Run, Cloud Tasks, and Cloud Scheduler region |
| IMAGE | Full Artifact Registry image path and unique tag |
| BUCKET | GCS bucket name |
| PREFIX | Storage prefix dedicated to this deployment |
| QUEUE | Cloud Tasks queue ID |
| API_SA, WORKER_SA | Runtime service account emails |
| TASKS_SA, SCHEDULER_SA | Invocation service account emails |
| CALLER_SA | Trusted application service account email |

The project needs Cloud Run, Cloud Storage, Artifact Registry, Cloud Tasks,
and Cloud Scheduler. Enable their APIs and build and push the image with an
authorized identity.

```sh
gcloud services enable run.googleapis.com storage.googleapis.com \
  artifactregistry.googleapis.com cloudtasks.googleapis.com \
  cloudscheduler.googleapis.com --project "$PROJECT"
gcloud auth configure-docker "$REGION-docker.pkg.dev"
docker buildx build --platform linux/amd64 --load -t "$IMAGE" .
docker push "$IMAGE"
gcloud tasks queues create "$QUEUE" --project "$PROJECT" --location "$REGION" \
  --max-concurrent-dispatches 2
```

Grant both runtime identities Storage Object User on the bucket, Cloud Tasks
Enqueuer on the queue, and Service Account User on TASKS_SA. Keep the
Cloud Tasks and Cloud Scheduler service agents' standard service-agent roles.
The deployer needs permission to attach the runtime identities and create
scheduler jobs as SCHEDULER_SA. [Cloud Tasks HTTP targets](https://docs.cloud.google.com/tasks/docs/creating-http-target-tasks), [Cloud Scheduler HTTP authentication](https://docs.cloud.google.com/scheduler/docs/http-target-auth).

## Deploy the services

The worker needs its own service URL as the OIDC audience. Deploy it once to
obtain the URL, then deploy both roles with the full queue configuration. The
example uses jq to serialize SANCTUM_CONFIG_JSON in a format accepted by
gcloud run deploy --env-vars-file.

```sh
config_dir="$(mktemp -d)"
write_config() {
  jq -n --arg role "$1" --arg bucket "$BUCKET" --arg prefix "$PREFIX" \
    --arg queue "projects/$PROJECT/locations/$REGION/queues/$QUEUE" \
    --arg worker_url "${WORKER_URL:-}" --arg tasks_sa "$TASKS_SA" '
      {
        version: 1, cloud_run_role: $role, mcp_listen: null,
        object_storage: {backend: "gcs", bucket: $bucket, prefix: $prefix}
      }
      + if $worker_url == "" then {} else {
        cloud_tasks: {queue: $queue, worker_url: $worker_url, service_account: $tasks_sa}
      } end
      | {SANCTUM_CONFIG_JSON: tojson}
    ' > "$config_dir/$1.json"
}

deploy_worker() {
  gcloud run deploy sanctum-worker --project "$PROJECT" --region "$REGION" \
    --image "$IMAGE" --service-account "$WORKER_SA" \
    --env-vars-file "$config_dir/worker.json" --no-allow-unauthenticated \
    --no-use-http2 --port 8080 --timeout 600s --concurrency 2 \
    --memory 8Gi --cpu 2 --min-instances 0
}

WORKER_URL=
write_config worker
deploy_worker
WORKER_URL="$(gcloud run services describe sanctum-worker --project "$PROJECT" \
  --region "$REGION" --format='value(status.url)')"
write_config worker
deploy_worker

write_config api
gcloud run deploy sanctum-api --project "$PROJECT" --region "$REGION" \
  --image "$IMAGE" --service-account "$API_SA" \
  --env-vars-file "$config_dir/api.json" --no-allow-unauthenticated \
  --use-http2 --port 8080 --timeout 600s --concurrency 2 \
  --memory 8Gi --cpu 2 --min-instances 0

for service_account in "$TASKS_SA" "$SCHEDULER_SA"; do
  gcloud run services add-iam-policy-binding sanctum-worker \
    --project "$PROJECT" --region "$REGION" \
    --member "serviceAccount:$service_account" --role roles/run.invoker
done
gcloud run services add-iam-policy-binding sanctum-api \
  --project "$PROJECT" --region "$REGION" \
  --member "serviceAccount:$CALLER_SA" --role roles/run.invoker
rm -r "$config_dir"
```

The examples start at two concurrent requests and dispatches. Cloud Run
accounts for temporary database and upload files in instance memory. Adjust
memory, timeouts, and concurrency after measuring the actual workload.
[Cloud Run service identity](https://docs.cloud.google.com/run/docs/authenticating/service-to-service), [Cloud Run file system](https://docs.cloud.google.com/run/docs/container-contract#file_system_access).

## Schedule dispatch repair

Create one HTTP job per shard. Each call scans one bounded page and advances
its stored cursor. The worker OIDC audience is the service origin.

```sh
for shard in $(seq 0 15); do
  gcloud scheduler jobs create http "sanctum-dispatch-$shard" \
    --project "$PROJECT" --location "$REGION" \
    --schedule '*/5 * * * *' --time-zone Etc/UTC --http-method POST \
    --uri "$WORKER_URL/internal/maintenance/dispatch/$shard" \
    --oidc-service-account-email "$SCHEDULER_SA" \
    --oidc-token-audience "$WORKER_URL" --attempt-deadline 300s
done

gcloud scheduler jobs create http sanctum-embedding-profile \
  --project "$PROJECT" --location "$REGION" \
  --schedule '*/5 * * * *' --time-zone Etc/UTC --http-method POST \
  --uri "$WORKER_URL/internal/maintenance/embedding-profile" \
  --oidc-service-account-email "$SCHEDULER_SA" \
  --oidc-token-audience "$WORKER_URL" --attempt-deadline 300s

gcloud scheduler jobs create http sanctum-source-sweep \
  --project "$PROJECT" --location "$REGION" \
  --schedule '*/5 * * * *' --time-zone Etc/UTC --http-method POST \
  --uri "$WORKER_URL/internal/maintenance/source-sweep" \
  --oidc-service-account-email "$SCHEDULER_SA" \
  --oidc-token-audience "$WORKER_URL" --attempt-deadline 300s

gcloud scheduler jobs create http sanctum-snapshot-sweep \
  --project "$PROJECT" --location "$REGION" \
  --schedule '*/5 * * * *' --time-zone Etc/UTC --http-method POST \
  --uri "$WORKER_URL/internal/maintenance/snapshot-sweep" \
  --oidc-service-account-email "$SCHEDULER_SA" \
  --oidc-token-audience "$WORKER_URL" --attempt-deadline 300s

gcloud scheduler jobs create http sanctum-upload-sweep \
  --project "$PROJECT" --location "$REGION" \
  --schedule '*/5 * * * *' --time-zone Etc/UTC --http-method POST \
  --uri "$WORKER_URL/internal/maintenance/upload-sweep" \
  --oidc-service-account-email "$SCHEDULER_SA" \
  --oidc-token-audience "$WORKER_URL" --attempt-deadline 300s
```

The upload sweep reclaims losing input attempts and old unpublished
extractions. It saves a scan cursor so subsequent calls continue the
same pass. The snapshot sweep advances stored cursors for catalog and collection
snapshots and deletes noncurrent generations after reader grace.
The source sweep advances a durable cursor and reclaims retired source
bytes in bounded pages. The embedding profile job checks the worker's configured profile and
resumes discovery after a change. It shares the existing dispatch
shards. Point scheduled requests and task deliveries at the active
worker revision when updating the embedding configuration.

## Verify a deployment

Use an identity allowed to impersonate CALLER_SA for this smoke test.
Submit a small text file that contains a known search term, then poll the
operation until it reports TEXT_READY. Fetch a returned chunk using its
source ID, revision ID, and ordinal.

```sh
API_URL="$(gcloud run services describe sanctum-api --project "$PROJECT" \
  --region "$REGION" --format='value(status.url)')"
export SANCTUM_GRPC_ID_TOKEN="$(gcloud auth print-identity-token \
  --impersonate-service-account "$CALLER_SA" --audiences "$API_URL")"
cargo run --locked -p sanctum-cli -- --endpoint "$API_URL" create notes
request_id="$(uuidgen)"
cargo run --locked -p sanctum-cli -- --endpoint "$API_URL" \
  upload notes ./notes.md --background --request-id "$request_id"
cargo run --locked -p sanctum-cli -- --endpoint "$API_URL" job notes "$request_id"
cargo run --locked -p sanctum-cli -- --endpoint "$API_URL" search notes 'known term'
curl --fail-with-body --http2 "$API_URL/mcp" \
  -H "Authorization: Bearer $SANCTUM_GRPC_ID_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2025-11-25' \
  --data '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
unset SANCTUM_GRPC_ID_TOKEN
```

The CLI adds the ID token only on HTTPS connections. The gRPC service
and stateless MCP endpoint share the API listener. Cloud Run forwards
requests to it over cleartext HTTP/2. Authorized callers can use the
same token with an HTTPS MCP client. Set the same embedding configuration
on both roles to enable background embedding of new revisions. When
updating Sanctum from a build that predates profile-bound tasks, update
the worker before the API so the worker accepts the new payload field.
[Cloud Run HTTP/2](https://docs.cloud.google.com/run/docs/configuring/http2),
[MCP client setup](MCP.md#hosted-connection).
The [storage guide](STORAGE.md) documents the worker stages, retry behavior,
and MinIO configuration. Live Cloud Run, GCS, and IAM verification requires
an authorized deployment environment.

Cloud Run roles write JSON logs to stderr. Use
`RUST_LOG=info,sanctum_server=debug,sanctum_service=debug,sanctum_db=debug`
on the API and worker to collect stage timings, dispatch outcomes, and
transfer sizes during a measurement window.
[Server diagnostics](LOCAL_API.md#server-diagnostics).

### Repeatable ingestion and range check

Generate the Python client as shown in the
[Python example](../examples/python/README.md). Set `API_URL` and export
`SANCTUM_GRPC_ID_TOKEN` as above, then run:

```sh
PYTHONPATH="$generated" python scripts/check-hosted-deployment.py \
  --api-url "$API_URL" --expect-private
```

The check uses a new test tenant, streams a deterministic UTF-8 fixture
through gRPC, waits for lexical publication, and compares the exact bytes
returned by gRPC and MCP. It checks that both routes reject unauthenticated
calls and writes a JSON report with the operation ID, dispatch shard, byte
count, and search timings. Use `--mib 200 --wait-seconds 600` to exercise the large
upload. The report's first and repeated search times are client
measurements. Correlate them with snapshot events and Cloud Run instance
IDs before interpreting them as cold and warm timings.
[Cloud Run request and system logs](https://docs.cloud.google.com/run/docs/logging).

### Recovery after scaling to zero

Run this exercise on a dedicated deployment with its own queue, scheduler
jobs, and storage prefix. It pauses delivery and creates a task that
arrives out of order.

1. Set the API and worker minimum instances to zero. Pause the Cloud
   Tasks queue and the deployment's scheduled worker invocations to allow
   the services to become idle. Choose a request ID and submit with no
   wait for indexing:

   ```sh
   operation="$(uuidgen | tr '[:upper:]' '[:lower:]')"
   PYTHONPATH="$generated" python scripts/check-hosted-deployment.py \
     --api-url "$API_URL" --operation "$operation" --phase submit
   ```

2. Using an identity with worker invocation permission and a token for
   `WORKER_URL`, POST the report's `tenant`, `collection`, and
   `operation` as `operation_id` to `/internal/tasks/prepare-upload` or
   `/internal/tasks/index-upload` before registration.
   Expect HTTP 503 and confirm the operation remains accepted. Then stop
   sending requests to both services. Check Cloud Monitoring's
   container instance count for both active and idle instances and
   record the observed idle interval. If the platform does not show
   the scale transition, record that separately from the application
   recovery result. [Cloud Run monitoring](https://docs.cloud.google.com/run/docs/monitoring),
   [Managing Cloud Tasks queues](https://docs.cloud.google.com/tasks/docs/manage-queues-and-tasks).
3. Resume the queue and scheduled jobs. Refresh the caller ID token if
   necessary and run the check with the same operation and size using
   `--phase verify --wait-seconds 600`. Record Cloud Tasks and worker
   request logs for registration, preparation, and indexing, then check the operation
   and source references in the report.
4. POST the original registration, preparation, and indexing task bodies again using
   the authorized worker identity. Expect HTTP 204 and verify the same
   revision and database generation are returned by another
   `--phase verify` check. Save the relevant instance IDs and generation
   from both runs. Cloud Run can choose a different instance for each
   request. The isolated test data stays in its deployment prefix for
   inspection.

### Repair an upload that never reached the queue

Run this exercise on a dedicated deployment with an empty queue and a fresh
storage prefix. Pause the real test queue and its scheduled dispatch jobs.
A paused queue still accepts tasks. Use an unused queue name in the test
API revision to make the task-creation request fail. The worker and Scheduler
keep the real queue configuration.
[Cloud Tasks queue behavior](https://docs.cloud.google.com/sdk/gcloud/reference/tasks/queues/pause).

1. Save the exact environment configuration used for the test API revision
   as `$config_dir/api.json`. If the deployment uses the unmodified
   example, recreate it with `write_config api` from the deploy section.
   Keep the diagnostics setting above on the worker. Create a separate
   file for the temporary API revision:

   ```sh
   absent_queue="sanctum-absent-$(uuidgen | tr '[:upper:]' '[:lower:]')"
   jq --arg queue "projects/$PROJECT/locations/$REGION/queues/$absent_queue" '
     .SANCTUM_CONFIG_JSON |= (fromjson | .cloud_tasks.queue = $queue | tojson)
   ' "$config_dir/api.json" > "$config_dir/api-repair-test.json"
   gcloud run services update sanctum-api --project "$PROJECT" --region "$REGION" \
     --env-vars-file "$config_dir/api-repair-test.json"
   ```

2. Submit with `--phase submit` and save the JSON report.

   ```sh
   operation="$(uuidgen | tr '[:upper:]' '[:lower:]')"
   report="$config_dir/accepted.json"
   PYTHONPATH="$generated" python scripts/check-hosted-deployment.py \
     --api-url "$API_URL" --operation "$operation" --phase submit > "$report"
   gcloud tasks list --queue "$QUEUE" --project "$PROJECT" --location "$REGION"
   ```

   Confirm `INGESTION_STATE_ACCEPTED`, the API warning
   `hosted operation remains pending dispatch` for that operation, and an
   empty real test queue. Restore the API configuration before invoking
   Scheduler.

   ```sh
   gcloud run services update sanctum-api --project "$PROJECT" --region "$REGION" \
     --env-vars-file "$config_dir/api.json"
   ```

3. Resume only the scheduled dispatch job whose number matches the
   report's `dispatch_shard`. Keep the real queue paused so the repaired
   task stays visible. Use its next scheduled invocation or trigger one
   on demand.

   ```sh
   shard="$(jq -r '.dispatch_shard' "$report")"
   gcloud scheduler jobs resume "sanctum-dispatch-$shard" \
     --project "$PROJECT" --location "$REGION"
   gcloud scheduler jobs run "sanctum-dispatch-$shard" \
     --project "$PROJECT" --location "$REGION"
   ```

   Record the successful Scheduler execution, the worker's
   `dispatch_reconcile` event with a positive dispatch count, and
   `upload_task_dispatched` for the operation at stage `Register`.
   List the queue again after the invocation completes and confirm the
   registration task appears. During this phase, invoke the repair endpoint only through
   Scheduler. [Scheduler execution logs](https://docs.cloud.google.com/scheduler/docs/troubleshooting),
   [Cloud Tasks inspection](https://docs.cloud.google.com/sdk/gcloud/reference/tasks/list).

4. Resume the queue and remaining scheduled jobs. Run `--phase verify`
   with the original operation and fixture size. Record text readiness,
   revision, and matching gRPC and MCP bytes. Restore the test deployment's
   normal queue and scheduler state if any exercise step fails.
