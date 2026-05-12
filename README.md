# Distributed Job Scheduler

Multi-module Spring Boot 3 system: **api-service** (REST + DB), **scheduler-service** (Redis lock + due jobs → Kafka), **worker-service** (Kafka consumer + retries + DLQ). Shared JPA model lives in **common-lib**.

## Prerequisites

- Java 17
- Maven 3.9+ **or** use the included Maven Wrapper (`mvnw` / `mvnw.cmd`; no global Maven on `PATH` required)
- Docker (for Kafka, Zookeeper, Redis, PostgreSQL)

## Start infrastructure

From the repository root:

```bash
docker compose up -d
```

Wait until Kafka becomes healthy (first start can take ~30–60 seconds).

## Build

**Option A — Maven Wrapper (works even when `mvn` is not on `PATH`)**

Windows (PowerShell or `cmd`, from repo root):

```powershell
.\mvnw.cmd clean install -DskipTests
```

macOS / Linux:

```bash
./mvnw clean install -DskipTests
```

The first run may download Maven 3.9.6 into your user cache.

**Option B — Global Maven**

```bash
mvn clean install -DskipTests
```

If `mvn` is not found, add Maven’s `bin` to your user `PATH`, for example:

`C:\Program Files\apache-maven-3.9.14\bin`

(Win + R → `sysdm.cpl` → Advanced → Environment Variables → edit **Path** under your user.)

If Maven reports `release version 17 not supported`, it is using an older JDK. Point `JAVA_HOME` at JDK 17 (for example `C:\Program Files\Java\jdk-17` on Windows) and ensure `%JAVA_HOME%\bin` is on `PATH`.

## Run services (three terminals)

Defaults assume `localhost` and ports from `docker-compose.yml`.

1. **API** — port `8080`

   ```bash
   java -jar api-service/target/api-service-1.0.0-SNAPSHOT.jar
   ```

2. **Scheduler** — port `8081`

   ```bash
   java -jar scheduler-service/target/scheduler-service-1.0.0-SNAPSHOT.jar
   ```

3. **Worker** — port `8082`

   ```bash
   java -jar worker-service/target/worker-service-1.0.0-SNAPSHOT.jar
   ```

Override any setting with environment variables, for example:

| Variable | Purpose |
|----------|---------|
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | PostgreSQL |
| `REDIS_HOST`, `REDIS_PORT` | Redis (scheduler lock) |
| `KAFKA_BOOTSTRAP` | e.g. `localhost:9092` |

## Kafka topics

- `jobs-topic` — main job stream (message key = job UUID for partition stickiness).
- `jobs-dlq` — dead-letter records after max failures (JSON same as main message).

Topics are auto-created by the broker in this dev setup.

## Example API requests

**Create a job (run as soon as possible)**

```bash
curl -s -X POST http://localhost:8080/jobs ^
  -H "Content-Type: application/json" ^
  -d "{\"payload\":\"{\\\"task\\\":\\\"hello\\\"}\",\"priority\":10}"
```

**Create a delayed job**

```bash
curl -s -X POST http://localhost:8080/jobs ^
  -H "Content-Type: application/json" ^
  -d "{\"scheduleTime\":\"2030-01-01T00:00:00Z\",\"payload\":\"later\",\"priority\":0}"
```

**Get status**

```bash
curl -s http://localhost:8080/jobs/<job-id>
```

**Force worker failure (demo)**

Payload containing the substring `FAIL` triggers failure, exponential backoff reschedules, then DLQ after `worker.max-failures-before-dlq` (default `3`).

## Metrics

Actuator Prometheus scrape: `http://localhost:8080/actuator/prometheus` (API; same path on 8081/8082 for other services).

## Architecture notes

- **Scheduler**: Redis `SETNX` lock + TTL so one instance enqueues per tick; per-job transaction updates `PENDING → QUEUED` then publishes to Kafka (rollback if publish fails).
- **Worker**: Atomic `QUEUED → RUNNING` update implements idempotency under Kafka at-least-once delivery.
- **Retries**: On failure, `retryCount` increases, status returns to `PENDING` with `scheduleTime = now + backoff` (exponential, capped). After max failures → `FAILED` + message to DLQ.
- **Priority**: Due `PENDING` jobs are ordered `priority DESC`, then `scheduleTime ASC`.

## Module layout

```
job-scheduler/
├── common-lib/
├── api-service/
├── scheduler-service/
├── worker-service/
├── docker-compose.yml
└── pom.xml
```
