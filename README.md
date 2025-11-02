# Intelligent Log Analysis & Anomaly Detection Platform 🔎

**Status:** Phase 1 (Core Data Pipeline) Complete & Tested

## Project Overview

This platform provides a scalable solution for ingesting, storing, and analyzing application logs. It features a high-throughput ingestion API and an asynchronous processing pipeline designed to handle significant load. The long-term goal is to incorporate AI-driven features like log clustering and anomaly detection to provide actionable insights from log data.

This project was built following professional engineering practices, including a dockerized environment, asynchronous task processing, and a focus on reliability and scalability.

---

## Architecture

The system employs a decoupled, asynchronous architecture optimized for high-throughput log ingestion and processing.



**Core Components:**

1.  **Nginx:** Acts as a reverse proxy, handling incoming HTTP requests, enforcing rate limits and payload size limits, and load balancing to the API server(s).
2.  **Django/DRF API (`web` service):** Provides the `/api/ingest/` endpoint. Handles API key authentication (with caching), validates incoming log data, and pushes validated logs onto the Redis Stream queue. Also serves read-only APIs for dashboards (future) and the `/metrics` endpoint for Prometheus.
3.  **Redis:** Acts as a high-speed message broker (using Streams for reliability) and a cache (for API key authentication).
4.  **Celery (`celery-worker`, `celery-beat` services):** Handles asynchronous processing.
    * `celery-beat`: Schedules the `process_log_master` task to run periodically.
    * `celery-worker`: Executes the `process_log_master` and `process_log_chunk` tasks.
5.  **PgBouncer:** A connection pooler that manages connections between the numerous Celery workers/API instances and the PostgreSQL database, preventing database connection exhaustion.
6.  **PostgreSQL (`db` service):** The primary data store for processed log entries and application metadata.
7.  **MinIO:** S3-compatible object storage, planned for log archival in future phases.
8.  **Prometheus & Grafana:** Observability stack for collecting and visualizing application metrics.

---

## Docker Setup

The entire application stack is orchestrated using Docker Compose (`docker-compose.yml` and `docker-compose.override.yml`).

* **Services:** Each component runs in its own container.
* **Networking:** All services communicate over a private bridge network (`appnet`).
* **Volumes:** Named volumes are used for persistent data storage (`pgdata`, `redisdata`, `miniodata`).
* **Configuration:** Service configurations (Nginx, PgBouncer, Prometheus) are managed via volume mounts from the `deploy/` directory. Secrets and environment-specific settings are loaded via a `.env` file.
* **Development vs. Production:**
    * The base `docker-compose.yml` uses Gunicorn for the `web` service.
    * `docker-compose.override.yml` switches the `web` service to use Django's development server (`runserver`) and adds bind mounts for hot-reloading during local development.

---

## Data Flow (Log Lifecycle)

1.  **Ingestion:** A client application sends a `POST` request with log data (JSON payload) and an `X-API-Key` header to `http://<host>/api/ingest/`.
2.  **Proxy & Auth:** Nginx receives the request, checks rate/size limits, and forwards it to the `web` (Django) service. The `HasAPIKey` permission class validates the key, first checking a Redis cache, then falling back to the database.
3.  **Validation & Queuing:** The `LogIngestionView` validates the payload using `LogEntrySerializer`. If valid, it adds the corresponding `application_id`, converts necessary fields (timestamp, metadata) to strings, and pushes the data as a message onto the `logs:queue` Redis Stream using `XADD`. It returns a `202 Accepted` response.
4.  **Processing (Master Task):** The `celery-beat` service schedules the `process_log_master` task every 10 seconds. A `celery-worker` executes this task, reading a batch of messages from the `logs:queue` stream using `XREADGROUP`. It decodes the messages and groups them into chunks (including message IDs).
5.  **Parallel Processing (Subtasks):** The master task dispatches these chunks to multiple `process_log_chunk` subtasks using `celery.group`, allowing parallel processing.
6.  **Database Write & Ack:** Each `process_log_chunk` subtask fetches the required `Application` objects, prepares `LogEntry` model instances, and saves them to PostgreSQL using `bulk_create` within a transaction. **Crucially**, only after the database write succeeds does the subtask acknowledge (`XACK`) the corresponding message IDs in the Redis Stream, ensuring at-least-once processing.

---

## Phase 1 Completion Status

Phase 1 focused on building and testing the core data pipeline. The following components are complete and functional:

* [✅] Dockerized environment setup for all services.
* [✅] Database models (`Application`, `LogEntry`) defined and migrated.
* [✅] API Key authentication with Redis caching.
* [✅] Log ingestion API endpoint (`/api/ingest/`) with validation.
* [✅] Queuing mechanism using Redis Streams.
* [✅] Asynchronous processing pipeline using Celery master/subtasks.
* [✅] Efficient database writes using `bulk_create`.
* [✅] Reliable message acknowledgment (`XACK`) after DB write.
* [✅] Basic load testing script implemented and used for validation.

---

## Key Design Decisions

* **Asynchronous Processing:** All heavy processing (DB writes) is handled asynchronously by Celery workers to keep the ingestion API fast and responsive.
* **Redis Streams:** Chosen over Redis Lists for better reliability, message tracking (acknowledgments), and consumer group coordination.
* **PgBouncer:** Used to manage database connections efficiently under high concurrency from Celery workers and the API service.
* **API Key Caching:** Implemented cache-aside pattern in Redis for API key authentication to reduce database load from the `web` service under high request volume.
* **Bulk Inserts:** Celery workers use `bulk_create` to minimize database transaction overhead.
* **Reliable Acknowledgment:** Redis Stream messages are acknowledged (`XACK`) only *after* the corresponding data is successfully saved to the database.

---

## Getting Started (Local Development)

1.  **Clone the repository.**
2.  **Create `.env` file:** In the project's root directory, copy `.env.example` to `.env` and fill in the required secrets.
    ```bash
    cp .env.example .env
    ```
    Ensure the following variables are set correctly for the Docker environment:
    ```ini
    # .env
    DJANGO_SECRET_KEY=your-strong-secret-key
    POSTGRES_DB=db
    POSTGRES_USER=user
    POSTGRES_PASSWORD=password
    DATABASE_URL=postgres://user:password@pgbouncer:6432/db
    CELERY_BROKER_URL=redis://redis:6379/0
    ```
3.  **Build and Start Containers:**
    ```bash
    docker-compose up --build -d
    ```
4.  **(First Run Only) Apply Migrations & Create Superuser:**
    ```bash
    docker-compose exec web python manage.py migrate
    docker-compose exec web python manage.py createsuperuser
    ```
5.  **(First Run Only) Create Application:** Use the Django admin (accessible at `http://localhost/admin/`) or the shell to create an `Application` instance and get its API key.
    ```bash
    docker-compose exec web python manage.py shell
    >>> from logs.models import Application
    >>> app = Application.objects.create(name='My Test App')
    >>> print(app.api_key)
    >>> exit()
    ```
6.  **Run Load Tester (Optional):** Open `scripts/load_tester.py` and replace the placeholder `API_KEY` with the one you generated in the previous step.
    ```bash
    python load_tester.py --mode bulk --num_logs 100 --concurrency 10
    ```
7.  **Access Services:**
    *   **API:** `http://localhost/api/ingest/` (via Nginx)
    *   **Django Admin:** `http://localhost/admin/`
    *   **Grafana:** `http://localhost:3000`
    *   **Prometheus:** `http://localhost:9090`

---

## Next Steps (Phase 2)

* Implement Dead Letter Queue (DLQ) for failed log processing.
* Add metrics collection using `django-prometheus`.
* Implement simple rule-based alerting task.
* Set up basic Grafana dashboards.

Summary
URL	Service	Metrics Provided	Purpose
localhost:8000/metrics	web (Django)	API requests, DB queries, Cache hits	API Performance Monitoring
localhost:9808/	celery-exporter	Task successes/failures, Runtimes	General Celery Health
localhost:8001/	celery-worker	Your custom metrics (logs processed, queue length)	Application-Specific Logic Monitoring
