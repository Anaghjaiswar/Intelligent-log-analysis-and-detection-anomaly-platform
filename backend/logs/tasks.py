import json
import logging
from datetime import datetime
from celery import shared_task, group
from django.core.cache import caches
from django.db import transaction
from .models import Application, LogEntry

logger = logging.getLogger(__name__)

# -----------Architecture-Flow------------------------------------
# Producer (API) : pushes logs to Redis stream logs:queue.
# Celery Master Task (process_log_master) : reads ~500 logs from Redis and splits them into 5 parallel subtasks of 100 each.
# Celery Subtasks (process_log_chunk) → each subtask handles inserts concurrently using bulk_create.


@shared_task(bind=True)
def process_log_chunk(self, entries):
    """Subtask that processes a batch of decoded log entries."""
    try:
        app_ids = {int(e["application_id"]) for e in entries}
        apps = {a.id: a for a in Application.objects.filter(id__in=app_ids)}

        to_create = []
        for e in entries:
            app = apps.get(int(e["application_id"]))
            if not app:
                continue
            to_create.append(
                LogEntry(
                    application=app,
                    timestamp=datetime.fromisoformat(e["timestamp"]),
                    level=e["level"].upper(),
                    message=e["message"],
                    metadata=json.loads(e.get("metadata", "{}")),
                )
            )

        if to_create:
            with transaction.atomic():
                LogEntry.objects.bulk_create(to_create, batch_size=100)
            logger.info(f"Inserted {len(to_create)} logs in chunk")
        return len(to_create)

    except Exception as exc:
        logger.error(f"Error in process_log_chunk: {exc}")
        return 0

@shared_task(bind=True, max_retries=3, default_retry_delay=5)
def process_log_master(self, consumer_name="worker-1", batch_size=500):
    """
    queue based log inngestion using redis streams
    """

    redis_client = caches["log_queue"].client.get_client()
    try:
        stream_name = "logs:queue"
        group_name = "log_consumers"
        
        # Ensure the consumer group exists
        try:
            redis_client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
        except Exception as e:
            # Group might already exist, ignore error if it's about group existence
            if "BUSYGROUP" not in str(e):
                logger.error(f"Error creating Redis consumer group {group_name} for stream {stream_name}: {e}")
                raise

        # read upto batchsize entries from queue
        entries = redis_client.xreadgroup(
            groupname=group_name,
            consumername=consumer_name, # This consumer_name should ideally be unique per worker instance
            streams={stream_name: ">"},
            count=batch_size,
            block=2000,
        )

        if not entries:
            logger.info("no logs in queue")
            return "Idle"
        
        decoded_entries = []
        message_ids = []

        # flatten queue responses
        for _, msgs in entries:
            for msg_id, data in msgs:
                message_ids.append(msg_id)
                try:
                    decoded_entries.append({
                        "application_id": data[b"application_id"].decode(),
                        "timestamp": data[b"timestamp"].decode(),
                        "level": data[b"level"].decode(),
                        "message": data[b"message"].decode(),
                        "metadata": data[b"metadata"].decode(),
                    })
                except Exception as e:
                    logger.warning(f"Bad entry skipped: {e}")
                    redis_client.xack(stream_name, group_name, msg_id)

        if not decoded_entries:
            return "no valid logs"

        # Divide into chunks of 100 logs
        chunk_size = 100
        chunks = [decoded_entries[i:i + chunk_size] for i in range(0, len(decoded_entries), chunk_size)]

        # Launch subtasks in parallel
        job = group(process_log_chunk.s(chunk) for chunk in chunks)
        result = job.apply_async()

        redis_client.xack(stream_name, group_name, *message_ids)

        return f"Spawned {len(chunks)} subtasks"

    except Exception as exc:
        logger.error(f"Master task error: {exc}")
        raise self.retry(exc=exc)