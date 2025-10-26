import json
import logging
import os
import uuid
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
def process_log_chunk(self, entries, stream_name, group_name, message_ids):
    """Subtask that processes a batch of decoded log entries."""
    try:
        app_ids = {int(e["application_id"]) for e in entries}
        # Fetch all unique applications in a single query
        apps = {a.id: a for a in Application.objects.filter(id__in=app_ids)}

        to_create = []
        for e in entries:
            app = apps.get(int(e["application_id"]))
            if not app:
                continue
            # Create LogEntry model instances
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
            
            # Acknowledge messages only after successful database insertion
            try:
                redis_client = caches["log_queue"].client.get_client()
                redis_client.xack(stream_name, group_name, *message_ids)
                logger.info(f"Successfully processed and acknowledged {len(to_create)} logs.")
            except Exception as e:
                # If ack fails, logs might be reprocessed, which is acceptable for at-least-once delivery.
                logger.error(f"DB insert succeeded, but Redis XACK failed for {len(message_ids)} messages: {e}")

        return len(to_create)

    except Exception as exc:
        logger.error(f"Error in process_log_chunk: {exc}")
        # Do not acknowledge messages if processing failed. They will be re-processed later.
        return 0

@shared_task(bind=True, max_retries=3, default_retry_delay=5)
def process_log_master(self, batch_size=500):
    """
    queue based log inngestion using redis streams
    """

    redis_client = caches["log_queue"].client.get_client()
    try:
        stream_name = "logs:queue"
        group_name = "log_consumers"
        
        # Create a unique consumer name for each worker process to ensure true parallel processing.
        # Combining hostname with PID is robust for multi-process workers on a single machine.
        try:
            worker_hostname = self.request.hostname
            process_id = os.getpid()
            consumer_name = f"{worker_hostname}-{process_id}"
        except Exception as e:
            logger.warning(f"Could not reliably determine hostname/PID: {e}. Using UUID fallback.")
            consumer_name = f"worker-{uuid.uuid4().hex}"

        lock_key = f"lock:create-group:{stream_name}:{group_name}"

        # Use a Redis lock to prevent a race condition on group creation
        # when multiple workers start at the same time.
        with redis_client.lock(lock_key, timeout=10):
            try:
                # Check if group exists by getting group info
                redis_client.xinfo_ipgroups(stream_name)
            except Exception:
                # If xinfo_groups fails, the stream or group likely doesn't exist.
                # It's safe to try and create it.
                logger.info(f"Consumer group '{group_name}' or stream '{stream_name}' not found. Attempting to create.")
                redis_client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
                logger.info(f"Successfully created consumer group '{group_name}'.")

        # read upto batchsize entries from queue
        entries = redis_client.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={stream_name: ">"},
            count=batch_size,
            block=2000,
        )

        if not entries:
            logger.info("no logs in queue")
            return "Idle"
        
        # Group messages and their IDs together for chunking
        entries_with_ids = []

        # flatten queue responses
        for _, msgs in entries:
            for msg_id, data in msgs:
                try:
                    decoded_entry = {
                            "application_id": data[b"application_id"].decode(),
                            "timestamp": data[b"timestamp"].decode(),
                            "level": data[b"level"].decode(),
                            "message": data[b"message"].decode(),
                            "metadata": data[b"metadata"].decode(),
                        }
                    entries_with_ids.append({'id': msg_id, 'data': decoded_entry})
                except Exception as e:
                    logger.warning(f"Bad entry skipped: {e}")
                    # Acknowledge malformed messages so they don't block the stream
                    redis_client.xack(stream_name, group_name, msg_id)

        if not entries_with_ids:
            return "no valid logs"

        # Divide into chunks of 100 logs
        chunk_size = 100
        chunks_of_entries = [entries_with_ids[i:i + chunk_size] for i in range(0, len(entries_with_ids), chunk_size)]

        # Launch subtasks in parallel
        job = group(process_log_chunk.s([item['data'] for item in chunk], stream_name, group_name, [item['id'] for item in chunk])
                    for chunk in chunks_of_entries)
        result = job.apply_async()

        return f"Spawned {len(chunks_of_entries)} subtasks to process {len(entries_with_ids)} logs."

    except Exception as exc:
        logger.error(f"Master task error: {exc}")
        raise self.retry(exc=exc)