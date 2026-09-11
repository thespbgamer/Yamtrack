import logging
from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone

from app import metadata
from app.models import UserMessage

logger = logging.getLogger(__name__)


@shared_task(name="Cleanup user messages")
def cleanup_user_messages():
    """Delete shown user messages older than the configured retention window."""
    cutoff = timezone.now() - timedelta(days=settings.USER_MESSAGE_RETENTION_DAYS)
    deleted_count, _ = UserMessage.objects.filter(
        shown_at__isnull=False,
        shown_at__lt=cutoff,
    ).delete()

    logger.info("Deleted %s old shown user messages.", deleted_count)

    return deleted_count


@shared_task(bind=True, name=metadata.METADATA_SYNC_TASK_NAME)
def sync_tracked_metadata(self, user_id, media_type=None, force=False):  # noqa: FBT002
    """Refresh provider metadata for a user's tracked items."""
    user = get_user_model().objects.get(id=user_id)
    result = metadata.sync_tracked_items(user, media_type, force=force)
    payload = metadata.format_sync_task_result(
        result["synced"],
        result["failed"],
        result["skipped"],
        result["errors"],
    )
    metadata.record_sync_run(
        user,
        result,
        media_type=media_type,
        force=force,
        task_id=getattr(self.request, "id", None) or "",
    )
    logger.info(
        "Metadata sync for user %s finished: %s",
        user_id,
        payload["summary"],
    )
    if payload["errors"]:
        logger.info(
            "Metadata sync failures for user %s: %s",
            user_id,
            "; ".join(
                metadata.format_failed_item_line(item) for item in payload["errors"]
            ),
        )
    return payload
