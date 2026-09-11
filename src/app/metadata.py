import ast
import json
import logging

from celery import states
from django.conf import settings
from django.core.cache import cache
from django.db.models import Count, Q
from django_celery_results.models import TaskResult

import events
from app.mixins import disable_fetch_releases
from app.models import Item, MediaTypes, MetadataSyncRun, Sources
from app.providers import services, tmdb

logger = logging.getLogger(__name__)

ALL_TRACKING_SCOPE = "all"
RECENT_SYNC_WINDOW_SECONDS = 3
SUCCESS_SKIP_WINDOW_SECONDS = 60 * 60
METADATA_SYNC_TASK_NAME = "Sync tracked metadata"
METADATA_SYNC_ERROR_TITLE = "\nCouldn't sync the following items:\n"
METADATA_SYNC_IN_PROGRESS_STATUSES = {
    states.PENDING,
    states.STARTED,
    states.RETRY,
    states.RECEIVED,
}
SYNC_IN_PROGRESS_MESSAGE = "A metadata sync is already in progress."


class ManualSourceError(Exception):
    """Raised when attempting to sync a manual-source item."""

    def __init__(self):
        """Initialize the exception."""
        super().__init__("Manual items cannot be synced.")


class RecentlySyncedError(Exception):
    """Raised when metadata was synced too recently to refresh again."""

    def __init__(self):
        """Initialize the exception."""
        super().__init__("The data was recently synced, please wait a few seconds.")


def get_syncable_media_types():
    """Return media types that can be bulk-synced (everything except episodes)."""
    return [value for value in MediaTypes.values if value != MediaTypes.EPISODE.value]


def get_metadata_sync_choices():
    """Return dropdown choices for the advanced metadata sync form."""
    return [
        (ALL_TRACKING_SCOPE, "All tracking"),
        *[
            (value, label)
            for value, label in MediaTypes.choices
            if value != MediaTypes.EPISODE.value
        ],
    ]


def get_valid_metadata_sync_scopes():
    """Return allowed POST values for the metadata sync scope."""
    return {ALL_TRACKING_SCOPE, *get_syncable_media_types()}


def get_tracked_item_counts(user):
    """Return tracked item counts keyed by sync scope, including all tracking."""
    counts = dict.fromkeys(get_syncable_media_types(), 0)
    counts[ALL_TRACKING_SCOPE] = 0

    type_totals = (
        get_tracked_items(user).values("media_type").annotate(total=Count("pk"))
    )
    for row in type_totals:
        counts[row["media_type"]] = row["total"]
        counts[ALL_TRACKING_SCOPE] += row["total"]

    return counts


def get_metadata_sync_choice_data(user):
    """Return dropdown choices with tracked item counts for the current user."""
    counts = get_tracked_item_counts(user)
    return [
        {"value": value, "label": label, "count": counts[value]}
        for value, label in get_metadata_sync_choices()
    ]


def empty_sync_scope_message(media_type=None):
    """Return an error message when a sync scope has no tracked items."""
    if media_type is None:
        return "You have no tracked items to sync."
    return "You have no tracked items in the selected category."


def get_in_progress_metadata_sync(user):
    """Return the user's latest unfinished metadata sync task, if any."""
    return (
        TaskResult.objects.filter(
            task_name=METADATA_SYNC_TASK_NAME,
            status__in=METADATA_SYNC_IN_PROGRESS_STATUSES,
            task_kwargs__contains=f"'user_id': {user.id},",
        )
        .order_by("-date_created")
        .first()
    )


def is_metadata_sync_in_progress(user):
    """Return whether the user already has a metadata sync queued or running."""
    return get_in_progress_metadata_sync(user) is not None


def get_metadata_cache_key(source, media_type, media_id, season_number=None):
    """Return the provider cache key used for a media item."""
    cache_key = f"{source}_{media_type}_{media_id}"
    if media_type == MediaTypes.SEASON.value:
        cache_key += f"_{season_number}"
    return cache_key


def get_success_sync_cache_key(source, media_type, media_id, season_number=None):
    """Return the cache key that marks a successful metadata sync."""
    item_key = get_metadata_cache_key(source, media_type, media_id, season_number)
    return f"metadata_sync_ok_{item_key}"


def mark_metadata_sync_success(source, media_type, media_id, season_number=None):
    """Remember a successful sync so bulk jobs can skip it for an hour."""
    cache.set(
        get_success_sync_cache_key(source, media_type, media_id, season_number),
        1,
        timeout=SUCCESS_SKIP_WINDOW_SECONDS,
    )


def was_metadata_synced_recently(source, media_type, media_id, season_number=None):
    """Return whether this item was synced successfully within the skip window."""
    return bool(
        cache.get(
            get_success_sync_cache_key(source, media_type, media_id, season_number),
        ),
    )


def get_item_display_title(item):
    """Return a user-facing title for history and error lines."""
    return str(item)


def get_media_type_label(media_type):
    """Return a readable label for a media type value."""
    try:
        return MediaTypes(media_type).label
    except ValueError:
        return media_type


LOGS_HINT_SUFFIXES = (
    " Check the logs for more details.",
    " Check the logs for more details",
)


def user_facing_error_reason(text):
    """Return an error message suitable for history, without log hints."""
    reason = str(text).strip()
    for suffix in LOGS_HINT_SUFFIXES:
        if reason.endswith(suffix):
            reason = reason[: -len(suffix)].rstrip()
            break
    return reason


def format_item_sync_error(item, exc):
    """Return a structured failure for history: title, type, and reason."""
    return {
        "title": get_item_display_title(item),
        "media_type": item.media_type,
        "media_label": get_media_type_label(item.media_type),
        "reason": user_facing_error_reason(exc) or type(exc).__name__,
    }


def _error_line_to_item(line):
    """Parse a legacy 'Title: reason' error line into a failed-item dict."""
    title, separator, reason = line.partition(": ")
    if separator:
        return {
            "title": title,
            "media_type": "",
            "media_label": "",
            "reason": user_facing_error_reason(reason),
        }
    return {
        "title": line,
        "media_type": "",
        "media_label": "",
        "reason": "",
    }


def normalize_sync_errors(errors):
    """Normalize stored sync errors into failed-item dicts."""
    if not errors:
        return []
    if isinstance(errors, str):
        lines = [line.strip() for line in errors.splitlines() if line.strip()]
        return [_error_line_to_item(line) for line in lines]

    items = []
    for error in errors:
        if isinstance(error, dict):
            title = str(error.get("title") or "").strip()
            reason = user_facing_error_reason(error.get("reason") or "")
            if not title and not reason:
                continue
            items.append(
                {
                    "title": title or "Unknown item",
                    "media_type": str(error.get("media_type") or ""),
                    "media_label": str(error.get("media_label") or ""),
                    "reason": reason or "Unknown error",
                },
            )
        else:
            line = str(error).strip()
            if line:
                items.append(_error_line_to_item(line))
    return items


def format_failed_item_line(item):
    """Return a single-line description of a failed item."""
    title = item.get("title") or "Unknown item"
    media_label = item.get("media_label") or ""
    reason = item.get("reason") or ""
    heading = f"{title} ({media_label})" if media_label else title
    if reason:
        return f"{heading}: {reason}"
    return heading


def sync_item_metadata(
    source,
    media_type,
    media_id,
    season_number=None,
    *,
    check_recent=True,
    fetch_releases=True,
):
    """Refresh title and image metadata from the provider.

    Returns:
        tuple[Item, str]: The updated item and a display title.

    Raises:
        ManualSourceError: If the item is a manual entry.
        RecentlySyncedError: If the item was synced within the recent window.
    """
    if source == Sources.MANUAL.value:
        raise ManualSourceError

    cache_key = get_metadata_cache_key(source, media_type, media_id, season_number)
    ttl = cache.ttl(cache_key)
    logger.debug("%s - Cache TTL for: %s", cache_key, ttl)

    if (
        check_recent
        and ttl is not None
        and ttl > (settings.CACHE_TIMEOUT - RECENT_SYNC_WINDOW_SECONDS)
    ):
        raise RecentlySyncedError

    deleted = cache.delete(cache_key)
    logger.debug("%s - Old cache deleted: %s", cache_key, deleted)

    metadata = services.get_media_metadata(
        media_type,
        media_id,
        source,
        [season_number],
    )
    item, _ = Item.objects.update_or_create(
        media_id=media_id,
        source=source,
        media_type=media_type,
        season_number=season_number,
        defaults={
            "title": metadata["title"],
            "image": metadata["image"],
        },
    )
    title = metadata["title"]
    if season_number:
        title += f" - Season {season_number}"

    if media_type == MediaTypes.SEASON.value:
        _update_season_episodes(source, media_id, season_number, metadata, title)

    if fetch_releases:
        item.fetch_releases(delay=False)

    mark_metadata_sync_success(source, media_type, media_id, season_number)
    return item, title


def _update_season_episodes(source, media_id, season_number, metadata, title):
    """Update existing episode items from freshly fetched season metadata."""
    metadata["episodes"] = tmdb.process_episodes(metadata, [])

    existing_episodes = {
        ep.episode_number: ep
        for ep in Item.objects.filter(
            source=source,
            media_type=MediaTypes.EPISODE.value,
            media_id=media_id,
            season_number=season_number,
        )
    }

    episodes_to_update = []
    episode_count = 0

    for episode_data in metadata["episodes"]:
        episode_number = episode_data["episode_number"]
        if episode_number in existing_episodes:
            episode_item = existing_episodes[episode_number]
            episode_item.title = metadata["title"]
            episode_item.image = episode_data["image"]
            episodes_to_update.append(episode_item)
            episode_count += 1

    logger.info(
        "Found %s existing episodes to update for %s",
        episode_count,
        title,
    )

    if episodes_to_update:
        updated_count = Item.objects.bulk_update(
            episodes_to_update,
            ["title", "image"],
            batch_size=100,
        )
        logger.info(
            "Successfully updated %s episodes for %s",
            updated_count,
            title,
        )


def get_tracked_items(user, media_type=None):
    """Return non-manual items the user tracks, optionally limited to one type."""
    if media_type is None:
        media_types = get_syncable_media_types()
    elif media_type in get_syncable_media_types():
        media_types = [media_type]
    else:
        return Item.objects.none()

    query = Q()
    for media_type_value in media_types:
        query |= Q(**{f"{media_type_value}__user": user})

    return Item.objects.filter(query).exclude(source=Sources.MANUAL.value).distinct()


def sync_tracked_items(user, media_type=None, *, force=False):
    """Sync metadata for a user's tracked items.

    Returns:
        dict: Counts of synced, skipped, and failed items, plus error lines.
    """
    items = list(get_tracked_items(user, media_type))
    synced = 0
    skipped = 0
    failed = 0
    errors = []

    with disable_fetch_releases():
        for item in items:
            if not force and was_metadata_synced_recently(
                item.source,
                item.media_type,
                item.media_id,
                item.season_number,
            ):
                skipped += 1
                continue
            try:
                sync_item_metadata(
                    item.source,
                    item.media_type,
                    item.media_id,
                    item.season_number,
                    check_recent=False,
                    fetch_releases=False,
                )
            except RecentlySyncedError:
                skipped += 1
            except Exception as exc:
                error = format_item_sync_error(item, exc)
                logger.exception("Failed to sync metadata for %s", error["title"])
                failed += 1
                errors.append(error)
            else:
                synced += 1

    if synced:
        events.tasks.reload_calendar.delay(user=user)

    return {
        "synced": synced,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
    }


def format_sync_result_message(synced, failed, skipped=0):
    """Build a user-facing summary for a bulk metadata sync."""
    parts = []
    if synced:
        item_word = "item" if synced == 1 else "items"
        parts.append(f"Synced metadata for {synced} {item_word}.")
    if skipped:
        skip_word = "item" if skipped == 1 else "items"
        parts.append(f"Skipped {skipped} recently synced {skip_word}.")
    if failed:
        fail_word = "item" if failed == 1 else "items"
        parts.append(f"{failed} {fail_word} failed.")
    if not parts:
        return "No tracked items to sync."
    return " ".join(parts)


def format_sync_task_result(synced, failed, skipped=0, errors=None):
    """Build the celery result payload, including failed item details."""
    failed_items = normalize_sync_errors(errors)
    return {
        "summary": format_sync_result_message(synced, failed, skipped),
        "errors": failed_items,
        "synced": synced,
        "failed": failed,
        "skipped": skipped,
    }


def get_sync_scope_label(task_kwargs):
    """Return a display label for the media type passed to a sync task."""
    try:
        kwargs = ast.literal_eval(task_kwargs or "{}")
    except (SyntaxError, ValueError):
        kwargs = {}
    if not isinstance(kwargs, dict):
        kwargs = {}

    media_type = kwargs.get("media_type")
    if not media_type:
        label = "All tracking"
    else:
        label = dict(MediaTypes.choices).get(media_type, media_type)
    if kwargs.get("force"):
        return f"{label} (forced)"
    return label


def _parse_success_sync_result(result):
    """Return summary text and failed items from a stored celery result."""
    if isinstance(result, dict):
        summary = result.get("summary") or format_sync_result_message(
            result.get("synced") or 0,
            result.get("failed") or 0,
            result.get("skipped") or 0,
        )
        return summary, normalize_sync_errors(result.get("errors"))
    if not isinstance(result, str):
        result = str(result)
    parts = result.split(METADATA_SYNC_ERROR_TITLE)
    summary = parts[0].strip()
    failed_items = normalize_sync_errors(parts[1] if len(parts) > 1 else "")
    return summary, failed_items


def process_metadata_sync_task(task):
    """Attach summary and error text for a metadata sync TaskResult."""
    task.failed_items = []
    if task.status == states.FAILURE:
        task.summary = "Unexpected error occurred while processing the task."
        try:
            result_json = json.loads(task.result)
        except (TypeError, json.JSONDecodeError, ValueError):
            result_json = None
        if isinstance(result_json, dict):
            messages = result_json.get("exc_message") or []
            if messages:
                task.summary = messages[0]
        task.errors = task.traceback
    elif task.status in {states.STARTED, states.RETRY}:
        task.summary = "This task is currently running."
        task.errors = None
    elif task.status in {states.PENDING, states.RECEIVED}:
        task.summary = "This task has been queued and is waiting to run."
        task.errors = None
    elif task.status == states.SUCCESS:
        try:
            result = json.loads(task.result)
        except (TypeError, json.JSONDecodeError, ValueError):
            result = task.result or ""
        task.summary, task.failed_items = _parse_success_sync_result(result)
        task.errors = (
            "\n".join(format_failed_item_line(item) for item in task.failed_items)
            or None
        )
    else:
        task.summary = task.status
        task.errors = None
    return task


def record_sync_run(user, result, *, media_type=None, force=False, task_id=""):
    """Persist a completed bulk sync so history can show failed items."""
    return MetadataSyncRun.objects.create(
        user=user,
        task_id=task_id or "",
        media_type=media_type or "",
        force=force,
        synced=result["synced"],
        skipped=result["skipped"],
        failed=result["failed"],
        errors=normalize_sync_errors(result["errors"]),
    )


def get_metadata_sync_history(user):
    """Return recent metadata sync task results for the user."""
    task_results = TaskResult.objects.filter(
        task_name=METADATA_SYNC_TASK_NAME,
        task_kwargs__contains=f"'user_id': {user.id},",
    ).order_by("-date_done")
    runs_by_task_id = {
        run.task_id: run
        for run in MetadataSyncRun.objects.filter(user=user).exclude(task_id="")
    }

    history = []
    for task in task_results:
        processed = process_metadata_sync_task(task)
        failed_items = processed.failed_items
        run = runs_by_task_id.get(task.task_id)
        if run is not None:
            failed_items = normalize_sync_errors(run.errors)
        error_lines = [format_failed_item_line(item) for item in failed_items]
        history.append(
            {
                "task": processed,
                "date": task.date_done,
                "status": task.status,
                "summary": processed.summary,
                "errors": "\n".join(error_lines) or processed.errors,
                "error_lines": error_lines
                or (processed.errors.splitlines() if processed.errors else []),
                "failed_items": failed_items,
                "scope": get_sync_scope_label(task.task_kwargs),
            },
        )
    return history
