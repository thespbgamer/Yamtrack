import json
from unittest.mock import patch

from celery import states
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import TestCase
from django_celery_results.models import TaskResult

from app import metadata
from app.models import (
    TV,
    Item,
    MediaTypes,
    Movie,
    Season,
    Sources,
    Status,
)
from app.providers import services
from events.tasks import reload_calendar


class MetadataSyncItemTests(TestCase):
    """Test per-item metadata sync behavior."""

    def setUp(self):
        """Create a user and a movie item."""
        self.user = get_user_model().objects.create_user(
            username="test",
            password="12345",  # noqa: S106
        )
        self.item = Item.objects.create(
            media_id="238",
            source=Sources.TMDB.value,
            media_type=MediaTypes.MOVIE.value,
            title="Old Title",
            image="http://example.com/old.jpg",
        )
        self.metadata_patcher = patch(
            "app.providers.services.get_media_metadata",
            return_value={
                "title": "The Godfather",
                "image": "http://example.com/new.jpg",
                "max_progress": 1,
            },
        )
        self.mock_get_metadata = self.metadata_patcher.start()
        self.addCleanup(self.metadata_patcher.stop)

    def test_manual_source_raises(self):
        """Manual items cannot be synced."""
        with self.assertRaises(metadata.ManualSourceError):
            metadata.sync_item_metadata(
                Sources.MANUAL.value,
                MediaTypes.MOVIE.value,
                "manual-id",
            )

    def test_recently_synced_raises_when_check_recent(self):
        """Skip a refresh when the cache was written moments ago."""
        with (
            patch("app.metadata.cache.ttl", return_value=settings.CACHE_TIMEOUT),
            self.assertRaises(metadata.RecentlySyncedError),
        ):
            metadata.sync_item_metadata(
                self.item.source,
                self.item.media_type,
                self.item.media_id,
            )

        self.item.refresh_from_db()
        self.assertEqual(self.item.title, "Old Title")

    @patch("app.models.Item.fetch_releases")
    def test_recent_check_skipped_when_disabled(self, mock_fetch_releases):
        """Force a refresh when check_recent is false."""
        with patch("app.metadata.cache.ttl", return_value=settings.CACHE_TIMEOUT):
            item, title = metadata.sync_item_metadata(
                self.item.source,
                self.item.media_type,
                self.item.media_id,
                check_recent=False,
            )

        item.refresh_from_db()
        self.assertEqual(item.title, "The Godfather")
        self.assertEqual(item.image, "http://example.com/new.jpg")
        self.assertEqual(title, "The Godfather")
        mock_fetch_releases.assert_called_once_with(delay=False)

    @patch("app.models.Item.fetch_releases")
    def test_sync_updates_title_and_image(self, mock_fetch_releases):
        """Persist refreshed title and image from the provider."""
        item, title = metadata.sync_item_metadata(
            self.item.source,
            self.item.media_type,
            self.item.media_id,
        )

        item.refresh_from_db()
        self.assertEqual(item.title, "The Godfather")
        self.assertEqual(item.image, "http://example.com/new.jpg")
        self.assertEqual(title, "The Godfather")
        mock_fetch_releases.assert_called_once_with(delay=False)


class MetadataTrackedItemsTests(TestCase):
    """Test selecting and bulk-syncing tracked items."""

    def setUp(self):
        """Create users and tracked media across types."""
        self.user = get_user_model().objects.create_user(
            username="test",
            password="12345",  # noqa: S106
        )
        self.other_user = get_user_model().objects.create_user(
            username="other",
            password="12345",  # noqa: S106
        )
        self.metadata_patcher = patch(
            "app.providers.services.get_media_metadata",
            return_value={
                "title": "Meta",
                "image": "http://example.com/meta.jpg",
                "max_progress": 1,
                "episodes": [],
            },
        )
        self.mock_get_metadata = self.metadata_patcher.start()
        self.addCleanup(self.metadata_patcher.stop)

        self.movie_item = Item.objects.create(
            media_id="238",
            source=Sources.TMDB.value,
            media_type=MediaTypes.MOVIE.value,
            title="Movie",
            image="http://example.com/movie.jpg",
        )
        movie = Movie(
            item=self.movie_item,
            user=self.user,
            status=Status.COMPLETED.value,
        )
        Movie.save_base(movie)

        self.other_movie_item = Item.objects.create(
            media_id="239",
            source=Sources.TMDB.value,
            media_type=MediaTypes.MOVIE.value,
            title="Other Movie",
            image="http://example.com/other.jpg",
        )
        other_movie = Movie(
            item=self.other_movie_item,
            user=self.other_user,
            status=Status.COMPLETED.value,
        )
        Movie.save_base(other_movie)

        self.manual_item = Item.objects.create(
            media_id="manual-1",
            source=Sources.MANUAL.value,
            media_type=MediaTypes.MOVIE.value,
            title="Manual Movie",
            image="http://example.com/manual.jpg",
        )
        manual_movie = Movie(
            item=self.manual_item,
            user=self.user,
            status=Status.COMPLETED.value,
        )
        Movie.save_base(manual_movie)

        tv_item = Item.objects.create(
            media_id="1668",
            source=Sources.TMDB.value,
            media_type=MediaTypes.TV.value,
            title="Friends",
            image="http://example.com/tv.jpg",
        )
        self.tv = TV(
            item=tv_item,
            user=self.user,
            status=Status.IN_PROGRESS.value,
        )
        TV.save_base(self.tv)

        self.season_item = Item.objects.create(
            media_id="1668",
            source=Sources.TMDB.value,
            media_type=MediaTypes.SEASON.value,
            title="Friends",
            image="http://example.com/season.jpg",
            season_number=1,
        )
        season = Season(
            item=self.season_item,
            user=self.user,
            related_tv=self.tv,
            status=Status.IN_PROGRESS.value,
        )
        Season.save_base(season)

        self.episode_item = Item.objects.create(
            media_id="1668",
            source=Sources.TMDB.value,
            media_type=MediaTypes.EPISODE.value,
            title="Friends",
            image="http://example.com/ep.jpg",
            season_number=1,
            episode_number=1,
        )
        cache.clear()
        reload_calendar.delay.reset_mock()

    def test_all_tracking_excludes_other_users_manual_and_episodes(self):
        """All tracking returns this user's non-manual, non-episode items."""
        items = set(metadata.get_tracked_items(self.user))

        self.assertIn(self.movie_item, items)
        self.assertIn(self.season_item, items)
        self.assertIn(self.tv.item, items)
        self.assertNotIn(self.other_movie_item, items)
        self.assertNotIn(self.manual_item, items)
        self.assertNotIn(self.episode_item, items)

    def test_category_filter_limits_to_one_media_type(self):
        """A category scope only returns that media type."""
        items = set(metadata.get_tracked_items(self.user, MediaTypes.MOVIE.value))

        self.assertEqual(items, {self.movie_item})

    def test_invalid_media_type_returns_empty(self):
        """Unknown or unsyncable types produce an empty queryset."""
        self.assertEqual(
            list(metadata.get_tracked_items(self.user, MediaTypes.EPISODE.value)),
            [],
        )

    def test_tracked_item_counts(self):
        """Counts include all tracking and exclude other users and manuals."""
        counts = metadata.get_tracked_item_counts(self.user)

        self.assertEqual(counts[MediaTypes.MOVIE.value], 1)
        self.assertEqual(counts[MediaTypes.TV.value], 1)
        self.assertEqual(counts[MediaTypes.SEASON.value], 1)
        self.assertEqual(counts[MediaTypes.ANIME.value], 0)
        self.assertEqual(counts[metadata.ALL_TRACKING_SCOPE], 3)
        self.assertNotIn(MediaTypes.EPISODE.value, counts)

    @patch("app.metadata.tmdb.process_episodes")
    def test_bulk_sync_updates_items_and_season_episodes(self, mock_process_episodes):
        """Bulk sync updates titles and existing season episode images."""
        mock_process_episodes.return_value = [
            {
                "episode_number": 1,
                "image": "http://example.com/ep-new.jpg",
            },
        ]
        self.mock_get_metadata.return_value = {
            "title": "Updated",
            "image": "http://example.com/updated.jpg",
            "episodes": [],
        }

        result = metadata.sync_tracked_items(self.user)

        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["skipped"], 0)
        self.assertGreaterEqual(result["synced"], 2)
        self.movie_item.refresh_from_db()
        self.season_item.refresh_from_db()
        self.episode_item.refresh_from_db()
        self.assertEqual(self.movie_item.title, "Updated")
        self.assertEqual(self.season_item.image, "http://example.com/updated.jpg")
        self.assertEqual(self.episode_item.title, "Updated")
        self.assertEqual(self.episode_item.image, "http://example.com/ep-new.jpg")
        reload_calendar.delay.assert_called_once_with(user=self.user)

    def test_bulk_sync_continues_after_provider_error(self):
        """One failed item does not prevent the rest from syncing."""

        def side_effect(media_type, media_id, _source, _season_numbers=None):
            if media_id == "238" and media_type == MediaTypes.MOVIE.value:
                error = Exception("provider down")
                raise services.ProviderAPIError(Sources.TMDB.value, error)
            return {
                "title": "Updated",
                "image": "http://example.com/updated.jpg",
                "episodes": [],
            }

        self.mock_get_metadata.side_effect = side_effect

        with patch("app.metadata.tmdb.process_episodes", return_value=[]):
            result = metadata.sync_tracked_items(self.user)

        self.assertEqual(result["failed"], 1)
        self.assertGreaterEqual(result["synced"], 1)
        self.assertEqual(result["errors"][0]["title"], "Movie")
        self.assertIn("The Movie Database", result["errors"][0]["reason"])
        self.movie_item.refresh_from_db()
        self.season_item.refresh_from_db()
        self.assertEqual(self.movie_item.title, "Movie")
        self.assertEqual(self.season_item.title, "Updated")

    def test_category_sync_does_not_touch_other_types(self):
        """Syncing movies leaves season metadata unchanged."""
        self.mock_get_metadata.return_value = {
            "title": "Updated Movie",
            "image": "http://example.com/movie-new.jpg",
        }

        result = metadata.sync_tracked_items(self.user, MediaTypes.MOVIE.value)

        self.assertEqual(
            result,
            {"synced": 1, "skipped": 0, "failed": 0, "errors": []},
        )
        self.movie_item.refresh_from_db()
        self.season_item.refresh_from_db()
        self.assertEqual(self.movie_item.title, "Updated Movie")
        self.assertEqual(self.season_item.title, "Friends")

    def test_bulk_skips_items_synced_in_the_last_hour(self):
        """A successful sync is not repeated until the skip window expires."""
        self.mock_get_metadata.return_value = {
            "title": "Updated Movie",
            "image": "http://example.com/movie-new.jpg",
        }

        first = metadata.sync_tracked_items(self.user, MediaTypes.MOVIE.value)
        self.mock_get_metadata.reset_mock()
        second = metadata.sync_tracked_items(self.user, MediaTypes.MOVIE.value)

        self.assertEqual(first["synced"], 1)
        self.assertEqual(second, {"synced": 0, "skipped": 1, "failed": 0, "errors": []})
        self.mock_get_metadata.assert_not_called()

    def test_force_sync_ignores_recent_skip_window(self):
        """Force re-syncs items that were synced in the last hour."""
        self.mock_get_metadata.return_value = {
            "title": "Updated Movie",
            "image": "http://example.com/movie-new.jpg",
        }

        metadata.sync_tracked_items(self.user, MediaTypes.MOVIE.value)
        self.mock_get_metadata.reset_mock()
        forced = metadata.sync_tracked_items(
            self.user,
            MediaTypes.MOVIE.value,
            force=True,
        )

        self.assertEqual(forced, {"synced": 1, "skipped": 0, "failed": 0, "errors": []})
        self.mock_get_metadata.assert_called_once()

    def test_failed_items_are_listed_and_can_be_retried(self):
        """Failures record the item title and are not treated as recent successes."""
        error = Exception("provider down")
        self.mock_get_metadata.side_effect = services.ProviderAPIError(
            Sources.TMDB.value,
            error,
        )

        result = metadata.sync_tracked_items(self.user, MediaTypes.MOVIE.value)

        self.assertEqual(result["synced"], 0)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(result["errors"][0]["title"], "Movie")
        self.assertEqual(result["errors"][0]["media_label"], "Movie")
        self.assertIn("The Movie Database", result["errors"][0]["reason"])
        self.assertNotIn("logs", result["errors"][0]["reason"])
        self.assertFalse(
            metadata.was_metadata_synced_recently(
                self.movie_item.source,
                self.movie_item.media_type,
                self.movie_item.media_id,
            ),
        )


class MetadataNotifyTests(TestCase):
    """Test user-facing bulk sync summaries."""

    def test_format_messages(self):
        """Summaries cover success, empty, and partial failure."""
        self.assertEqual(
            metadata.format_sync_result_message(1, 0),
            "Synced metadata for 1 item.",
        )
        self.assertEqual(
            metadata.format_sync_result_message(2, 0),
            "Synced metadata for 2 items.",
        )
        self.assertEqual(
            metadata.format_sync_result_message(0, 0),
            "No tracked items to sync.",
        )
        self.assertEqual(
            metadata.format_sync_result_message(1, 1),
            "Synced metadata for 1 item. 1 item failed.",
        )
        self.assertEqual(
            metadata.format_sync_result_message(2, 1, skipped=3),
            (
                "Synced metadata for 2 items. "
                "Skipped 3 recently synced items. "
                "1 item failed."
            ),
        )
        self.assertEqual(
            metadata.format_sync_result_message(0, 0, skipped=2),
            "Skipped 2 recently synced items.",
        )
        self.assertEqual(
            metadata.empty_sync_scope_message(),
            "You have no tracked items to sync.",
        )
        self.assertEqual(
            metadata.empty_sync_scope_message(MediaTypes.TV.value),
            "You have no tracked items in the selected category.",
        )
        self.assertEqual(
            metadata.format_sync_task_result(
                synced=1,
                failed=1,
                errors=["Movie: provider down"],
            ),
            {
                "summary": "Synced metadata for 1 item. 1 item failed.",
                "errors": [
                    {
                        "title": "Movie",
                        "media_type": "",
                        "media_label": "",
                        "reason": "provider down",
                    },
                ],
                "synced": 1,
                "failed": 1,
                "skipped": 0,
            },
        )


class MetadataSyncProgressTests(TestCase):
    """Test detecting an unfinished bulk metadata sync."""

    def setUp(self):
        """Create users for progress lookups."""
        self.user = get_user_model().objects.create_user(username="test")
        self.other_user = get_user_model().objects.create_user(username="other")

    def _create_result(self, user, status, task_id="sync-task"):
        """Store a celery metadata-sync result."""
        return TaskResult.objects.create(
            task_id=task_id,
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {user.id}, 'media_type': None}}",
            status=status,
            result="",
        )

    def test_in_progress_for_unfinished_statuses(self):
        """Queued and running states count as in progress."""
        for index, status in enumerate(metadata.METADATA_SYNC_IN_PROGRESS_STATUSES):
            with self.subTest(status=status):
                self._create_result(self.user, status, task_id=f"open-{index}")
                self.assertTrue(metadata.is_metadata_sync_in_progress(self.user))
                TaskResult.objects.all().delete()

    def test_not_in_progress_after_success_or_failure(self):
        """Finished states allow another sync to start."""
        self._create_result(self.user, states.SUCCESS, task_id="ok")
        self.assertFalse(metadata.is_metadata_sync_in_progress(self.user))

        TaskResult.objects.all().delete()
        self._create_result(self.user, states.FAILURE, task_id="fail")
        self.assertFalse(metadata.is_metadata_sync_in_progress(self.user))

    def test_other_users_tasks_are_ignored(self):
        """Only the current user's unfinished sync is considered."""
        self._create_result(self.other_user, states.STARTED, task_id="other")
        self.assertFalse(metadata.is_metadata_sync_in_progress(self.user))

    def test_history_includes_structured_failed_items(self):
        """Dict celery payloads expose failed item titles and reasons."""
        payload = metadata.format_sync_task_result(
            synced=1,
            failed=1,
            errors=[
                {
                    "title": "The Godfather",
                    "media_type": MediaTypes.MOVIE.value,
                    "media_label": "Movie",
                    "reason": "The Movie Database API network error.",
                },
            ],
        )
        TaskResult.objects.create(
            task_id="hist-structured",
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {self.user.id}, 'media_type': 'movie'}}",
            status=states.SUCCESS,
            result=json.dumps(payload),
        )

        history = metadata.get_metadata_sync_history(self.user)

        self.assertEqual(history[0]["failed_items"][0]["title"], "The Godfather")
        self.assertEqual(history[0]["failed_items"][0]["media_label"], "Movie")
        self.assertIn("The Movie Database", history[0]["failed_items"][0]["reason"])

    def test_history_reads_legacy_string_error_details(self):
        """Older string celery results still expose failed item names."""
        payload = (
            "Synced metadata for 1 item. 1 item failed."
            f"{metadata.METADATA_SYNC_ERROR_TITLE}Movie: provider down"
        )
        TaskResult.objects.create(
            task_id="hist-legacy",
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {self.user.id}, 'media_type': 'movie'}}",
            status=states.SUCCESS,
            result=json.dumps(payload),
        )

        history = metadata.get_metadata_sync_history(self.user)

        self.assertEqual(history[0]["failed_items"][0]["title"], "Movie")
        self.assertEqual(history[0]["failed_items"][0]["reason"], "provider down")

    def test_history_prefers_saved_run_failed_items(self):
        """Failed items persist even when the celery result is only a summary."""
        task = TaskResult.objects.create(
            task_id="hist-run",
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {self.user.id}, 'media_type': None}}",
            status=states.SUCCESS,
            result=json.dumps("Synced metadata for 788 items. 4 items failed."),
        )
        metadata.record_sync_run(
            self.user,
            {
                "synced": 788,
                "failed": 1,
                "skipped": 0,
                "errors": [
                    {
                        "title": "Example Show",
                        "media_type": MediaTypes.TV.value,
                        "media_label": "TV Show",
                        "reason": "The Movie Database API network error.",
                    },
                ],
            },
            task_id=task.task_id,
        )

        history = metadata.get_metadata_sync_history(self.user)

        self.assertEqual(history[0]["failed_items"][0]["title"], "Example Show")
        self.assertEqual(
            history[0]["failed_items"][0]["reason"],
            "The Movie Database API network error.",
        )

    def test_history_strips_log_hints_from_saved_errors(self):
        """Stored provider errors do not tell the user to check logs."""
        payload = metadata.format_sync_task_result(
            synced=1,
            failed=1,
            errors=[
                {
                    "title": "Example Show",
                    "media_label": "TV Show",
                    "reason": (
                        "There was an error contacting the The Movie Database API "
                        "(HTTP 404). Check the logs for more details."
                    ),
                },
            ],
        )
        TaskResult.objects.create(
            task_id="hist-logs",
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {self.user.id}, 'media_type': None}}",
            status=states.SUCCESS,
            result=json.dumps(payload),
        )

        history = metadata.get_metadata_sync_history(self.user)

        reason = history[0]["failed_items"][0]["reason"]
        self.assertIn("HTTP 404", reason)
        self.assertNotIn("logs", reason)

    def test_history_marks_forced_syncs(self):
        """Force syncs are labeled in history."""
        payload = json.dumps("Synced metadata for 1 item.")
        TaskResult.objects.create(
            task_id="forced",
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=(
                f"{{'user_id': {self.user.id}, 'media_type': None, 'force': True}}"
            ),
            status=states.SUCCESS,
            result=payload,
        )

        history = metadata.get_metadata_sync_history(self.user)

        self.assertEqual(history[0]["scope"], "All tracking (forced)")

    def test_history_ignores_other_users(self):
        """History only includes the current user's metadata syncs."""
        payload = json.dumps("Synced metadata for 1 item.")
        TaskResult.objects.create(
            task_id="mine",
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {self.user.id}, 'media_type': None}}",
            status=states.SUCCESS,
            result=payload,
        )
        TaskResult.objects.create(
            task_id="theirs",
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {self.other_user.id}, 'media_type': None}}",
            status=states.SUCCESS,
            result=payload,
        )

        history = metadata.get_metadata_sync_history(self.user)

        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["scope"], "All tracking")
