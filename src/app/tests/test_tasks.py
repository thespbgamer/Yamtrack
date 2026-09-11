from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils import timezone

from app.models import (
    Item,
    MediaTypes,
    MetadataSyncRun,
    Movie,
    Sources,
    Status,
    UserMessage,
    UserMessageLevel,
)
from app.providers import services
from app.tasks import cleanup_user_messages, sync_tracked_metadata


class CleanupUserMessagesTaskTests(TestCase):
    """Test cleanup of old shown user messages."""

    def setUp(self):
        """Create a user for task tests."""
        self.user = get_user_model().objects.create_user(
            username="test",
        )

    @override_settings(USER_MESSAGE_RETENTION_DAYS=30)
    def test_cleanup_user_messages_deletes_only_old_shown_messages(self):
        """Delete only shown messages older than the retention window."""
        now = timezone.now()
        old_shown = UserMessage.objects.create(
            user=self.user,
            level=UserMessageLevel.INFO,
            message="old shown",
            shown_at=now - timedelta(days=31),
        )
        recent_shown = UserMessage.objects.create(
            user=self.user,
            level=UserMessageLevel.INFO,
            message="recent shown",
            shown_at=now - timedelta(days=5),
        )
        unseen = UserMessage.objects.create(
            user=self.user,
            level=UserMessageLevel.INFO,
            message="unseen",
        )

        deleted_count = cleanup_user_messages()

        self.assertEqual(deleted_count, 1)
        self.assertFalse(UserMessage.objects.filter(id=old_shown.id).exists())
        self.assertTrue(UserMessage.objects.filter(id=recent_shown.id).exists())
        self.assertTrue(UserMessage.objects.filter(id=unseen.id).exists())


class SyncTrackedMetadataTaskTests(TestCase):
    """Test the bulk metadata sync celery task."""

    def setUp(self):
        """Create a user with one tracked movie."""
        self.user = get_user_model().objects.create_user(username="test")
        self.item = Item.objects.create(
            media_id="238",
            source=Sources.TMDB.value,
            media_type=MediaTypes.MOVIE.value,
            title="Old Title",
            image="http://example.com/old.jpg",
        )
        movie = Movie(
            item=self.item,
            user=self.user,
            status=Status.COMPLETED.value,
        )
        Movie.save_base(movie)
        cache.clear()

    @patch("app.models.Item.fetch_releases")
    @patch("app.metadata.services.get_media_metadata")
    def test_task_syncs_items_without_user_message(
        self,
        mock_get_metadata,
        mock_fetch_releases,
    ):
        """The celery task updates items and returns a history summary."""
        mock_get_metadata.return_value = {
            "title": "The Godfather",
            "image": "http://example.com/new.jpg",
            "max_progress": 1,
        }

        message = sync_tracked_metadata(self.user.id)

        self.item.refresh_from_db()
        self.assertEqual(self.item.title, "The Godfather")
        self.assertEqual(
            message,
            {
                "summary": "Synced metadata for 1 item.",
                "errors": [],
                "synced": 1,
                "failed": 0,
                "skipped": 0,
            },
        )
        self.assertFalse(UserMessage.objects.filter(user=self.user).exists())
        mock_fetch_releases.assert_not_called()
        self.assertTrue(
            MetadataSyncRun.objects.filter(user=self.user, synced=1).exists(),
        )

    @patch("app.models.Item.fetch_releases")
    @patch("app.metadata.services.get_media_metadata")
    def test_task_result_includes_failed_item_details(
        self,
        mock_get_metadata,
        mock_fetch_releases,
    ):
        """Failed items are returned with title and reason for history."""
        mock_get_metadata.side_effect = services.ProviderAPIError(
            Sources.TMDB.value,
            Exception("provider down"),
        )

        payload = sync_tracked_metadata(self.user.id)

        self.assertEqual(payload["synced"], 0)
        self.assertEqual(payload["failed"], 1)
        self.assertEqual(payload["errors"][0]["title"], "Old Title")
        self.assertEqual(payload["errors"][0]["media_label"], "Movie")
        self.assertIn("The Movie Database", payload["errors"][0]["reason"])
        self.assertNotIn("logs", payload["errors"][0]["reason"])
        mock_fetch_releases.assert_not_called()
        run = MetadataSyncRun.objects.get(user=self.user)
        self.assertEqual(run.failed, 1)
        self.assertEqual(run.errors[0]["title"], "Old Title")
