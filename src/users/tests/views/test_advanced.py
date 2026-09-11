import json
from unittest.mock import patch

from celery import states
from django.contrib.auth import get_user_model
from django.contrib.messages import get_messages
from django.test import TestCase
from django.urls import reverse
from django_celery_results.models import TaskResult

from app import metadata
from app.models import Item, MediaTypes, Movie, Sources, Status


class SyncMetadataSettingsViewTests(TestCase):
    """Tests for the Sync Metadata settings page and action."""

    def setUp(self):
        """Create user for the tests."""
        self.credentials = {"username": "testuser", "password": "testpass123"}
        self.user = get_user_model().objects.create_user(**self.credentials)
        self.client.login(**self.credentials)

    def _create_tracked_movie(self):
        """Create a tracked movie for the logged-in user."""
        item = Item.objects.create(
            media_id="238",
            source=Sources.TMDB.value,
            media_type=MediaTypes.MOVIE.value,
            title="The Godfather",
            image="http://example.com/movie.jpg",
        )
        movie = Movie(
            item=item,
            user=self.user,
            status=Status.COMPLETED.value,
        )
        Movie.save_base(movie)
        return item

    def test_page_get_renders_sync_choices(self):
        """GET includes all-tracking and category choices, not episodes."""
        response = self.client.get(reverse("sync_metadata_settings"))

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "users/sync_metadata.html")
        choices = response.context["metadata_sync_choices"]
        values = [choice["value"] for choice in choices]
        labels = [choice["label"] for choice in choices]
        self.assertEqual(values[0], "all")
        self.assertIn("All tracking", labels)
        self.assertIn(MediaTypes.TV.value, values)
        self.assertIn(MediaTypes.SEASON.value, values)
        self.assertIn(MediaTypes.MOVIE.value, values)
        self.assertNotIn(MediaTypes.EPISODE.value, values)
        self.assertContains(response, "Sync Metadata")
        self.assertContains(response, "Force all")
        self.assertContains(response, "Sync History")
        self.assertContains(response, "No metadata syncs in the last 7 days")

    def test_page_get_disables_sync_when_library_empty(self):
        """Empty libraries disable the sync button and show an error."""
        response = self.client.get(reverse("sync_metadata_settings"))
        html = response.content.decode()

        all_choice = response.context["metadata_sync_choices"][0]
        self.assertEqual(all_choice["count"], 0)
        self.assertRegex(
            html,
            r':disabled="selectedCount === 0 \|\| syncInProgress"\s+disabled>',
        )
        self.assertContains(response, "You have no tracked items to sync.")

    def test_page_get_enables_sync_when_items_exist(self):
        """Tracked items keep the default All tracking option enabled."""
        self._create_tracked_movie()

        response = self.client.get(reverse("sync_metadata_settings"))
        html = response.content.decode()
        choices = {
            choice["value"]: choice
            for choice in response.context["metadata_sync_choices"]
        }

        self.assertEqual(choices["all"]["count"], 1)
        self.assertEqual(choices[MediaTypes.MOVIE.value]["count"], 1)
        self.assertEqual(choices[MediaTypes.TV.value]["count"], 0)
        self.assertRegex(html, r':disabled="selectedCount === 0 \|\| syncInProgress"')
        self.assertNotRegex(
            html,
            r':disabled="selectedCount === 0 \|\| syncInProgress"\s+disabled>',
        )
        self.assertFalse(response.context["metadata_sync_in_progress"])

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_all_tracking_queues_task(self, mock_delay):
        """Valid all-tracking POST queues the celery task."""
        self._create_tracked_movie()

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "all"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_called_once_with(
            user_id=self.user.id,
            media_type=None,
        )

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_force_queues_task_without_recent_skip(self, mock_delay):
        """Force checkbox queues a sync that ignores the 1-hour skip."""
        self._create_tracked_movie()

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "all", "force": "1"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_called_once_with(
            user_id=self.user.id,
            media_type=None,
            force=True,
        )

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_category_queues_task(self, mock_delay):
        """Valid category POST queues the celery task for that type."""
        self._create_tracked_movie()

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": MediaTypes.MOVIE.value},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_called_once_with(
            user_id=self.user.id,
            media_type=MediaTypes.MOVIE.value,
        )

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_empty_library_rejected(self, mock_delay):
        """POST with no tracked items is rejected without queueing."""
        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "all"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_not_called()
        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(len(messages), 1)
        self.assertEqual(str(messages[0]), "You have no tracked items to sync.")

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_empty_category_rejected(self, mock_delay):
        """POST for a category with no items is rejected without queueing."""
        self._create_tracked_movie()

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": MediaTypes.TV.value},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_not_called()
        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(len(messages), 1)
        self.assertEqual(
            str(messages[0]),
            "You have no tracked items in the selected category.",
        )

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_invalid_scope_rejected(self, mock_delay):
        """Invalid dropdown values are rejected without queueing."""
        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "not-a-type"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_not_called()
        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(len(messages), 1)
        self.assertIn("Invalid media type", str(messages[0]))

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_demo_user_rejected(self, mock_delay):
        """Demo accounts cannot start a metadata sync."""
        self.user.is_demo = True
        self.user.save(update_fields=["is_demo"])

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "all"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_not_called()
        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(len(messages), 1)
        self.assertIn("view-only", str(messages[0]))

    def _create_metadata_sync_result(self, user, status, task_id="sync-task"):
        """Store a celery metadata-sync result for the given user."""
        return TaskResult.objects.create(
            task_id=task_id,
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {user.id}, 'media_type': None}}",
            status=status,
            result="",
        )

    def test_page_get_disables_sync_when_in_progress(self):
        """An unfinished sync disables the form until it finishes."""
        self._create_tracked_movie()
        self._create_metadata_sync_result(self.user, states.STARTED)

        response = self.client.get(reverse("sync_metadata_settings"))
        html = response.content.decode()

        self.assertTrue(response.context["metadata_sync_in_progress"])
        self.assertRegex(
            html,
            r':disabled="selectedCount === 0 \|\| syncInProgress"\s+disabled>',
        )
        self.assertContains(response, metadata.SYNC_IN_PROGRESS_MESSAGE)

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_rejected_while_in_progress(self, mock_delay):
        """A second sync is rejected while one is queued or running."""
        self._create_tracked_movie()
        self._create_metadata_sync_result(self.user, states.PENDING)

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "all"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_not_called()
        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(len(messages), 1)
        self.assertEqual(str(messages[0]), metadata.SYNC_IN_PROGRESS_MESSAGE)

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_allowed_after_success(self, mock_delay):
        """A finished successful sync does not block a new one."""
        self._create_tracked_movie()
        self._create_metadata_sync_result(self.user, states.SUCCESS)

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "all"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_called_once_with(
            user_id=self.user.id,
            media_type=None,
        )

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_allowed_after_failure(self, mock_delay):
        """A finished failed sync does not block a new one."""
        self._create_tracked_movie()
        self._create_metadata_sync_result(self.user, states.FAILURE)

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "all"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_called_once_with(
            user_id=self.user.id,
            media_type=None,
        )

    @patch("users.views.app_tasks.sync_tracked_metadata.delay")
    def test_sync_not_blocked_by_other_users_task(self, mock_delay):
        """Another user's running sync does not lock this user."""
        self._create_tracked_movie()
        other_user = get_user_model().objects.create_user(
            username="otheruser",
            password="testpass123",  # noqa: S106
        )
        self._create_metadata_sync_result(other_user, states.STARTED, task_id="other")

        response = self.client.post(
            reverse("sync_tracked_metadata"),
            {"media_type": "all"},
        )

        self.assertRedirects(response, reverse("sync_metadata_settings"))
        mock_delay.assert_called_once_with(
            user_id=self.user.id,
            media_type=None,
        )

    def test_history_shows_failed_items(self):
        """History lists the specific items that failed."""
        payload = metadata.format_sync_task_result(
            synced=1,
            failed=1,
            errors=["The Godfather: provider down"],
        )
        TaskResult.objects.create(
            task_id="hist-fail",
            task_name=metadata.METADATA_SYNC_TASK_NAME,
            task_kwargs=f"{{'user_id': {self.user.id}, 'media_type': 'movie'}}",
            status=states.SUCCESS,
            result=json.dumps(payload),
        )

        response = self.client.get(reverse("sync_metadata_settings"))

        self.assertContains(response, "Finished")
        self.assertContains(response, "Movie")
        self.assertContains(response, "1 item failed")
        self.assertContains(response, "Show failed items")
        self.assertContains(response, "The Godfather")
        self.assertContains(response, "provider down")
        self.assertNotContains(response, "No metadata syncs in the last 7 days")

    def test_history_shows_failed_items_from_saved_run(self):
        """History lists failures saved separately from the celery summary."""
        TaskResult.objects.create(
            task_id="hist-run-fail",
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
                        "media_type": "tv",
                        "media_label": "TV Show",
                        "reason": "The Movie Database API network error.",
                    },
                ],
            },
            task_id="hist-run-fail",
        )

        response = self.client.get(reverse("sync_metadata_settings"))

        self.assertContains(response, "Finished")
        self.assertContains(response, "Show failed items")
        self.assertContains(response, "Example Show")
        self.assertContains(response, "The Movie Database API network error.")


class AdvancedSettingsViewTests(TestCase):
    """Tests for the advanced settings page."""

    def setUp(self):
        """Create user for the tests."""
        self.credentials = {"username": "testuser", "password": "testpass123"}
        self.user = get_user_model().objects.create_user(**self.credentials)
        self.client.login(**self.credentials)

    def test_advanced_shows_cache_clear_not_metadata_sync(self):
        """Advanced keeps cache clearing and does not host metadata sync."""
        response = self.client.get(reverse("advanced"))

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "users/advanced.html")
        self.assertContains(response, "Clear Search Cache")
        self.assertNotContains(response, "Force all")
        self.assertNotContains(response, "Sync History")
