from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings

from redis_metrics.admin import RedisMetricsAdminSite
from redis_metrics.redis_client import (
    build_timeseries,
    build_live_metric_payload,
    get_configured_connections,
    summarize_prefixes,
    summarize_requested_keys,
    summarize_celery,
)


class RedisConnectionConfigTests(SimpleTestCase):
    @override_settings(
        REDIS_METRICS_CONNECTIONS={
            "cache": {
                "LOCATION": "redis://cache:6379/0",
                "KEY_PREFIX": "app-cache",
                "LABEL": "Primary Cache",
            },
            "celery": "redis://celery:6379/1",
        },
        CACHES={
            "default": {
                "BACKEND": "django.core.cache.backends.redis.RedisCache",
                "LOCATION": "redis://default:6379/0",
                "KEY_PREFIX": "default-prefix",
            }
        },
    )
    def test_explicit_redis_metrics_connections_take_priority_and_preserve_cache_discovery(self):
        connections = get_configured_connections()

        self.assertEqual(connections["cache"]["url"], "redis://cache:6379/0")
        self.assertEqual(connections["cache"]["key_prefix"], "app-cache")
        self.assertEqual(connections["cache"]["label"], "Primary Cache")
        self.assertEqual(connections["celery"]["url"], "redis://celery:6379/1")
        self.assertEqual(connections["celery"]["role"], "celery")
        self.assertIn("default", connections)


class RedisSummaryTests(SimpleTestCase):
    def test_prefix_summary_detects_django_style_prefixes(self):
        records = [
            {"prefix_group": "myapp", "ttl": 90},
            {"prefix_group": "myapp", "ttl": 30},
            {"prefix_group": "session", "ttl": -1},
        ]

        summary = summarize_prefixes(records, configured_prefix="myapp")

        self.assertEqual(summary["detected_key_prefix"], "myapp")
        self.assertEqual(summary["entry_count"], 3)
        self.assertEqual(summary["most_frequent_prefixes"][0]["name"], "myapp")

    def test_requested_keys_prefers_frequency_then_idle_time(self):
        records = [
            {"key": "cold", "frequency": 1, "idle_seconds": 90, "idle_label": "1m 30s"},
            {"key": "hot", "frequency": 7, "idle_seconds": 5, "idle_label": "5s"},
            {"key": "fallback", "frequency": None, "idle_seconds": 1, "idle_label": "1s"},
        ]

        ranked = summarize_requested_keys(records)

        self.assertEqual(ranked[0]["key"], "hot")
        self.assertEqual(ranked[0]["score"], 7)

    def test_build_timeseries_calculates_interval_hit_rate(self):
        snapshots = [
            {"timestamp": "2026-04-14T10:00:00", "memory": 100, "ops": 5, "hits": 10, "misses": 5},
            {"timestamp": "2026-04-14T10:01:00", "memory": 120, "ops": 9, "hits": 18, "misses": 7},
        ]

        timeseries = build_timeseries(snapshots)

        self.assertEqual(timeseries[0]["hit_rate"], 0.0)
        self.assertEqual(timeseries[1]["hit_rate"], 80.0)

    def test_celery_summary_reports_queues_and_scheduled_tasks(self):
        class FakeCeleryClient:
            def scan(self, cursor=0, match=None, count=None):
                data = {
                    "celery*": [b"celery", b"celery-task-meta-1"],
                    "*kombu*": [b"_kombu.binding.celery"],
                    "*unacked*": [b"unacked", b"unacked_index"],
                    "*pidbox*": [],
                    "*reply*.pidbox*": [],
                    "*schedule*": [b"schedule"],
                    "*eta*": [],
                }
                return 0, data.get(match, [])

            def type(self, key):
                mapping = {
                    "celery": b"list",
                    "celery-task-meta-1": b"string",
                    "_kombu.binding.celery": b"set",
                    "unacked": b"hash",
                    "unacked_index": b"zset",
                    "schedule": b"zset",
                }
                return mapping[key]

            def llen(self, key):
                return {"celery": 4}.get(key, 0)

            def hlen(self, key):
                return {"unacked": 2}.get(key, 0)

            def scard(self, key):
                return 0

            def zcard(self, key):
                return {"unacked_index": 3, "schedule": 5}.get(key, 0)

        summary = summarize_celery(FakeCeleryClient(), {"role": "celery"})

        self.assertEqual(summary["queue_count"], 1)
        self.assertEqual(summary["total_queue_depth"], 4)
        self.assertEqual(summary["scheduled_tasks"], 8)
        self.assertEqual(summary["result_keys"], 1)


class RedisAdminTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.user = get_user_model().objects.create_superuser(
            username="admin",
            email="admin@example.com",
            password="pass12345",
        )
        self.admin_site = RedisMetricsAdminSite()

    @patch("redis_metrics.admin.get_connection_choices", return_value=[{"name": "cache", "label": "Cache", "url": "redis://cache"}])
    @patch("redis_metrics.admin.get_redis_dashboard")
    def test_dashboard_view_uses_selected_connection(self, mock_dashboard, _mock_choices):
        mock_dashboard.return_value = {
            "connection": {"name": "cache", "label": "Cache"},
            "metrics": [],
            "metric_map": {},
            "health": {"state": "healthy", "checks": []},
            "prefix_summary": {"most_frequent_prefixes": []},
            "key_insights": {"most_requested": [], "useless_keys": [], "size_analysis": {"largest_keys": [], "sampled_memory_human": "0B"}},
            "timeseries": [],
            "live_chart_seed": [],
            "slowlog": [],
            "eviction": {},
            "clients": {"connected_clients": []},
            "celery": None,
            "sample_size": 0,
        }
        request = self.factory.get("/admin/redis-metrics/", {"connection": "cache"})
        request.user = self.user

        response = self.admin_site.redis_metrics_view(request)
        response.render()

        mock_dashboard.assert_called_once_with("cache")
        self.assertEqual(response.context_data["connection"]["name"], "cache")
        self.assertEqual(response.status_code, 200)

    @patch("redis_metrics.admin.build_live_metric_payload")
    def test_live_metrics_view_returns_json_payload(self, mock_payload):
        mock_payload.return_value = {
            "connection": "cache",
            "timestamp": "10:00:00",
            "metrics": {"Used Memory": "10MB"},
            "health_state": "healthy",
            "series": [],
        }
        request = self.factory.get("/admin/redis-metrics/live/", {"connection": "cache"})
        request.user = self.user

        response = self.admin_site.live_metrics_view(request)

        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(
            response.content,
            {
                "connection": "cache",
                "timestamp": "10:00:00",
                "metrics": {"Used Memory": "10MB"},
                "health_state": "healthy",
                "series": [],
            },
        )
