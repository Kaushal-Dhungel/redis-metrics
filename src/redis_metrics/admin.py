from django.contrib import admin, messages
from django.http import JsonResponse
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse

from .redis_client import (
    build_live_metric_payload,
    clear_connection,
    delete_key,
    delete_keys_by_pattern,
    get_connection_choices,
    get_key_details,
    get_key_page,
    get_redis_dashboard,
    update_string_key,
)


class RedisMetricsAdminSite:
    def get_urls(self):
        return [
            path(
                "redis-metrics/",
                admin.site.admin_view(self.redis_metrics_view),
                name="redis_metrics_dashboard",
            ),
            path(
                "redis-metrics/keys/",
                admin.site.admin_view(self.key_explorer_view),
                name="redis_metrics_key_explorer",
            ),
            path(
                "redis-metrics/live/",
                admin.site.admin_view(self.live_metrics_view),
                name="redis_metrics_live",
            ),
            path(
                "redis-metrics/keys/delete/",
                admin.site.admin_view(self.delete_key_view),
                name="redis_metrics_delete_key",
            ),
            path(
                "redis-metrics/keys/delete-pattern/",
                admin.site.admin_view(self.delete_pattern_view),
                name="redis_metrics_delete_pattern",
            ),
            path(
                "redis-metrics/keys/clear/",
                admin.site.admin_view(self.clear_connection_view),
                name="redis_metrics_clear_connection",
            ),
            path(
                "redis-metrics/keys/edit/",
                admin.site.admin_view(self.edit_key_view),
                name="redis_metrics_edit_key",
            ),
        ]

    def base_context(self, request, title, connection_name):
        return {
            **admin.site.each_context(request),
            "title": title,
            "selected_connection": connection_name,
            "connections": get_connection_choices(),
            "dashboard_url": reverse("admin:redis_metrics_dashboard"),
            "key_explorer_url": reverse("admin:redis_metrics_key_explorer"),
            "live_metrics_url": reverse("admin:redis_metrics_live"),
        }

    def redis_metrics_view(self, request):
        connection_name = request.GET.get("connection")
        context = self.base_context(request, "Redis Metrics", connection_name)

        try:
            dashboard = get_redis_dashboard(connection_name)
            context.update(dashboard)
            context["redis_ok"] = True
        except Exception as exc:
            context["redis_ok"] = False
            context["error"] = str(exc)

        return TemplateResponse(request, "redis_metrics/admin_dashboard.html", context)

    def live_metrics_view(self, request):
        connection_name = request.GET.get("connection")
        payload = build_live_metric_payload(connection_name)
        return JsonResponse(payload)

    def key_explorer_view(self, request):
        query = (request.GET.get("q") or "").strip()
        connection_name = request.GET.get("connection")

        try:
            page = int(request.GET.get("page", 1))
        except ValueError:
            page = 1

        data = get_key_page(connection_name=connection_name, query=query, page=page, page_size=25)
        context = self.base_context(request, "Redis Key Explorer", data["connection"]["name"])
        context.update({
            "search_query": data["query"],
            "pattern": data["pattern"],
            "results": data["items"],
            "page": data["page"],
            "total_pages": data["total_pages"],
            "total_count": data["total_count"],
            "has_prev": data["has_prev"],
            "has_next": data["has_next"],
            "prev_page": data["prev_page"],
            "next_page": data["next_page"],
            "connection": data["connection"],
        })

        return TemplateResponse(request, "redis_metrics/key_explorer.html", context)

    def delete_key_view(self, request):
        if request.method != "POST":
            return redirect("admin:redis_metrics_key_explorer")

        connection_name = request.POST.get("connection")
        key = request.POST.get("key", "")
        query = request.POST.get("q", "")
        page = request.POST.get("page", "1")

        if key:
            deleted = delete_key(key, connection_name=connection_name)
            if deleted:
                messages.success(request, f"Deleted key: {key}")
            else:
                messages.warning(request, f"Key not found: {key}")

        redirect_url = reverse("admin:redis_metrics_key_explorer")
        return redirect(f"{redirect_url}?connection={connection_name}&q={query}&page={page}")

    def delete_pattern_view(self, request):
        if request.method != "POST":
            return redirect("admin:redis_metrics_key_explorer")

        connection_name = request.POST.get("connection")
        query = request.POST.get("q", "").strip()
        deleted_count = delete_keys_by_pattern(query, connection_name=connection_name)

        if deleted_count:
            messages.success(request, f"Deleted {deleted_count} keys.")
        else:
            messages.warning(request, "No matching keys found.")

        redirect_url = reverse("admin:redis_metrics_key_explorer")
        return redirect(f"{redirect_url}?connection={connection_name}&q={query}")

    def clear_connection_view(self, request):
        if request.method != "POST":
            return redirect("admin:redis_metrics_key_explorer")

        connection_name = request.POST.get("connection")
        deleted_count = clear_connection(connection_name=connection_name)

        if deleted_count:
            messages.success(request, f"Cleared {deleted_count} keys from the selected Redis connection.")
        else:
            messages.warning(request, "No keys were removed.")

        redirect_url = reverse("admin:redis_metrics_key_explorer")
        return redirect(f"{redirect_url}?connection={connection_name}")

    def edit_key_view(self, request):
        connection_name = request.GET.get("connection") if request.method == "GET" else request.POST.get("connection")
        key = request.GET.get("key") if request.method == "GET" else request.POST.get("key")

        if not key:
            return redirect("admin:redis_metrics_key_explorer")

        details = get_key_details(key, connection_name=connection_name)
        if details is None:
            messages.error(request, "Key does not exist.")
            return redirect(f"{reverse('admin:redis_metrics_key_explorer')}?connection={connection_name}")

        if request.method == "POST":
            if not details["is_editable"]:
                messages.error(request, "This key is not editable.")
                return redirect(f"{reverse('admin:redis_metrics_key_explorer')}?connection={connection_name}")

            new_value = request.POST.get("value", "")
            serializer = request.POST.get("serializer", "text")
            try:
                update_string_key(
                    key,
                    new_value,
                    connection_name=connection_name,
                    serializer=serializer,
                    keep_ttl=True,
                )
            except ValueError as exc:
                messages.error(request, f"Invalid value: {exc}")
                return redirect(
                    f"{reverse('admin:redis_metrics_edit_key')}?connection={connection_name}&key={key}"
                )
            messages.success(request, f"Updated key: {key}")
            return redirect(
                f"{reverse('admin:redis_metrics_edit_key')}?connection={connection_name}&key={key}"
            )

        context = self.base_context(request, "Edit Redis Key", connection_name)
        selected_connection = next(
            (
                item
                for item in get_connection_choices()
                if item["name"] == (connection_name or context["selected_connection"])
            ),
            get_connection_choices()[0],
        )
        context.update({
            "connection": selected_connection,
            "key_data": details,
        })

        return TemplateResponse(request, "redis_metrics/edit_key.html", context)


redis_metrics_admin = RedisMetricsAdminSite()
original_get_urls = admin.site.get_urls


def get_urls():
    return redis_metrics_admin.get_urls() + original_get_urls()


admin.site.get_urls = get_urls
