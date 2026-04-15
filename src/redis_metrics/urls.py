from django.urls import path
from .views import dashboard_view

app_name = "redis_metrics"

urlpatterns = [
    path("", dashboard_view, name="dashboard"),
]