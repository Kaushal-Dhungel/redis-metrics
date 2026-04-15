from django.shortcuts import redirect
from django.urls import reverse


def dashboard_view(request):
    return redirect(reverse("admin:redis_metrics_dashboard"))
