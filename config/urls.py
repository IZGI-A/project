from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('bank/api/', include('external_bank.urls')),
    path('api/', include('api.urls')),
    path('', include('frontend.urls')),
    path('', include('django_prometheus.urls')),
]
