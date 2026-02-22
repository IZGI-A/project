from django.urls import path

from .views import (
    SyncConfigListView, SyncConfigDetailView,
    SyncTriggerView, SyncLogListView, SyncLogDetailView,
    ValidationErrorListView, DataView, ProfilingView,
)

app_name = 'api'

urlpatterns = [
    # Sync configurations
    path('sync/configs/', SyncConfigListView.as_view(), name='sync-config-list'),
    path('sync/configs/<int:pk>/', SyncConfigDetailView.as_view(), name='sync-config-detail'),

    # Sync trigger
    path('sync/trigger/', SyncTriggerView.as_view(), name='sync-trigger'),

    # Sync logs
    path('sync/logs/', SyncLogListView.as_view(), name='sync-log-list'),
    path('sync/logs/<uuid:pk>/', SyncLogDetailView.as_view(), name='sync-log-detail'),

    # Validation errors
    path('sync/logs/<uuid:sync_log_id>/errors/', ValidationErrorListView.as_view(),
         name='validation-error-list'),

    # Data
    path('data/', DataView.as_view(), name='data'),

    # Profiling
    path('profiling/', ProfilingView.as_view(), name='profiling'),
]
