from django.urls import path

from .views import (
    LoginView, LogoutView, DashboardView, UploadView,
    SyncView, SyncTriggerView, DataViewPage,
    ProfilingPageView, ErrorsView,
    FailedRecordsPreviewView, FailedRecordsDownloadView, FailedRecordsDismissView,
    SettingsView,
)

app_name = 'frontend'

urlpatterns = [
    path('login/', LoginView.as_view(), name='login'),
    path('logout/', LogoutView.as_view(), name='logout'),
    path('', DashboardView.as_view(), name='dashboard'),
    path('upload/', UploadView.as_view(), name='upload'),
    path('sync/', SyncView.as_view(), name='sync'),
    path('sync/trigger/', SyncTriggerView.as_view(), name='sync-trigger'),
    path('sync/failed/preview/', FailedRecordsPreviewView.as_view(), name='failed-preview'),
    path('sync/failed/download/', FailedRecordsDownloadView.as_view(), name='failed-download'),
    path('sync/failed/dismiss/', FailedRecordsDismissView.as_view(), name='failed-dismiss'),
    path('data/', DataViewPage.as_view(), name='data-view'),
    path('profiling/', ProfilingPageView.as_view(), name='profiling'),
    path('errors/', ErrorsView.as_view(), name='errors'),
    path('settings/', SettingsView.as_view(), name='settings'),
]
