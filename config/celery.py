"""
Celery application configuration.

Beat schedule:
  - check_and_sync: Every 60s, checks Redis for new data and triggers sync if found.
"""
import os

from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('config')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# Beat schedule
app.conf.beat_schedule = {
    'check-and-sync-every-60s': {
        'task': 'adapter.tasks.check_and_sync',
        'schedule': 60.0,
    },
}
