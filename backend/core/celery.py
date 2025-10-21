import os
from celery import Celery

# Set the default Django settings module for the 'celery' program.
# This is crucial for your tasks to have access to your Django models.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

# Create the Celery application instance.
# The name 'core' should match your Django project folder name.
app = Celery('core')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related settings
#   should have a `CELERY_` prefix in settings.py.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
# Celery will automatically look for a 'tasks.py' file in each app.
app.autodiscover_tasks()