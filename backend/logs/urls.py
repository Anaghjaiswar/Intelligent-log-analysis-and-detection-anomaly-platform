from django.urls import path
from .views import LogIngestionView

app_name = 'logs'

urlpatterns = [
    path('ingest/', LogIngestionView.as_view(), name='log-ingest'),
]