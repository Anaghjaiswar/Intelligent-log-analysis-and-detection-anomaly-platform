from django.contrib import admin
from .models import Application, LogEntry


admin.site.register(Application)
admin.site.register(LogEntry)

