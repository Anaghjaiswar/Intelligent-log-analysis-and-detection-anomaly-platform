from django.contrib import admin
from django.utils import timezone
from django.utils.html import format_html
import datetime
from .models import Application, LogEntry

class TimeRangeFilter(admin.SimpleListFilter):
    title = 'Time Range' 
    parameter_name = 'timerange' 

    def lookups(self, request, model_admin):
        """
        Returns a list of tuples. The first element is the URL parameter value,
        the second is the human-readable label.
        """
        return (
            ('1h', 'Last Hour'),
            ('6h', 'Last 6 Hours'),
            ('24h', 'Last 24 Hours'),
            ('today', 'Today'),
        )

    def queryset(self, request, queryset):
        """
        Filters the queryset based on the selected value.
        """
        now = timezone.now()
        if self.value() == '1h':
            return queryset.filter(timestamp__gte=now - datetime.timedelta(hours=1))
        if self.value() == '6h':
            return queryset.filter(timestamp__gte=now - datetime.timedelta(hours=6))
        if self.value() == '24h':
            return queryset.filter(timestamp__gte=now - datetime.timedelta(days=1))
        if self.value() == 'today':
            today_start = timezone.localtime(now).replace(hour=0, minute=0, second=0, microsecond=0)
            return queryset.filter(timestamp__gte=today_start)
        return queryset

# --- Admin Views ---

@admin.register(Application)
class ApplicationAdmin(admin.ModelAdmin):
    list_display = ('name', 'api_key', 'log_count')
    search_fields = ('name',)
    readonly_fields = ('api_key',)

    def log_count(self, obj):
        return obj.logs.count()
    log_count.short_description = 'Log Count'

@admin.register(LogEntry)
class LogEntryAdmin(admin.ModelAdmin):
    list_display = ('formatted_timestamp', 'application', 'colored_level', 'short_message')
    list_filter = ('level', 'application', TimeRangeFilter, ('timestamp', admin.DateFieldListFilter))
    search_fields = ('message', 'metadata')
    list_display_links = ('formatted_timestamp', 'application')
    list_per_page = 50
    readonly_fields = ('application', 'timestamp', 'level', 'message', 'metadata')

    @admin.display(description='Timestamp (Local)', ordering='timestamp')
    def formatted_timestamp(self, obj):
        local_time = timezone.localtime(obj.timestamp)
        # Format with milliseconds and timezone name (e.g., IST)
        return local_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + f" ({local_time.tzname()})"

    @admin.display(description='Level', ordering='level')
    def colored_level(self, obj):
        level = obj.level.upper()
        color = 'darkgreen'; weight = 'normal'
        if level == 'ERROR': color = 'red'; weight = 'bold'
        elif level == 'CRITICAL': color = 'darkred'; weight = 'bold'
        elif level == 'WARNING': color = 'orange'
        elif level == 'DEBUG': color = 'grey'
        return format_html('<span style="color: {}; font-weight: {};">{}</span>',
                           color, weight, obj.get_level_display())

    @admin.display(description='Message Preview')
    def short_message(self, obj):
        return (obj.message[:100] + '...') if len(obj.message) > 100 else obj.message

    def has_add_permission(self, request): return False
    def has_change_permission(self, request, obj=None): return False