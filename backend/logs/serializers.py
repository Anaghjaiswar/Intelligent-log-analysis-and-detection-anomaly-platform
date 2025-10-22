from rest_framework import serializers
from .models import LogEntry
from django.utils import timezone

class LogEntrySerializer(serializers.ModelSerializer):
    """
    handles validation and serialization of LogEntry model
    """
    timestamp = serializers.DateTimeField(required=True)
    level = serializers.ChoiceField(choices=LogEntry.LEVEL_CHOICES,required=True)
    message = serializers.CharField(required=True, allow_blank=False)
    metadata = serializers.JSONField(required=False, default=dict)

    class Meta:
        model = LogEntry
        fields = ['timestamp', 'level', 'message', 'metadata']
        # i did not set the application field because it will be set by the view based on appication's api key

    def validate_level(self, value: str) -> str:
        return value.upper()
    
    def validate_timestamp(self, value):
        """Ensure timestamp is not in the future."""
        if value > timezone.now():
            raise serializers.ValidationError("Timestamp cannot be in the future.")
        return value