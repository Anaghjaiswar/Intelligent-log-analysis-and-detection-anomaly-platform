from django.db import models
import uuid

class Application(models.Model):
    """
    it is the client application that will send logs.
    application will have a unique , auto generated api key to identify itself.
    """

    name = models.CharField(max_length=255, unique=True)
    api_key = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)

    def __str__(self) -> str:
        return self.name
    


class LogEntry(models.Model):
    """
    it is the log entry model that will store the logs sent by the application.
    """
    LEVEL_CHOICES = [
        ('DEBUG', 'Debug'),
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('CRITICAL', 'Critical'),
    ]

    application = models.ForeignKey(
        Application, 
        on_delete=models.CASCADE, 
        related_name='logs',
        db_index=True,
        help_text="The application that sent the log entry."
    )
    timestamp = models.DateTimeField(
        db_index=True, 
        help_text="The time when the log entry was created."
    )
    level = models.CharField(
        max_length=10, 
        choices=LEVEL_CHOICES, 
        db_index=True, 
        help_text="The severity level of the log entry."
    )
    message = models.TextField(help_text="The log message.")
    metadata = models.JSONField(
        default=dict, 
        blank=True, 
        help_text="structured data associated with log entry"
    )

    class Meta:
        ordering = ["-timestamp"]
        indexes = [
            models.Index(fields=['application', 'level', '-timestamp']),
        ]

    def clean(self):
        self.level = self.level.upper()

    def __str__(self) -> str:
        return f"{self.timestamp.isoformat()} [{self.level}] - {self.application.name}"
    