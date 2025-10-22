import json
from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .serializers import LogEntrySerializer
from .auth import HasAPIKey
from django.core.cache import caches


class LogIngestionView(APIView):
    """
    API endpoint for ingesting log entries.
    Authenticates the request via an API key, validates the log data,
    and queues it for asynchronous processing.
    """
    ## this line involves db lookup but on indexed fields(quite fast), but in extreme high-throughput scenarios we will look for caching strategies.
    permission_classes = [HasAPIKey] 

    serializer_class = LogEntrySerializer

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)

        data_to_queue = serializer.validated_data
        # The 'request.application' is attached by the HasAPIKey permission class.
        data_to_queue['application_id'] = request.application.id

        # Convert datetime to string for JSON serialization if it exists
        if 'timestamp' in data_to_queue and data_to_queue['timestamp']:
            data_to_queue['timestamp'] = data_to_queue['timestamp'].isoformat()

        # Convert metadata to JSON string for Redis Stream
        if 'metadata' in data_to_queue and isinstance(data_to_queue['metadata'], dict):
            data_to_queue['metadata'] = json.dumps(data_to_queue['metadata'])
        else:
            data_to_queue['metadata'] = "{}" # Ensure it's a string, even if empty

        try:
            # Get the raw Redis client from the 'log_queue' cache configuration
            redis_client = caches['log_queue'].client.get_client()
            redis_client.xadd("logs:queue", data_to_queue) # Use Redis Streams
        except Exception:
            # django-redis will raise an exception if it can't connect
            return Response({"error": "Log queue service is unavailable."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

        return Response({"message": "Log accepted for processing."}, status=status.HTTP_202_ACCEPTED)
