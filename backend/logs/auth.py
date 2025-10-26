from rest_framework.permissions import BasePermission
from django.core.cache import caches
from .models import Application

class HasAPIKey(BasePermission):
    """
    Custom permission to authenticate requests via an API key.
    - Checks for the 'X-API-Key' header.
    - Implements a cache-aside strategy for high performance, freeing up
      database connections for write-heavy operations.
    """
    message = 'Invalid or missing API Key.'
    # Use the 'default' cache defined in settings.py, which points to Redis DB 1
    cache = caches['default']

    def has_permission(self, request, view):
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return False

        # 1. Check the cache first
        cache_key = f"api_key:{api_key}"
        cached_app_id = self.cache.get(cache_key)
        if cached_app_id is not None:
            if cached_app_id == "not_found":
                # Cache hit, but it's explicitly marked as invalid. Deny permission.
                return False
            else:
                # Cache hit, valid application found
                request.application = Application.objects.get(id=cached_app_id)
                return True

        # 2. Cache miss: Query the database
        try:
            application = Application.objects.get(api_key=api_key)
            # 3. Cache the result for 1 hour (3600 seconds)
            self.cache.set(cache_key, application.id, timeout=3600)
            request.application = application
            return True
        except Application.DoesNotExist:
            # Cache "not found" for 60s to mitigate brute-force attempts on invalid keys
            self.cache.set(cache_key, "not_found", timeout=60)
            return False
