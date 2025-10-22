from rest_framework import permissions
from .models import Application

class HasAPIKey(permissions.BasePermission):
    """
    custom permission to allow access only to requests with valid api key via the "X-API-KEY" header.
    """
    message = "Invalid or missing API Key."

    def has_permission(self, request, view) -> bool:
        """
        we need this to check if the request is coming from agenuine application with a valid api key.
        """

        api_key = request.META.get('HTTP_X_API_KEY')
        if not api_key:
            return False
        
        try:
            # try to get application from that api key
            application = Application.objects.get(api_key=api_key)
        except Application.DoesNotExist:
            return False
        
        # attach the application object to the request for further use in views
        # it will help to know which application is making the request
        request.application = application
        return True

