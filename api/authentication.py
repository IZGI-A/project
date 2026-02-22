"""
API Key authentication for tenant-based access.

Each tenant has a hashed API key. Requests include the key in
the Authorization header: "Api-Key sk_live_..."
"""
from django.contrib.auth.hashers import check_password
from rest_framework import authentication, exceptions

from adapter.models import Tenant


class ApiKeyAuthentication(authentication.BaseAuthentication):
    """
    Authenticate requests using tenant API keys.

    Usage:
        Authorization: Api-Key sk_live_abc123...
    """

    KEYWORD = 'Api-Key'

    def authenticate(self, request):
        auth_header = authentication.get_authorization_header(request).decode('utf-8')
        if not auth_header:
            return None

        parts = auth_header.split(' ', 1)
        if len(parts) != 2 or parts[0] != self.KEYWORD:
            return None

        api_key = parts[1].strip()
        prefix = api_key[:16]

        try:
            tenant = Tenant.objects.get(api_key_prefix=prefix, is_active=True)
        except Tenant.DoesNotExist:
            raise exceptions.AuthenticationFailed('Invalid API key.')

        if not check_password(api_key, tenant.api_key_hash):
            raise exceptions.AuthenticationFailed('Invalid API key.')

        # Attach tenant to request for downstream use
        request.tenant = tenant
        return (tenant, api_key)

    def authenticate_header(self, request):
        return self.KEYWORD
