"""
API Key authentication for tenant-based access.

Each tenant has a hashed API key. Requests include the key in
the Authorization header: "Api-Key sk_live_..."
"""
from django.contrib.auth.hashers import check_password
from rest_framework import authentication, exceptions

from adapter.models import Tenant
from core.cache import (
    cache_get, cache_set, tenant_auth_key, TTL_TENANT_AUTH,
)


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

        # 1. Try cache
        cache_key = tenant_auth_key(prefix)
        tenant_data = cache_get(cache_key)

        if tenant_data is not None:
            try:
                tenant = Tenant(
                    id=tenant_data['id'],
                    tenant_id=tenant_data['tenant_id'],
                    name=tenant_data['name'],
                    api_key_hash=tenant_data['api_key_hash'],
                    api_key_prefix=tenant_data['api_key_prefix'],
                    pg_schema=tenant_data['pg_schema'],
                    ch_database=tenant_data['ch_database'],
                    is_active=tenant_data['is_active'],
                )
            except Exception:
                tenant_data = None

        # 2. Cache miss — hit DB
        if tenant_data is None:
            try:
                tenant = Tenant.objects.get(api_key_prefix=prefix, is_active=True)
            except Tenant.DoesNotExist:
                raise exceptions.AuthenticationFailed('Invalid API key.')

            cache_set(cache_key, {
                'id': tenant.id,
                'tenant_id': tenant.tenant_id,
                'name': tenant.name,
                'api_key_hash': tenant.api_key_hash,
                'api_key_prefix': tenant.api_key_prefix,
                'pg_schema': tenant.pg_schema,
                'ch_database': tenant.ch_database,
                'is_active': tenant.is_active,
            }, TTL_TENANT_AUTH)

        # 3. Always verify password (even on cache hit)
        if not check_password(api_key, tenant.api_key_hash):
            raise exceptions.AuthenticationFailed('Invalid API key.')

        # Attach tenant to request for downstream use
        request.tenant = tenant
        return (tenant, api_key)

    def authenticate_header(self, request):
        return self.KEYWORD
