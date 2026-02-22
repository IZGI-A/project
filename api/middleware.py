"""
Tenant middleware: sets the correct schema context based on the
authenticated tenant, so PostgreSQL search_path routes queries to
the right schema.
"""
from config.db_router import set_current_tenant_schema, clear_current_tenant_schema


class TenantMiddleware:
    """
    Sets the tenant schema context for each request.

    Reads request.tenant (set by ApiKeyAuthentication) and sets
    the PostgreSQL search_path to the tenant's schema.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        tenant = getattr(request, 'tenant', None)
        if tenant:
            set_current_tenant_schema(tenant.pg_schema)

        try:
            response = self.get_response(request)
        finally:
            clear_current_tenant_schema()

        return response
