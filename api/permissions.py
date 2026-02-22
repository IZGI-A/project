"""Permission classes for tenant isolation."""
from rest_framework import permissions


class TenantIsolationPermission(permissions.BasePermission):
    """
    Ensures requests can only access data belonging to the authenticated tenant.

    The tenant is set on the request by ApiKeyAuthentication.
    """

    def has_permission(self, request, view):
        if not hasattr(request, 'tenant') or request.tenant is None:
            return False
        return request.tenant.is_active
