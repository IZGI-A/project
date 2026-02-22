import threading
import logging

from django.db import connection

logger = logging.getLogger(__name__)

_thread_local = threading.local()


def set_current_tenant_schema(schema_name):
    """Set the current tenant's schema via search_path."""
    _thread_local.tenant_schema = schema_name
    if schema_name:
        with connection.cursor() as cursor:
            cursor.execute("SET search_path TO %s, public", [schema_name])


def get_current_tenant_schema():
    """Get the current tenant's schema name from thread-local storage."""
    return getattr(_thread_local, 'tenant_schema', None)


def clear_current_tenant_schema():
    """Reset search_path to public only."""
    _thread_local.tenant_schema = None
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET search_path TO public")
    except Exception:
        pass


class TenantSchemaRouter:
    """
    Routes all database operations to 'default'.

    Tenant isolation is achieved via PostgreSQL search_path,
    not separate databases. The search_path is set by
    set_current_tenant_schema() before any ORM query.
    """

    def db_for_read(self, model, **hints):
        return 'default'

    def db_for_write(self, model, **hints):
        return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return db == 'default'
