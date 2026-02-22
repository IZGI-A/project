"""REST API views for the financial data integration adapter."""
import logging

from rest_framework import status, generics
from rest_framework.response import Response
from rest_framework.views import APIView

from adapter.models import SyncLog, SyncConfiguration, ValidationError
from adapter.sync.engine import SyncEngine
from config.db_router import set_current_tenant_schema, clear_current_tenant_schema
from .authentication import ApiKeyAuthentication
from .permissions import TenantIsolationPermission
from .serializers import (
    SyncConfigurationSerializer, SyncLogSerializer,
    ValidationErrorSerializer, SyncTriggerSerializer,
)

logger = logging.getLogger(__name__)


class TenantMixin:
    """Mixin that sets tenant DB context."""
    authentication_classes = [ApiKeyAuthentication]
    permission_classes = [TenantIsolationPermission]

    def initial(self, request, *args, **kwargs):
        super().initial(request, *args, **kwargs)
        if hasattr(request, 'tenant'):
            set_current_tenant_schema(request.tenant.pg_schema)

    def finalize_response(self, request, response, *args, **kwargs):
        clear_current_tenant_schema()
        return super().finalize_response(request, response, *args, **kwargs)


class SyncConfigListView(TenantMixin, generics.ListCreateAPIView):
    """List and create sync configurations for the tenant."""
    serializer_class = SyncConfigurationSerializer

    def get_queryset(self):
        return SyncConfiguration.objects.all()


class SyncConfigDetailView(TenantMixin, generics.RetrieveUpdateAPIView):
    """Retrieve or update a sync configuration."""
    serializer_class = SyncConfigurationSerializer

    def get_queryset(self):
        return SyncConfiguration.objects.all()


class SyncTriggerView(TenantMixin, APIView):
    """Manually trigger a sync for a loan type."""

    def post(self, request):
        serializer = SyncTriggerSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        loan_type = serializer.validated_data['loan_type']
        tenant = request.tenant

        # Find sync configuration
        try:
            config = SyncConfiguration.objects.get(loan_type=loan_type)
        except SyncConfiguration.DoesNotExist:
            return Response(
                {'error': f'No sync configuration for {loan_type}'},
                status=status.HTTP_404_NOT_FOUND,
            )

        if not config.is_enabled:
            return Response(
                {'error': f'Sync for {loan_type} is disabled'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        engine = SyncEngine(
            tenant_id=tenant.tenant_id,
            pg_schema=tenant.pg_schema,
            ch_database=tenant.ch_database,
            external_bank_url=config.external_bank_url,
        )

        sync_log = engine.sync(loan_type)
        return Response(
            SyncLogSerializer(sync_log).data,
            status=status.HTTP_200_OK if sync_log.status == 'COMPLETED'
            else status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


class SyncLogListView(TenantMixin, generics.ListAPIView):
    """List sync logs for the tenant."""
    serializer_class = SyncLogSerializer

    def get_queryset(self):
        qs = SyncLog.objects.all()
        loan_type = self.request.query_params.get('loan_type')
        if loan_type:
            qs = qs.filter(loan_type=loan_type)
        return qs


class SyncLogDetailView(TenantMixin, generics.RetrieveAPIView):
    """Retrieve a sync log with validation error summary."""
    serializer_class = SyncLogSerializer

    def get_queryset(self):
        return SyncLog.objects.all()


class ValidationErrorListView(TenantMixin, generics.ListAPIView):
    """List validation errors for a specific sync log."""
    serializer_class = ValidationErrorSerializer

    def get_queryset(self):
        sync_log_id = self.kwargs.get('sync_log_id')
        qs = ValidationError.objects.filter(sync_log_id=sync_log_id)
        file_type = self.request.query_params.get('file_type')
        if file_type:
            qs = qs.filter(file_type=file_type)
        error_type = self.request.query_params.get('error_type')
        if error_type:
            qs = qs.filter(error_type=error_type)
        return qs


class DataView(TenantMixin, APIView):
    """Retrieve data from ClickHouse data warehouse."""

    def get(self, request):
        from adapter.clickhouse_manager import get_clickhouse_client

        tenant = request.tenant
        loan_type = request.query_params.get('loan_type')
        data_type = request.query_params.get('data_type', 'credit')
        limit = min(int(request.query_params.get('limit', 100)), 10000)
        offset = int(request.query_params.get('offset', 0))

        if not loan_type:
            return Response(
                {'error': 'loan_type query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        table = 'fact_credit' if data_type == 'credit' else 'fact_payment'

        try:
            client = get_clickhouse_client(database=tenant.ch_database)
            count_result = client.query(
                f"SELECT count() FROM {table} WHERE loan_type = {{loan_type:String}}",
                parameters={'loan_type': loan_type},
            )
            total_count = count_result.result_rows[0][0]

            result = client.query(
                f"SELECT * FROM {table} "
                f"WHERE loan_type = {{loan_type:String}} "
                f"ORDER BY loan_account_number "
                f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
                parameters={
                    'loan_type': loan_type,
                    'limit': limit,
                    'offset': offset,
                },
            )

            columns = result.column_names
            rows = []
            for row in result.result_rows:
                rows.append(dict(zip(columns, [self._serialize_value(v) for v in row])))

            return Response({
                'loan_type': loan_type,
                'data_type': data_type,
                'total_count': total_count,
                'limit': limit,
                'offset': offset,
                'data': rows,
            })
        except Exception as e:
            logger.error("ClickHouse query error: %s", e)
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @staticmethod
    def _serialize_value(value):
        from datetime import date, datetime
        from decimal import Decimal
        from uuid import UUID
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, UUID):
            return str(value)
        return value


class ProfilingView(TenantMixin, APIView):
    """Data profiling endpoint - queries ClickHouse directly."""

    def get(self, request):
        from adapter.profiling.engine import ProfilingEngine

        tenant = request.tenant
        loan_type = request.query_params.get('loan_type')
        data_type = request.query_params.get('data_type', 'credit')

        if not loan_type:
            return Response(
                {'error': 'loan_type query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        engine = ProfilingEngine(tenant.ch_database)
        try:
            profile = engine.profile(loan_type, data_type)
            return Response(profile)
        except Exception as e:
            logger.error("Profiling error: %s", e)
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
