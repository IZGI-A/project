"""Frontend views using Django templates + HTMX."""
import json
import logging

from django.contrib.auth.hashers import check_password
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.views import View

from adapter.models import Tenant, SyncLog, SyncConfiguration, ValidationError
from adapter.clickhouse_manager import get_clickhouse_client
from adapter.profiling.engine import ProfilingEngine
from adapter.sync.engine import SyncEngine
from config.db_router import set_current_tenant_schema, clear_current_tenant_schema

logger = logging.getLogger(__name__)


class LoginView(View):
    """Login page - authenticate with API key."""

    def get(self, request):
        if request.session.get('tenant_id'):
            return redirect('frontend:dashboard')
        return render(request, 'login.html')

    def post(self, request):
        api_key = request.POST.get('api_key', '').strip()
        if not api_key:
            return render(request, 'login.html', {'error': 'API key is required.'})

        prefix = api_key[:16]
        try:
            tenant = Tenant.objects.get(api_key_prefix=prefix, is_active=True)
        except Tenant.DoesNotExist:
            return render(request, 'login.html', {'error': 'Invalid API key.'})

        if not check_password(api_key, tenant.api_key_hash):
            return render(request, 'login.html', {'error': 'Invalid API key.'})

        request.session['tenant_id'] = tenant.tenant_id
        request.session['tenant_name'] = tenant.name
        request.session['pg_schema'] = tenant.pg_schema
        request.session['ch_database'] = tenant.ch_database
        return redirect('frontend:dashboard')


class LogoutView(View):
    def get(self, request):
        request.session.flush()
        return redirect('frontend:login')


def _get_tenant_context(request):
    """Get tenant info from session and set schema context."""
    tenant_id = request.session.get('tenant_id')
    if not tenant_id:
        return None
    set_current_tenant_schema(request.session.get('pg_schema'))
    return {
        'tenant_id': tenant_id,
        'tenant_name': request.session.get('tenant_name'),
        'pg_schema': request.session.get('pg_schema'),
        'ch_database': request.session.get('ch_database'),
    }


class DashboardView(View):
    """Main dashboard with summary stats."""

    def get(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return redirect('frontend:login')

        try:
            configs = list(SyncConfiguration.objects.values(
                'loan_type', 'is_enabled', 'last_sync_at', 'last_sync_status',
            ))
            recent_logs = list(SyncLog.objects.order_by('-started_at')[:10].values(
                'id', 'loan_type', 'status', 'started_at',
                'total_credit_rows', 'total_payment_rows',
                'valid_credit_rows', 'valid_payment_rows', 'error_count',
            ))

            # Get row counts from ClickHouse
            ch_stats = {}
            try:
                client = get_clickhouse_client(database=ctx['ch_database'])
                for lt in ['RETAIL', 'COMMERCIAL']:
                    for dt in ['credit', 'payment']:
                        table = f'fact_{dt}'
                        result = client.query(
                            f"SELECT count() FROM {table} "
                            f"WHERE loan_type = {{lt:String}}",
                            parameters={'lt': lt},
                        )
                        ch_stats[f"{lt}_{dt}"] = result.result_rows[0][0]
            except Exception as e:
                logger.warning("Could not fetch ClickHouse stats: %s", e)

            context = {
                **ctx,
                'configs': configs,
                'recent_logs': recent_logs,
                'ch_stats': ch_stats,
            }
        except Exception as e:
            logger.warning("Dashboard error: %s", e)
            context = {**ctx, 'configs': [], 'recent_logs': [], 'ch_stats': {}}
        finally:
            clear_current_tenant_schema()

        return render(request, 'dashboard.html', context)


class UploadView(View):
    """CSV upload page."""

    def get(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return redirect('frontend:login')
        clear_current_tenant_schema()
        return render(request, 'upload.html', ctx)

    def post(self, request):
        import csv
        import io
        from external_bank import storage

        ctx = _get_tenant_context(request)
        if not ctx:
            return redirect('frontend:login')
        clear_current_tenant_schema()

        loan_type = request.POST.get('loan_type')
        file_type = request.POST.get('file_type')
        csv_file = request.FILES.get('file')

        if not all([loan_type, file_type, csv_file]):
            return render(request, 'upload.html', {
                **ctx, 'error': 'All fields are required.',
            })

        records = []
        text_wrapper = io.TextIOWrapper(csv_file, encoding='utf-8')
        reader = csv.DictReader(text_wrapper, delimiter=';')
        for row in reader:
            records.append(dict(row))

        storage.store_data(ctx['tenant_id'], loan_type, file_type, records)

        return render(request, 'upload.html', {
            **ctx,
            'success': f'{len(records)} rows uploaded for {loan_type}/{file_type}.',
        })


class SyncView(View):
    """Sync management page."""

    def get(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return redirect('frontend:login')

        try:
            configs = list(SyncConfiguration.objects.all())
            logs = list(SyncLog.objects.order_by('-started_at')[:20])
        except Exception:
            configs, logs = [], []
        finally:
            clear_current_tenant_schema()

        return render(request, 'sync.html', {
            **ctx, 'configs': configs, 'logs': logs,
        })


class SyncTriggerView(View):
    """HTMX endpoint to trigger sync."""

    def post(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return JsonResponse({'error': 'Not authenticated'}, status=401)

        loan_type = request.POST.get('loan_type')
        try:
            config = SyncConfiguration.objects.get(loan_type=loan_type)
            engine = SyncEngine(
                tenant_id=ctx['tenant_id'],
                pg_schema=ctx['pg_schema'],
                ch_database=ctx['ch_database'],
                external_bank_url=config.external_bank_url,
            )
            sync_log = engine.sync(loan_type)

            logs = list(SyncLog.objects.order_by('-started_at')[:20])
        except Exception as e:
            logger.error("Sync trigger error: %s", e)
            logs = []
        finally:
            clear_current_tenant_schema()

        return render(request, 'partials/sync_logs.html', {
            **ctx, 'logs': logs,
        })


class DataViewPage(View):
    """Data viewer page."""

    def get(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return redirect('frontend:login')
        clear_current_tenant_schema()

        loan_type = request.GET.get('loan_type', 'RETAIL')
        data_type = request.GET.get('data_type', 'credit')
        page = int(request.GET.get('page', 1))
        page_size = 50
        offset = (page - 1) * page_size

        data = []
        columns = []
        total_count = 0

        try:
            client = get_clickhouse_client(database=ctx['ch_database'])
            table = f'fact_{data_type}'

            count_result = client.query(
                f"SELECT count() FROM {table} WHERE loan_type = {{lt:String}}",
                parameters={'lt': loan_type},
            )
            total_count = count_result.result_rows[0][0]

            result = client.query(
                f"SELECT * FROM {table} "
                f"WHERE loan_type = {{lt:String}} "
                f"ORDER BY loan_account_number "
                f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
                parameters={'lt': loan_type, 'limit': page_size, 'offset': offset},
            )
            columns = result.column_names
            data = result.result_rows
        except Exception as e:
            logger.warning("Data view error: %s", e)

        total_pages = max(1, (total_count + page_size - 1) // page_size)

        context = {
            **ctx,
            'loan_type': loan_type,
            'data_type': data_type,
            'columns': columns,
            'data': data,
            'total_count': total_count,
            'page': page,
            'total_pages': total_pages,
            'page_range': range(max(1, page - 2), min(total_pages + 1, page + 3)),
        }
        return render(request, 'data_view.html', context)


class ProfilingPageView(View):
    """Data profiling page."""

    def get(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return redirect('frontend:login')
        clear_current_tenant_schema()

        loan_type = request.GET.get('loan_type', 'RETAIL')
        data_type = request.GET.get('data_type', 'credit')

        profile = {}
        try:
            engine = ProfilingEngine(ctx['ch_database'])
            profile = engine.profile(loan_type, data_type)
        except Exception as e:
            logger.warning("Profiling page error: %s", e)

        context = {
            **ctx,
            'loan_type': loan_type,
            'data_type': data_type,
            'profile': profile,
            'profile_json': json.dumps(profile, default=str),
        }
        return render(request, 'profiling.html', context)


class ErrorsView(View):
    """Validation errors page."""

    def get(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return redirect('frontend:login')

        try:
            sync_log_id = request.GET.get('sync_log_id')
            if sync_log_id:
                errors = list(
                    ValidationError.objects.filter(sync_log_id=sync_log_id)
                    .order_by('row_number')[:500]
                )
                sync_log = SyncLog.objects.get(id=sync_log_id)
            else:
                errors = []
                sync_log = None

            logs = list(SyncLog.objects.filter(error_count__gt=0).order_by('-started_at')[:20])
        except Exception as e:
            logger.warning("Errors page error: %s", e)
            errors, sync_log, logs = [], None, []
        finally:
            clear_current_tenant_schema()

        return render(request, 'errors.html', {
            **ctx, 'errors': errors, 'sync_log': sync_log, 'logs': logs,
        })
