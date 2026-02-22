"""Frontend views using Django templates + HTMX."""
import csv
import io
import json
import logging

from django.contrib.auth.hashers import check_password
from django.http import JsonResponse, HttpResponse
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

        text_wrapper = io.TextIOWrapper(csv_file, encoding='utf-8')
        reader = csv.DictReader(text_wrapper, delimiter=';')

        # Validate CSV headers match the selected loan_type/file_type
        headers = set(reader.fieldnames or [])

        # Columns unique to each type — used to reject wrong file
        COMMERCIAL_ONLY = {'loan_product_type', 'sector_code', 'internal_credit_rating',
                           'default_probability', 'risk_class', 'customer_segment'}
        RETAIL_ONLY = {'insurance_included', 'customer_district_code',
                       'customer_province_code'}
        CREDIT_ONLY = {'customer_id', 'customer_type', 'original_loan_amount',
                       'outstanding_principal_balance'}
        PAYMENT_ONLY = {'installment_number', 'installment_amount',
                        'principal_component', 'installment_status'}

        if file_type == 'credit':
            required = {'loan_account_number', 'customer_id', 'customer_type',
                        'loan_status_code', 'original_loan_amount',
                        'outstanding_principal_balance'}
            if loan_type == 'COMMERCIAL':
                required |= {'loan_product_type', 'sector_code'}
                rejected = RETAIL_ONLY
            else:
                rejected = COMMERCIAL_ONLY
        else:
            required = {'loan_account_number', 'installment_number',
                        'installment_amount', 'principal_component'}
            rejected = CREDIT_ONLY

        missing = required - headers
        if missing:
            return render(request, 'upload.html', {
                **ctx,
                'error': f'This file does not match {loan_type}/{file_type}. '
                         f'Missing columns: {", ".join(sorted(missing))}',
            })

        unexpected = rejected & headers
        if unexpected:
            return render(request, 'upload.html', {
                **ctx,
                'error': f'This file does not match {loan_type}/{file_type}. '
                         f'Unexpected columns: {", ".join(sorted(unexpected))}',
            })

        records = []
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

        # Check Redis for failed records (post-sync failures only)
        from external_bank import storage
        failed_records = []
        for loan_type in ['RETAIL', 'COMMERCIAL']:
            for file_type in ['credit', 'payment_plan']:
                count = storage.get_failed_row_count(ctx['tenant_id'], loan_type, file_type)
                if count > 0:
                    failed_records.append({
                        'loan_type': loan_type,
                        'file_type': file_type,
                        'count': count,
                    })

        return render(request, 'sync.html', {
            **ctx, 'configs': configs, 'logs': logs,
            'failed_records': failed_records,
        })


class SyncTriggerView(View):
    """HTMX endpoint to trigger sync."""

    def post(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return HttpResponse(status=401)

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

        # Build failed records for OOB swap
        from external_bank import storage
        failed_records = []
        for lt in ['RETAIL', 'COMMERCIAL']:
            for ft in ['credit', 'payment_plan']:
                c = storage.get_failed_row_count(ctx['tenant_id'], lt, ft)
                if c > 0:
                    failed_records.append({
                        'loan_type': lt,
                        'file_type': ft,
                        'count': c,
                    })

        # Render sync logs (primary swap) + failed records (OOB swap)
        logs_html = render(request, 'partials/sync_logs.html', {
            **ctx, 'logs': logs,
        }).content.decode()

        failed_html = render(request, 'partials/failed_records.html', {
            **ctx, 'failed_records': failed_records,
        }).content.decode()

        combined = logs_html + f'\n<div id="failed-records" hx-swap-oob="innerHTML">{failed_html}</div>'
        return HttpResponse(combined)


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


class FailedRecordsPreviewView(View):
    """HTMX endpoint: preview failed records from Redis."""

    def get(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return HttpResponse(status=401)
        clear_current_tenant_schema()

        from external_bank import storage

        loan_type = request.GET.get('loan_type')
        file_type = request.GET.get('file_type')

        if not loan_type or not file_type:
            return JsonResponse({'error': 'loan_type and file_type required'}, status=400)

        records = storage.get_failed(ctx['tenant_id'], loan_type, file_type)
        preview = records[:20]  # Show first 20 rows
        columns = list(preview[0].keys()) if preview else []
        rows = [[row.get(col, '') for col in columns] for row in preview]

        return render(request, 'partials/failed_preview.html', {
            'columns': columns,
            'rows': rows,
            'total': len(records),
            'showing': len(preview),
        })


class FailedRecordsDownloadView(View):
    """Download failed records from Redis as CSV."""

    def get(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return redirect('frontend:login')
        clear_current_tenant_schema()

        from external_bank import storage

        loan_type = request.GET.get('loan_type')
        file_type = request.GET.get('file_type')

        if not loan_type or not file_type:
            return JsonResponse({'error': 'loan_type and file_type required'}, status=400)

        records = storage.get_failed(ctx['tenant_id'], loan_type, file_type)
        if not records:
            return JsonResponse({'error': 'No failed records found'}, status=404)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=records[0].keys(), delimiter=';')
        writer.writeheader()
        writer.writerows(records)

        response = HttpResponse(output.getvalue(), content_type='text/csv')
        filename = f"failed_{loan_type.lower()}_{file_type}.csv"
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response


class FailedRecordsDismissView(View):
    """Dismiss (delete) failed records from Redis."""

    def post(self, request):
        ctx = _get_tenant_context(request)
        if not ctx:
            return HttpResponse(status=401)
        clear_current_tenant_schema()

        from external_bank import storage

        loan_type = request.POST.get('loan_type')
        file_type = request.POST.get('file_type')

        if not loan_type or not file_type:
            return HttpResponse('Missing parameters', status=400)

        count = storage.get_failed_row_count(ctx['tenant_id'], loan_type, file_type)
        storage.clear_failed(ctx['tenant_id'], loan_type, file_type)

        logger.info(
            "Dismissed %d failed records: %s/%s/%s",
            count, ctx['tenant_id'], loan_type, file_type,
        )

        # Return updated failed records partial
        failed_records = []
        for lt in ['RETAIL', 'COMMERCIAL']:
            for ft in ['credit', 'payment_plan']:
                c = storage.get_failed_row_count(ctx['tenant_id'], lt, ft)
                if c > 0:
                    failed_records.append({
                        'loan_type': lt,
                        'file_type': ft,
                        'count': c,
                    })

        return render(request, 'partials/failed_records.html', {
            **ctx, 'failed_records': failed_records,
        })
