import csv
import io

from rest_framework import status
from rest_framework.parsers import MultiPartParser, JSONParser
from rest_framework.response import Response
from rest_framework.views import APIView

from external_bank import storage


class CSVUploadView(APIView):
    """Upload CSV file to the simulated bank storage."""
    parser_classes = [MultiPartParser]

    def post(self, request):
        tenant_id = request.data.get('tenant_id')
        loan_type = request.data.get('loan_type')
        file_type = request.data.get('file_type')  # 'credit' or 'payment_plan'
        csv_file = request.FILES.get('file')

        if not all([tenant_id, loan_type, file_type, csv_file]):
            return Response(
                {'error': 'tenant_id, loan_type, file_type, and file are required'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if loan_type not in ('RETAIL', 'COMMERCIAL'):
            return Response(
                {'error': 'loan_type must be RETAIL or COMMERCIAL'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if file_type not in ('credit', 'payment_plan'):
            return Response(
                {'error': 'file_type must be credit or payment_plan'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        records = []
        chunk_size = 10000
        text_wrapper = io.TextIOWrapper(csv_file, encoding='utf-8')
        reader = csv.DictReader(text_wrapper, delimiter=';')

        chunk = []
        for row in reader:
            chunk.append(dict(row))
            if len(chunk) >= chunk_size:
                records.extend(chunk)
                chunk = []
        if chunk:
            records.extend(chunk)

        storage.store_data(tenant_id, loan_type, file_type, records)

        return Response({
            'status': 'uploaded',
            'tenant_id': tenant_id,
            'loan_type': loan_type,
            'file_type': file_type,
            'rows': len(records),
        }, status=status.HTTP_201_CREATED)


class DataUpdateView(APIView):
    """Update (replace) data for a tenant/loan_type/file_type via CSV upload."""
    parser_classes = [MultiPartParser]

    def put(self, request):
        tenant_id = request.data.get('tenant_id')
        loan_type = request.data.get('loan_type')
        file_type = request.data.get('file_type')
        csv_file = request.FILES.get('file')

        if not all([tenant_id, loan_type, file_type, csv_file]):
            return Response(
                {'error': 'tenant_id, loan_type, file_type, and file are required'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        records = []
        text_wrapper = io.TextIOWrapper(csv_file, encoding='utf-8')
        reader = csv.DictReader(text_wrapper, delimiter=';')
        for row in reader:
            records.append(dict(row))

        storage.store_data(tenant_id, loan_type, file_type, records)

        return Response({
            'status': 'updated',
            'tenant_id': tenant_id,
            'loan_type': loan_type,
            'file_type': file_type,
            'rows': len(records),
        })


class DataRetrieveView(APIView):
    """Retrieve stored data as JSON."""
    parser_classes = [JSONParser]

    def get(self, request):
        tenant_id = request.query_params.get('tenant_id')
        loan_type = request.query_params.get('loan_type')
        file_type = request.query_params.get('file_type')

        if not all([tenant_id, loan_type, file_type]):
            return Response(
                {'error': 'tenant_id, loan_type, and file_type query params are required'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        data = storage.get_data(tenant_id, loan_type, file_type)

        return Response({
            'tenant_id': tenant_id,
            'loan_type': loan_type,
            'file_type': file_type,
            'count': len(data),
            'data': data,
        })
