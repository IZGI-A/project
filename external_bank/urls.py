from django.urls import path

from external_bank.views import CSVUploadView, DataUpdateView, DataRetrieveView

app_name = 'external_bank'

urlpatterns = [
    path('upload/', CSVUploadView.as_view(), name='upload'),
    path('update/', DataUpdateView.as_view(), name='update'),
    path('data/', DataRetrieveView.as_view(), name='data'),
]
