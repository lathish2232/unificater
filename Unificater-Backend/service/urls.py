from django.urls import path

from service.views import process_request

urlpatterns = [
    path('', process_request)
]
