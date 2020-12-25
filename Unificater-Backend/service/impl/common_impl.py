import logging
import time
import traceback

from django.http import JsonResponse
from pymongo.errors import ServerSelectionTimeoutError
from rest_framework import status

from service.util.db_utils import get_data_from_collection_json
from service.util.http_constances import internal_err_msg
from service.util.unify_response import success_response, time_out_response, internal_server_response, \
    no_content_response, write_audit

LOGGER = logging.getLogger('unify_service')


def get_connection_types(request):
    try:
        start = time.time()
        LOGGER.info('Get connection types')
        url = '/connectionJson/connectionTypes'
        result = get_data_from_collection_json('dataSource', url)
        if result:
            for element in result:
                del element['connections']
            response = success_response(data=result)
        else:
            response = no_content_response()
        json_response = JsonResponse(response, status=status.HTTP_200_OK)
    except ServerSelectionTimeoutError as tex:
        LOGGER.error(f'Server Timeout occurred while getting connection types: {tex} | {traceback.format_exc()}')
        response = time_out_response()
        json_response = JsonResponse(response, status=status.HTTP_408_REQUEST_TIMEOUT)
    except Exception as ex:
        LOGGER.error(f'Exception occurred while getting connection types: {ex} | {traceback.format_exc()}')
        response = internal_server_response(internal_err_msg)
        json_response = JsonResponse(response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    write_audit(request, response, start, time.time())
    return json_response
