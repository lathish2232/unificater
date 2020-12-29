import logging
import time
import traceback

from django.http import JsonResponse
from pymongo.errors import ServerSelectionTimeoutError
from rest_framework import status

from service.util.db_utils import get_data_from_collection_json,get_collections
from service.util.http_constances import internal_err_msg
from service.util.unify_response import success_response, time_out_response, internal_server_response, write_audit, \
    no_content_response
from service.util.unify_uris import CONN_TYPE_ID, DATABASE_ID

LOGGER = logging.getLogger('unify_service')


def get_databases(request, url_ends):
    try:
        start = time.time()
        LOGGER.info('Get databases')
        url_str = '/connectionJson/connectionTypes/' + CONN_TYPE_ID[url_ends] + '/connections'
        result = get_data_from_collection_json('dataSource', url_str)
        if result:
            for element in result:
                del element['functionName']
                del element['isFwf']
                del element['returnType']
                del element['connectionParameters']
            response = success_response(data=result)
        else:
            response = no_content_response()
        json_response = JsonResponse(response, status=status.HTTP_200_OK)
    except ServerSelectionTimeoutError as tex:
        LOGGER.error(
            f'Server Timeout occurred while getting databases: {tex} | {traceback.format_exc()}')
        response = time_out_response()
        json_response = JsonResponse(response, status=status.HTTP_408_REQUEST_TIMEOUT)
    except Exception as ex:
        LOGGER.error(f'Exception occurred while getting databases: {ex} | {traceback.format_exc()}')
        response = internal_server_response(internal_err_msg)
        json_response = JsonResponse(response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    write_audit(request, response, start, time.time())
    return json_response


def get_db_connection_types(request, url_path):
    try:
        start = time.time()
        LOGGER.info('Get databases connection params...')
        url_ends = url_path.split('/')[-1]
        if url_ends == 'postgreSql' or url_ends == 'MSSQL'or url_ends == 'mySql':
            url = '/connectionJson/connectionTypes/' + CONN_TYPE_ID.get(
                'database') + '/connections/' + DATABASE_ID.get(url_ends) #+ '/connectionParameters'
            conn_params = get_data_from_collection_json('dataSource', url)
        if conn_params:
            result = {'type': 'database', 'displayName': 'Database', 'name': url_ends,
                      'connectionParameters': conn_params['connectionParameters'], 'dataInstances': [{'id':'dataInstancesId_1','functionName':None,'dataParameters':[]}]}
            result['dataInstances'][0]['functionName']=conn_params['functionName']  
            response = success_response(data=result)
        else:
            response = no_content_response()
        json_response = JsonResponse(response, status=status.HTTP_200_OK)
    except ServerSelectionTimeoutError as tex:
        LOGGER.error(
            f'Server Timeout occurred while getting database connection params: {tex} | {traceback.format_exc()}')
        response = time_out_response()
        json_response = JsonResponse(response, status=status.HTTP_408_REQUEST_TIMEOUT)
    except Exception as ex:
        LOGGER.error(f'Exception occurred while getting database connection params: {ex} | {traceback.format_exc()}')
        response = internal_server_response(internal_err_msg)
        json_response = JsonResponse(response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    write_audit(request, response, start, time.time())
    return json_response

def list_collection_names():
    LOGGER.info('Get created priojects')
    result={}
    result['collections']=get_collections()
    return JsonResponse(success_response(data=result),status=status.HTTP_200_OK)