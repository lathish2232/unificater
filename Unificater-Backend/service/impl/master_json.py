import logging
import time
import traceback
from uuid import uuid4

from bson import CodecOptions
from bson.raw_bson import RawBSONDocument
from django.http import JsonResponse
from pymongo.errors import CollectionInvalid
from rest_framework import status

from service.util.db_utils import get_mongod_connection, get_data_from_collection_json
from service.util.http_constances import internal_err_msg
from service.util.unify_response import success_response, internal_server_response, duplicate_content_response, \
    success_delete_response, no_content_response, success_create_response, not_accepted_response, write_audit

LOGGER = logging.getLogger('unify_service')


def get_flow(request, url_path):
    try:
        start = time.time()
        LOGGER.info('Get flow')
        collection = url_path.split('/')[1]
        data = get_data_from_collection_json(collection, url_path)
        if data:
            response = success_response(data=data)
        else:
            response = no_content_response()
        json_response = JsonResponse(response, status=status.HTTP_200_OK)
    except Exception as ex:
        LOGGER.error(f'Exception occurred while getting flow: {ex} | {traceback.format_exc()}')
        response = internal_server_response(internal_err_msg)
        json_response = JsonResponse(response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    write_audit(request, response, start, time.time())
    return json_response


def create_flow(request):
    try:
        start = time.time()
        LOGGER.info('Create flow')
        codec_options = CodecOptions(document_class=RawBSONDocument)
        flow_name = request.data.get('Name', None)
        doc = {flow_name: {'UFID': str(uuid4()),
                           'instances': [],
                           'nodes': []}}
        mongo_db = get_mongod_connection()
        mongo_db.create_collection(flow_name, codec_options=codec_options)
        mongo_db[flow_name].insert_one(doc)
        del doc['_id']
        response = success_create_response(f'Flow {flow_name} created successfully.', data=doc)
        json_response = JsonResponse(response, status=status.HTTP_201_CREATED)
    except CollectionInvalid as cex:
        str_ex = str(cex)
        if "already exists" in str_ex:
            str_ex = str_ex.replace('collection ', '')
            LOGGER.error(f'{cex} | {traceback.format_exc()}')
            response = duplicate_content_response(str_ex)
            json_response = JsonResponse(response, status=status.HTTP_409_CONFLICT)
    except Exception as ex:
        LOGGER.error(f'Exception occurred while create flow: {ex} | {traceback.format_exc()}')
        response = internal_server_response(internal_err_msg)
        json_response = JsonResponse(response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    write_audit(request, response, start, time.time())
    return json_response


def delete_flow(request):
    try:
        start = time.time()
        LOGGER.info('Delete flow')
        collection = request.data.get('Name', None)
        mongo_db = get_mongod_connection()
        collection_list = mongo_db.collection_names()
        if collection in collection_list:
            mongo_db[collection].drop()
            response = success_delete_response(message='Flow ' + collection + ' has been deleted successfully')
            json_response = JsonResponse(response, status=status.HTTP_202_ACCEPTED)
        else:
            response = not_accepted_response(message='Flow ' + collection + ' not exist')
            json_response = JsonResponse(response, status=status.HTTP_406_NOT_ACCEPTABLE)
    except Exception as ex:
        LOGGER.error(f'Exception occurred while delete flow: {ex} | {traceback.format_exc()}')
        response = internal_server_response(internal_err_msg)
        json_response = JsonResponse(response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    write_audit(request, response, start, time.time())
    return json_response

    
    