import asyncio
import re
import traceback

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.decorators import api_view

from service.impl.common_impl import get_connection_types
from service.impl.db_metadata import db_metadata
from service.impl.database_impl import get_db_connection_types, get_databases,list_collection_names
from service.impl.flow_instance import Data_source
from service.impl.master_json import create_flow, delete_flow, get_flow
from service.impl.single_file_impl import single_file_browser
from service.util.http_constances import internal_err_msg
from service.util.unify_response import internal_server_response

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


@csrf_exempt
@api_view(['GET', 'POST', 'PUT', 'DELETE'])
def process_request(request):
    try:
        # url_path = request.get_full_path().split('/?')[-1]
        url_path = request.path
        pattern1 = re.compile(r'/connectionTypes/(\w+)')
        pattern2 = re.compile(r'/connectionTypes/database/(\w+)')
        if not url_path.startswith('/connectionTypes'):
            pattern3 = re.compile(r'(/\w+)/.*')
            m3 = pattern3.match(url_path)
            pattern4 = re.compile(r'(/\w+)/instances/(\w+)/database(/\w+(/\w+(/\w+)?)?)?')
            m4 = pattern4.match(url_path)
        m1 = pattern1.match(url_path)
        m2 = pattern2.match(url_path)
        meta_obj = db_metadata()
        conn_obj = Data_source()
        if request.method == 'GET':
            if url_path.startswith('/connectionTypes'):
                if url_path == '/connectionTypes':
                    data = get_connection_types(request)
                elif url_path == '/connectionTypes/singleFile':
                    data = loop.run_until_complete(single_file_browser(request,url_path))
                elif url_path == '/connectionTypes/' + m1.group(1):
                    url_ends = url_path.split('/')[-1]
                    if url_ends == 'database':
                        data = get_databases(request, url_ends)
                elif url_path == '/connectionTypes/database/' + m2.group(1):
                    data = get_db_connection_types(request, url_path)
            else:
                if url_path.endswith('/extractdata'):
                    data = meta_obj.show_table_data(url_path)
                elif url_path.endswith('/showcollections'):
                    data = list_collection_names()
                else:
                    data = get_flow(request, url_path)

        elif request.method == 'POST':
            if url_path == '/flow':
                data = create_flow(request)
            elif url_path == m3.group(1) + '/instances':
                data = conn_obj.createFlow(request)
        elif request.method == 'PUT':
            data = conn_obj.updateItems(request)
        elif request.method == 'DELETE':
            if url_path == '/flow':
                data = delete_flow(request)
            else:
                data = conn_obj.deleteItems(url_path)
        return data
    except Exception as ex:
        print(traceback.format_exc())
        return JsonResponse(internal_server_response(internal_err_msg),
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
