import logging
import os
import time
import traceback
from tkinter import Tk, filedialog

from django.http import JsonResponse
from rest_framework import status

from service.util.db_utils import get_data_from_collection_json
from service.util.http_constances import success_msg, internal_err_msg
from service.util.unify_response import success_response, internal_server_response, no_content_response, write_audit
from service.util.unify_uris import CONN_TYPE_ID

LOGGER = logging.getLogger('unify_service')


async def single_file_browser(request, url_path):
    try:
        start = time.time()
        LOGGER.info('single file browser')
        doc = {'type': None, 'displayName': None, 'fileType': None,
               'connectionParameters': [{'id': 'connectionParameters_1', 'path': None}],
               'dataInstances': [
                   {'id': 'dataInstancesId_1', 'functionName': None, 'isFwf': None, 'dataParameters': None}]}
        root = Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        filename = filedialog.askopenfilenames(parent=root, title="unificater file Browser",
                                               filetypes=[("All Files/pattern", "*")])
        if filename:
            filename = filename[0]
            root.destroy()
            # add connection_type to flow
            url_ends = url_path.split('/')[-1]
            filetype = os.path.splitext(filename)[1][1:]
            doc['fileType'] = filetype
            doc['type'] = url_ends
            doc['displayName'] = filename.split('/')[-1]
            doc['connectionParameters'][0]['path'] = filename
            if filetype == 'csv':
                url = '/connectionJson/connectionTypes/' + CONN_TYPE_ID.get(
                    url_ends) + '/connections/' + filetype.upper()
                result = get_data_from_collection_json('dataSource', url)
                if result:
                    doc['dataInstances'][0]['functionName'] = result['functionName']
                    doc['dataInstances'][0]['isFwf'] = result['isFwf']
                    doc['dataInstances'][0]['dataParameters'] = result['connectionParameters']
                    doc['dataInstances'][0]['dataParameters'][0]['userValue'] = filename
                    response = success_response(message=success_msg, data=doc)
                    json_response = JsonResponse(response, status=status.HTTP_200_OK)
                else:
                    response = no_content_response()
        else:
            response = no_content_response()
            json_response = JsonResponse(response, status=status.HTTP_200_OK)
    except Exception as ex:
        LOGGER.error(f'Exception occurred while getting single file: {ex} | {traceback.format_exc()}')
        response = internal_server_response(internal_err_msg)
        json_response = JsonResponse(response, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    write_audit(request, response, start, time.time())
    return json_response
