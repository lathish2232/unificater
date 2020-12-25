
from service.util.unify_response import success_response,unauthorized
from rest_framework import status
from django.http import JsonResponse

from service.impl.main_uniflow import FlowData
from service.util.db_utils import Mongo_DB_Connector
from service.util.json_utils import extract_sub_json
from service.util.user_db_conn_validation import instance_db_validation

conn_validation=instance_db_validation()

class Data_source(Mongo_DB_Connector, FlowData):

    def __init__(self):
        pass

    def deleteItems(self, urlpath):
        collection = urlpath.split('/')[1]
        record = self.db[collection].find_one({}, {'_id': 0})
        ufid = record[collection]['UFID']
        keys = extract_sub_json(urlpath, record)[1]

        self.deleteMasterJsonItems(record, keys)
        self.db[collection].update_one({f'{collection}.UFID': ufid}, {'$set': record})
        return record

    def updateItems(self, request):
        collection = request.get_full_path().split('/')[1]
        urlpath = request.get_full_path().split('/?')[-1]
        values = request.data
        record = self.db[collection].find_one({}, {'_id': 0})
        ufid = record[collection]['UFID']
        keys = extract_sub_json(urlpath, record)[1]
        self.updateMasterJsonItems(record, keys, values)
        self.db[collection].update_one({f'{collection}.UFID': ufid}, {'$set': record})
        return JsonResponse(success_response(data="updated successfylly"),status=status.HTTP_200_OK)

    def createFlow(self, request):
        print('>>'*30)
        d = {'id': None}
        url = request.get_full_path()
        collection = url.split('/')[1]
        request_body = request.data
        record = self.db[collection].find_one({}, {'_id': 0})
        ufid = record[collection]['UFID']
        _, keys, _, json = extract_sub_json(url, record)
        id = len(json) + 1
        d['id'] = url.split('/')[-1] + "Id_" + str(id)
        request_body.update(d)
        if url.endswith('/instances'):
            if request_body["type"]=="database":
                return conn_validation.validate_db_conn(request_body,collection,keys,ufid)
      
            else:
                self.insertMasterJsonItems(record, keys, 0, request_body)
                self.db[collection].update_one({f'{collection}.UFID': ufid}, {'$set': record})
                data = self.db[collection].find_one({}, {'_id': 0})
                return JsonResponse(success_response(data=data), status=status.HTTP_200_OK)
