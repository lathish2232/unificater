from psycopg2 import  OperationalError
from mysql.connector import OperationalError 
import mysql.connector
import psycopg2

from service.util.unify_response import success_response,unauthorized
from rest_framework import status
from django.http import JsonResponse
from service.impl.flow_instance import Mongo_DB_Connector, FlowData
from service.util.json_utils import extract_sub_json


class instance_db_validation(Mongo_DB_Connector,FlowData):
    
    def __init__(self):
        pass
    def validate_db_conn(self,request_body,collection,keys,ufid):
        record = self.db[collection].find_one({}, {'_id': 0})
        if request_body['type'] == 'database':
            dbtype = request_body['name']
            login_keys = [i['fieldName'].upper() for i in request_body['connectionParameters']]
            login_values = [i['userValue'] for i in request_body['connectionParameters']]
            data = dict(zip(login_keys, login_values))
            host = data.get('HOSTADDRESS', None)
            user = data.get('USERNAME', None)
            password = data.get('PASSWORD', None)
            portno = data.get('PORT NO', '')
            port = int(portno) if portno else portno
            # service_id = data.get('SERVICE_ID', None)
            
            if dbtype == 'postgreSql':
                try:
                    connect_str = f"user='{user}' host='{host}' password='{password}' port='{port}'"
                    connection = psycopg2.connect(connect_str)
                    connection.close()
                except Exception as error:
                    return JsonResponse(unauthorized(error=str(error).rstrip()), status=status.HTTP_401_UNAUTHORIZED)
            elif dbtype =='mySql':
                try:
                    connection= mysql.connector.connect(host=host, user=user, password=password)
                    connection.close()
                except mysql.connector.errors.InterfaceError as error:
                    return JsonResponse(unauthorized(error=str(error).rstrip()), status=status.HTTP_401_UNAUTHORIZED)
            self.insertMasterJsonItems(record, keys, 0, request_body)
            self.db[collection].update_one({f'{collection}.UFID': ufid}, {'$set': record})
            data = self.db[collection].find_one({}, {'_id': 0})
            return JsonResponse(success_response(data=data), status=status.HTTP_200_OK)   
           