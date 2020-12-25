from sqlalchemy import create_engine

from service.impl.flow_instance import Data_source, extract_sub_json
from service.util.unify_response import success_response
from service.util.json_utils import data_instance
from rest_framework import status
from django.http import JsonResponse


class db_metadata(Data_source):
    def __init__(self):
        pass

    def getCollectionData(self, urlpath):
        collection = urlpath.split('/')[1]
        record = self.db[collection].find_one({}, {'_id': 0})
        return record

    def get_metadata(self,urlpath,request):
        print('>>>>>'*20)
        record =self.getCollectionData(urlpath)
        url=urlpath.split('/database',1)[0]
        msJson =extract_sub_json(url,record)[3]
        info =request.data
        if msJson['type'] =='database':
            login_keys=[i['fieldName'].upper() for i in msJson['connectionParameters']]
            login_values=[i['userValue'] for i in msJson['connectionParameters']]
            data = dict(zip(login_keys, login_values))

            host = data.get('HOSTADDRESS', None)
            user = data.get('USERNAME', None)
            password = data.get('PASSWORD', None)
            portno = data.get('PORT NO', '')
            port = int(portno)if portno else portno
            dbname =info.get('info','').split('>')[0]
            #service_id = data.get('SERVICE_ID', None)
            meta_data = []
            type=urlpath.split('/')[-1]
            if  msJson['name']=='postgreSql':
                try:
                    connect_str = f"postgres://{user}:{password}@{host}/{dbname}"
                    postgres_conn = create_engine(connect_str)
                    if dbname:
                        metaType =info.get('info','').split('>')[-1]
                        schema = info.get('info','').split('>')[1]
                        
                        if metaType == "schemas":
                            metadata_select_sql = 'select schema_name as "schemaName" from information_schema.schemata;'
                        elif metaType == "tables":
                            metadata_select_sql = f"""
                                SELECT Distinct TABLE_SCHEMA as "tableSchema",TABLE_NAME as "tableName" FROM INFORMATION_SCHEMA.columns
                                WHERE table_schema = '{schema}' ORDER BY table_name;"""
                        elif metaType == "columns":
                            tName =info.get('info','').split('>')[2]
                            metadata_select_sql = f"""
                                    SELECT TABLE_SCHEMA as "tableSchema",TABLE_NAME as "tableName" ,COLUMN_NAME as"columnName", data_type as"dataType" FROM INFORMATION_SCHEMA.columns
                                    WHERE table_schema = '{schema}' and TABLE_NAME='{tName}' ORDER BY ORDINAL_POSITION;"""
                    elif type =="database":
                        metadata_select_sql="""SELECT datname as "dbName" FROM pg_database
                                                WHERE datistemplate = false;"""
                    result = postgres_conn.execute(metadata_select_sql)
                    columnnames = [i for i in postgres_conn.execute(metadata_select_sql).keys()]
                    id=1
                    print(columnnames,result)
                    for row in result:
                        meta_data.append(dict(zip(columnnames,row)))
                    print(meta_data)
                    for item in meta_data:
                         item.update({'id':id})
                         id=id+1    
                except:
                    print(" fetching method was failed")
                    raise
                finally:
                    postgres_conn.dispose()
            return JsonResponse(success_response(data=meta_data), status=status.HTTP_200_OK)
    
    def show_table_data(self,url_path):
        record={}
        url='/'+'/'.join(url_path.split('/')[1:-1])
        master_json=self.getCollectionData(url_path)
        dataInstances_json=extract_sub_json(url,master_json)[3]
        record['file_data']=data_instance(dataInstances_json)
        return JsonResponse(success_response(data=record), status=status.HTTP_200_OK)