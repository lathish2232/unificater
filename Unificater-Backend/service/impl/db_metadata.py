from sqlalchemy import create_engine

from service.impl.flow_instance import Data_source, extract_sub_json
from service.util.unify_response import success_response,unauthorized,no_content_response
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
            id=1
            type=urlpath.split('/')[-1]
            try:
                if  msJson['name']=='postgreSql':
                    connect_str = f"postgres://{user}:{password}@{host}/{dbname}"
                    connection = create_engine(connect_str)
                    if info:
                        metaType =info.get('info','').split('>')[-1]
                        schema = info.get('info','').split('>')[1]
                        if metaType == "schemas":
                            sql = 'select catalog_name as "dbName",schema_name as "schemaName" from information_schema.schemata;'
                        elif metaType == "tables":
                            sql = f"""
                                select distinct table_catalog as "dbName", table_schema as "tableSchema",TABLE_NAME as "tableName" from INFORMATION_SCHEMA.columns
                                WHERE table_schema = '{schema}' ORDER BY table_name;"""
                        elif metaType == "columns":
                            tName =info.get('info','').split('>')[2]
                            sql = f"""
                                    select table_catalog as "dbName" ,table_schema as "tableSchema",table_name as "tableName" ,COLUMN_NAME as"columnName", data_type as"dataType" from INFORMATION_SCHEMA.columns
                                    WHERE table_schema = '{schema}' and table_name='{tName}' ORDER BY ORDINAL_POSITION;"""
                        idtype=metaType[:-1]
                    elif type =="database":
                        sql="""SELECT datname as "dbName" FROM pg_database
                                            WHERE datistemplate = false;"""
                        idtype =type
                
                elif  msJson['name']=='mySql':
                    connect_str = f"mysql://{user}:{password}@{host}"
                    connection = create_engine(connect_str)
                    if info:
                        metaType =info.get('info','').split('>')[-1]
                        table = info.get('info','').split('>')[1]
                        schema=info.get('info','').split('>')[0]
                        if metaType == "tables":
                            sql = f"""
                                    Select Distinct table_schema as dbName,table_name as tableName 
                                    from INFORMATION_SCHEMA.columns where table_schema ='{schema}' ;"""
                        elif metaType == "columns":
                            sql = f"""Select Distinct table_schema as dbName,table_name as tableName,column_name as columnName ,data_Type as dataType
                                    from INFORMATION_SCHEMA.columns where table_schema ='{schema}' and table_name='{table}'
                                    order by Ordinal_Position;"""
                        idtype=metaType[:-1]             
                    elif type =="database": 
                        sql="select distinct table_schema as dbName from information_schema.tables"
                        idtype =type
                result = connection.execute(sql)
                columnnames = [i for i in connection.execute(sql).keys()]
                for row in result:
                    meta_data.append(dict(zip(columnnames,row)))
                for item in meta_data:
                    item.update({'id':idtype+'Id_'+str(id)})
                    id=id+1 
                if len(meta_data)==0:
                    return JsonResponse(no_content_response(), status=status.HTTP_204_NO_CONTENT)
                else:
                    return JsonResponse(success_response(data=meta_data), status=status.HTTP_200_OK)
            except Exception as error:
                return JsonResponse(unauthorized(error=str(error).rstrip()), status=status.HTTP_401_UNAUTHORIZED)        
            finally:
                connection.dispose()

    def show_table_data(self,url_path):
        record={}
        url='/'+'/'.join(url_path.split('/')[1:-1])
        master_json=self.getCollectionData(url_path)
        dataInstances_json=extract_sub_json(url,master_json)[3]
        record['file_data']=data_instance(dataInstances_json)
        return JsonResponse(success_response(data=record), status=status.HTTP_200_OK)