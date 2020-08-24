import requests
import json
import os
from datetime import datetime
from utility.logger import log
import pandas as pd
from etl.database.save_connection import Connections
from etl.database.databaseprocess import Databaseprocess
from etl.database.settings.db_settings import DATABASE
import psycopg2

class DatabasesourceMiddleware:
    
    
    def __init__ (self):
        #self.connection = psycopg2.connect(DATABASES['master_connection_str'])	
        pass
         
    def run(self, request,id=None,connectionid=None,post_request=False,active_connections_ind=False):
        """
        This function stare the user feedback received from UI server
        """
        return_status = None
        result = {}

        try:
            etl_obj=Databaseprocess()

            # validate payload
            log.info(request.args)
            if post_request:
                user_input =request.json
                data=etl_obj.validate_user_connection(user_input)

            elif active_connections_ind:
                data =etl_obj.get_connection_details()
            else:
                if connectionid:
                    metadata_dict ={'schemas':'TABLE_SCHEMA','tables':'TABLE_NAME','columns':'COLUMN_NAME'}
                    type=request.args.get("type",None)
                    schema=request.args.get("schema",None)
                    table=request.args.get("table",None)

                    if request.args["type"] in metadata_dict:
                        metadata=etl_obj.get_db_object_metadata(connectionid,type,schema,table)

                        if len(metadata)>0:
                            data=list(set(map(lambda element_dict:element_dict[metadata_dict[request.args['type']]],metadata)))[int(request.args['offset']):int(request.args['limit'])]
                        else:
                            if table:
                                data = f'Table Name:- {table} not available in database, please pass correct Table name'
                            elif schema :
                                data = f'{schema} not available in database, please pass correct Schema'
                            else:
                                data ='No data rows was found'
                else:
                    source_id=None
                    if id:
                       source_id=int(id)

                    data =etl_obj.get_datasource(source_id)    
            return_status = 200
            result['status'] = 1
            result ['data']=data

        except ValueError as e:
            result = {}
            log.exception("Value Exception while submitting feedback")
            result['status'] = 0
            return_status = 400
            result ['message'] = e.args[0]
        except Exception:
            result = {}
            log.exception("Exception while submitting feedback")
            return_status = 500
            result['status'] = 0
            result ['message']  = ('Internal Error has occurred while processing the request')
        finally:
            del etl_obj
        return return_status ,result
        