import requests
import json
import os
from datetime import datetime
from utility.logger import log
import pandas as pd
from etl.database.save_connection import Connections
from etl.database.databaseprocess import Databaseprocess
from etl.database.settings.db_settings import DATABASES
import psycopg2

class DatabasesourceMiddleware:
    
    
    def __init__ (self):
        self.connection = psycopg2.connect(DATABASES['master_connection_str'])	
         
    def run(self, request,id=None,connectionid=None,post_request=False,active_connections_ind=False):
        """
        This function stare the user feedback received from UI server
        """
        return_status = None
        result = {}

        try:
            # validate payload
            data=log.info(request.args)
            if post_request:
                user_input =request.json
                data=Databaseprocess.validate_user_connection(self,user_input)

            elif active_connections_ind:
                data =Databaseprocess.get_connection_details(self)
            else:
                if id:
                    data =Databaseprocess.get_datasource(self,id)

                elif connectionid:
                    metadata_dict ={'schemas':'TABLE_SCHEMA','tables':'TABLE_NAME','columns':'COLUMN_NAME'}
                    if request.args["type"] in metadata_dict :
                        metadata=Databaseprocess.get_db_object_metadata(self,connectionid)
                        print(metadata)
                        data=list(set(map(lambda element_dict:element_dict[metadata_dict[request.args['type']]],metadata)))[0:10]#[request.args['offset']:request.args['limit']]
                        
                    else:
                        data ="please check type value in request"
                else:
                    data =Databaseprocess.get_datasource(self)   
                
            return_status = 200
            result['status'] = 1
            result ['data']=data

        except ValueError as e:
            result = {}
            log.exception("Value Excerticon while submitting feedback")
            result['status'] = 0
            return_status = 400
            result ['message'] = e.args[0]
        except Exception:
            result = {}
            log.exception("Exception while submitting feedback")
            return_status = 500
            result['status'] = 0
            result ['message']  = ("Internal Error has occurred while processing the request")

        return return_status ,result