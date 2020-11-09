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

    def __init__(self):
        #self.connection = psycopg2.connect(DATABASES['master_connection_str'])
        pass

    def run(self, request,active_id=None, id=None, connectionid=None, post_request=False, active_connections_ind=False, operation=None):
        """
        This function stare the user feedback received from UI server
        """
        return_status = None
        result = {}

        try:
            etl_obj = Databaseprocess()
            if request.get:
                print('hello')
            
        
            return_status = 200
            result['status'] = 1
            result['data'] = data

        except ValueError as e:
            result = {}
            log.exception("Value Exception while submitting feedback")
            result['status'] = 0
            return_status = 400
            result['message'] = e.args[0]
        except Exception:
            result = {}
            log.exception("Exception while submitting feedback")
            return_status = 500
            result['status'] = 0
            result['message'] = (
                'Internal Error has occurred while processing the request')
        finally:
            del etl_obj
        return return_status, result
