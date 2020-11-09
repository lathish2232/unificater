import requests
import json
import os
from datetime import datetime
from utility.logger import log
import pandas as pd

from etl.database.instance_metadata import Instance_metadata

class InstancesMiddleware:
    
    def __init__(self):
        #self.connection = psycopg2.connect(DATABASES['master_connection_str'])
        pass

    def run(self, request, post_request=False):
        """     
        This function stare the user feedback received from UI server
        """
        return_status = None
        result = {}
        flow_id = request.args.get('unificater_flow_id',None)
        instance_id= request.args.get('instance_id',None)
        try:
            etl_obj = Instance_metadata()
            # validate payload
            if post_request:
                if request.json:
                    record =request.json
                    data =etl_obj.insert(record)
            else:
                data = etl_obj.select_instances(flow_id,instance_id)
            return_status = 200
            result['status'] = 1
            result['data'] = data
        except ValueError as e:
            result = {}
            log.exception("Value Exception while submitting feedback")
            result['status'] = 0
            return_status = 400
            result['message'] = e.args[0]
        except Exception as e:
            result = {}
            log.exception("Exception while submitting feedback")
            return_status = 500
            result['status'] = 0
            result['message'] = ("Internal Error has Occurred while processing the File request")
        finally:
            del etl_obj
        return return_status, result
            
            
    def data_instance(self, request):
        return_status = None
        result = {}
        flow_id = request.args.get('unificater_flow_id',None)
        instance_id= request.args.get('instance_id',None)
        etl_obj = Instance_metadata()
        try:
            data = etl_obj.select_data_instances(flow_id,instance_id)
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
