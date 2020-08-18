import requests
import json
import os
from datetime import datetime
from utility.logger import log
import pandas as pd


from etl.files.fileprocess import Fileprocess

class FilesourceMiddleware:
    def __init__ (self, filesource_parser):
        self.args = filesource_parser.parse_args()
    def run(self, request):
        """
        This function stores the user feedback received from Ul server
        """
        return_status = None
        result = {}
        log.info(f"args: (str(self.args)")
        try:
            # validate payload
            log.info(request.args)
            log.info(f"input file: {request.json['file']}")
            
            
            etl_obj=Fileprocess (request.json)

            msg=etl_obj.validate()
            if not etl_obj.validate() == "successful":
                raise ValueError (msg)

            if request.args["file_source_type" ]=="excel":
                data = etl_obj.excel_file_process()

            else:
                data = etl_obj.non_excel_file_process ()

            return_status = 200
            result['status'] = 1
            result ['data']=data

        except ValueError as e:
            result = {}
            log.exception("Value Excerticon while submitting feedback")
            result['status'] = 0
            return_status = 400
            result ['message'] = e.args[0]

        except:
            result ={}
            log.exception("Exception while submitting feedback")
            return_status =500
            result['status'] =0
            result['message'] ='internal error has occured while processing the request'
        
        return return_status,result