import requests
import json
from utility.logger import log
from etl.database.uni_metadata import uni_metadata
import asyncio

loop = asyncio.get_event_loop()
class UniflowMiddleware:

    def __init__(self):
        pass
    def run(self, request,id=None, post_request=False):
        """     
        This function stare the user feedback received from UI server
        """
        return_status = None
        result = {}
        etl_obj =uni_metadata()
        try:
            if id:
                record=request.json
                data =etl_obj.rename_unificaterflow(id,record)
            else:
                data= etl_obj.create_unificaterflow()
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

    def active(self,request, id=None):
    
        return_status = None
        result = {}
        etl_obj =uni_metadata()
        try:
            if id:
                data =etl_obj.select_unificaterflow(id)
            else:
                data= etl_obj.select_unificaterflow(id=None)
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
    
    def run_connection(self,request,id,connection_id,isfwf,post_request=False):
        return_status = None
        result = {}
        etl_obj =uni_metadata()
        data ={'connection_details':None,'parameters':None}
        try:
            if post_request:
                if id:
                    print('hello')
            else:
                print(id,connection_id)
                if id and connection_id and isfwf:
                    data["connection_details"]=etl_obj.select_connection_and_connectionTypes(id,connection_id)
                    data['parameters']= etl_obj.get_connection_paraeters(id,connection_id,isfwf)
                elif id and connection_id:
                    id=int(id)
                    connection_id=int(connection_id)
                    if id==1 or connection_id==1:
                        d={"connection_details":None,"filename":None,"filetype":None,"parameters":None}
                        data = loop.run_until_complete(etl_obj.file_browser(d,id,connection_id))
                    else:
                        data["connection_details"]=etl_obj.select_connection_and_connectionTypes(id,connection_id)
                        data['parameters']= etl_obj.get_connection_paraeters(id,connection_id,isfwf)
                elif id:
                    data=etl_obj.select_connections(id)
                else:
                    data= etl_obj.select_connection_types()
                
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