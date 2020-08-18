import yaml
import os
import logging
import logging.config
import json
import sys
import socket
from datetime import datetime


def init_logger():
    with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'conf\logger.yaml'), 'rt') as f:
        config = yaml.safe_load(f.read())

            # replacing the default log level from yaml file with a env value if one is set
        if os.environ.get('LOG_LEVEL') is not None:
            config.get('root') ['level'] = os.environ.get ('LOG_LEVEL')
        
    logging.config.dictConfig(config) 
   
 

def metrics_logging (request, resp, tookmsec):
    metric = {}
    log.info(f"function invoked: metricsLogging, with parameters: response[{str(resp)}], tookmsec[{str(tookmsec)}]")
    try:
        metric['authorization'] = request.headers.get ("authorization")
        metric['logtracking_id'] = request.headers.get ("logtrackingId")
        metric['app_id'] = request.headers.get ("app_id")
        metric['uri'] = request.path.encode ('utf-8')
       
        if request.method != "GET":
            metric['req_body'] = json.dumps (request.json)
        else:
            metric['req_body'] = json.dumps('')

        metric['resp_body'] = resp.data
        metric['query_params'] = json.dumps(request.args)
        metric['http_status'] = resp.status_code
        metric['type'] = 'request'
        metric['http_verb'] = request.method 
        metric['clint_ip'] = request.remort_addr 
        metric['server_hostname'] = socket.gethostname()
        metric['bytes'] = sys.getsizeof(resp.data)
        metric['tookmsec'] = int(tookmsec) 
        metric['timeout'] = None 
        metric['response_timestamp'] = datetime.now().strftime('%y-%m-%d %H:%M:%S.%f')
        metricslog.info(metric)

    except:
        log.exception('Exception while metrics logging')
#logger intialization for application logs
log =logging.getLogger('app')

#logger intialization for request response metrics lofgging
metricslog = logging.getLogger('metrics_logger')