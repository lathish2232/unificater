import logging
from datetime import datetime, time

from service.util.http_constances import timeout, success_msg, no_data

LOGGER = logging.getLogger('audit')


def success_response(message=success_msg, data=None):
    if data:
        if not isinstance(data, list):
            data = [data]
        return {"code": 200, "message": message, "data": data}
    else:
        return {"code": 200, "message": message}


def success_create_response(message, data=None):
    if data:
        return {"code": 201, "message": message, "data": data}
    else:
        return {"code": 201, "message": message}


def success_delete_response(message):
    return {"code": 202, "message": message}


def no_content_response(message=no_data):
    return {"code": 204, "message": message}


def validation_error_response(message):
    return {"code": 400, "message": message}


def not_accepted_response(message):
    return {"code": 406, "message": message}


def time_out_response(message=timeout):
    return {"code": 408, "message": message}


def duplicate_content_response(message):
    return {"code": 409, "message": message}


def internal_server_response(message):
    return {"code": 500, "message": message}


def write_audit(request, response, starts, ends):
    start_dt = datetime.fromtimestamp(starts).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    end_dt = datetime.fromtimestamp(ends).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    elapsed = round((ends - starts) / 1000, 3)
    LOGGER.info(
        f'{request.path} | {request.method} | {response.get("code")} | {elapsed} | {start_dt} | {end_dt} | {response.get("message")}')

def  unauthorized(error):
    return{"code": 401,"error":error}
