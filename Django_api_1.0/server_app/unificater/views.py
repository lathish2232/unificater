from django.shortcuts import render
from rest_framework import viewsets
from .serializers import *
from .models import *
from rest_framework.decorators import api_view,permission_classes
from rest_framework.response import Response
from django.http.response import JsonResponse
from rest_framework import permissions
from datetime import datetime
import random
from django.views.decorators.csrf import csrf_exempt
#user modules
from .utility.logger import init_logger, log, metrics_logging
from .middleware.unificater_flow import UniflowMiddleware
from .middleware.instance import InstancesMiddleware
from .etl.database.uni_metadata import uni_metadata

init_logger()

# Create your views here.

def flows(request):
    if request.method == 'GET':
            flow_name ='unificater flow '+str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            created_by=None
            data={}
            new_flow=UnificaterFlows1(unificater_flow_name=flow_name,created_date=created_date,created_by=created_by)
            new_flow.save()
            ser=UnificaterFlowSerializers(new_flow)
            data["data"]=[ser.data]
            return JsonResponse(data,status=200)

    elif  request.method == 'POST':
        existing_flow=UnificaterFlows1.objects.get(unificater_flow_name=request.data["flow_name"])
        if not existing_flow.unificater_flow_name:
            existing_flow=UnificaterFlows1.objects.get(unificater_flow_id=request.data["flow_id"])
            existing_flow.unificater_flow_name=request.data["flow_name"]
            existing_flow.save()
            res={'data':"flow created successfully"}
        else:
           res={'data':"Name already available in unificater flow, please provide diffrent name"}
        return JsonResponse(res,status=200)


class FlowsViewSet(viewsets.ModelViewSet):
    queryset=UnificaterFlows1.objects.all().order_by('-unificater_flow_id')
    serializer_class=UnificaterFlowSerializers

@csrf_exempt
@api_view(['POST','GET'])    
def unificaterflow(request,id=None):
    if request.method == 'GET':
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = UniflowMiddleware()
            return_status, result = fp.run(request)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = JsonResponse(result,status=return_status)
        return resp

    elif  request.method == 'POST':
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            id=request.GET.get("flow_id", None)
            if not id:
                raise ValueError
            fp = UniflowMiddleware()
            return_status, result = fp.run(request,id)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = JsonResponse(result,status=return_status)
        return resp

@csrf_exempt
@api_view(['POST','GET'])   
def  allflows(request,id=None):
    if request.method == 'GET':
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            id=request.GET.get("flow_id", None)
            fp = UniflowMiddleware()
            return_status, result = fp.active(request,id)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = JsonResponse(result,status=return_status)
        return resp
    
@csrf_exempt
@api_view(['POST','GET'])
def connections(request,id=None,connection_id=None,isfwf=None):
    if request.method == 'GET':
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = UniflowMiddleware()
            id=request.GET.get("id", None)
            connection_id=request.GET.get("connection_id", None)
            isfwf=request.GET.get("isfwf", None)
            return_status, result = fp.run_connection(request,id,connection_id,isfwf)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = JsonResponse(result,status=return_status)
        return resp
    elif  request.method == 'POST':
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = UniflowMiddleware()
            return_status, result = fp.run_connection(request,True)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = JsonResponse(result,status=return_status)
        return resp


@csrf_exempt
@api_view(['POST','GET'])
def Instances(request):
    if request.method == 'GET':
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = InstancesMiddleware()
            return_status, result = fp.run(request, False)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = JsonResponse(result,status=return_status)
        return resp
    elif  request.method == 'POST':
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = InstancesMiddleware()
            return_status, result = fp.run(request, True)
        except Exception as e:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['message'] = 'Internal Error has Occurred while processing the File request'
            result['status'] = 0
        finally:
            resp = JsonResponse(result,status=return_status)
        return resp


@csrf_exempt
@api_view(['POST','GET'])
def  data_instance(request):
    if request.method == 'GET':
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = InstancesMiddleware()
            return_status, result = fp.data_instance(request)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = JsonResponse(result,status=return_status)
        return resp    
    
