from django.shortcuts import render
from django.contrib.auth.models import User,Group
from django.contrib.auth import authenticate, logout, login
from rest_framework import viewsets
from rest_framework.decorators import api_view,permission_classes
from rest_framework.response import *
from django.http import JsonResponse
from rest_framework import permissions
from django.views.decorators.csrf import csrf_exempt,ensure_csrf_cookie
import json

from .core.flow_instances import unificaterflows
from .core.Read_flow_data import FlowData
# Create your views here.
flow_obj = unificaterflows()
read_obj =FlowData()
data={}
@csrf_exempt
@api_view(['POST','GET'])
def create_flow(request):
    if request.method =='GET':
        data =flow_obj.create_flow()
    elif request.method == 'POST':
        data =flow_obj.update(request.data)
    return Response(data, status = 200)

def select_connectiontypes(request):
    connection_type = request.GET.get('type',None)
    isfwf =request.GET.get('isfwf',None)
    id = request.GET.get('id',None)
    if request.method =='GET':
        if connection_type and isfwf:
            data =flow_obj.select_collections(connection_type ,isfwf)
        elif isfwf =='true':
            data=flow_obj.select_collections(connection_type ,isfwf)
        elif connection_type =='database' and id=='3':
            data =flow_obj.get_db_connection_paraeters(id)
        else:
            data=flow_obj.select_collections(connection_type ,isfwf)
    return JsonResponse(data, status = 200)

@csrf_exempt
@api_view(['POST','GET'])
def create_instances(request):
    if request.method == 'POST':
        request_body =request.data
        data =flow_obj.create_instance(request_body)
    if request.method == 'GET':
        data_ins_id=request.GET.get('id',None)
        data=read_obj.read_file_data(data_ins_id)
    return Response(data, status = 200)

def select_allflows(request):
    if request.method == 'GET':
        data=read_obj.get_allflows()
    return JsonResponse(data, status = 200)
    



