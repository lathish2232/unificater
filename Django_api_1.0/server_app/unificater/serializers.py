from .models import *
from rest_framework import serializers


class ConnectionParametersSerializers(serializers.ModelSerializer):
    class Meta:
        model=ConnectionParameters


class ConnectionTypesSerializers(serializers.ModelSerializer):
    class Meta:
        model=ConnectionTypes
        #fields = '__all__'


class ConnectionsSerializers(serializers.ModelSerializer):
    class Meta:
        model=Connections
        #fields = '__all__'

class DataInstanceSerializers(serializers.ModelSerializer):
    class Meta:
        model=DataInstance1
        #fields = '__all__'


class DatabaseSourcesSerializers(serializers.ModelSerializer):
    class Meta:
        model=DatabaseSources
        #fields = '__all__'


class DatasourceSerializers(serializers.ModelSerializer):
    class Meta:
        model=Datasource
        #fields = '__all__'

class DatasourceConnectionsSerializers(serializers.ModelSerializer):
    class Meta:
        model=DatasourceConnections
        #fields = '__all__'

class InstanceTblSerializers(serializers.ModelSerializer):
    class Meta:
        model=InstanceTbl1
        #fields = '__all__'


class ReqConnectionDetailsSerializers(serializers.ModelSerializer):
    class Meta:
        model=ReqConnectionDetails
        #fields = '__all__'

class UserConnectionParametersSerializers(serializers.ModelSerializer):
    class Meta:
        model=UserConnectionParameters
        #fields = '__all__'

class UnificaterFlowSerializers(serializers.ModelSerializer):
    class Meta:
        model=UnificaterFlows1
        fields = '__all__'