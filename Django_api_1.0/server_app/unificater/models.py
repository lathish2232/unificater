# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class ConnectionParameters(models.Model):
    field_id = models.BigIntegerField(blank=True, null=True)
    connection_id = models.BigIntegerField(blank=True, null=True)
    function_name = models.TextField(blank=True, null=True)
    source_connection_name = models.TextField(blank=True, null=True)
    is_fixed_width = models.BooleanField(blank=True, null=True)
    field_name = models.TextField(blank=True, null=True)
    parameter_data_type = models.TextField(blank=True, null=True)
    type = models.TextField(blank=True, null=True)
    is_required = models.BooleanField(blank=True, null=True)
    parameter_default_value = models.TextField(blank=True, null=True)
    help = models.TextField(blank=True, null=True)
    return_type = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'connection_parameters'


class ConnectionTypes(models.Model):
    connection_type_id = models.IntegerField()
    connection_type = models.CharField(max_length=1)

    class Meta:
        managed = False
        db_table = 'connection_types'


class Connections(models.Model):
    connection_id = models.BigIntegerField(primary_key=True)
    connection_name = models.TextField(blank=True, null=True)
    connection_type_id = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'connections'


class DataInstance1(models.Model):
    data_instance_id = models.AutoField(primary_key=True)
    instance_id = models.IntegerField(blank=True, null=True)
    unificater_flow_id = models.IntegerField()
    data_instance_object_name = models.CharField(max_length=1000)
    connection_id = models.IntegerField(blank=True, null=True)
    data_instance_object_type = models.CharField(max_length=100, blank=True, null=True)
    query_text = models.TextField(blank=True, null=True)
    data_instance_object_status = models.CharField(max_length=100, blank=True, null=True)
    data_instance_error_message = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'data_instance1'


class DatabaseSources(models.Model):
    id = models.IntegerField(primary_key=True)
    type = models.CharField(max_length=100, blank=True, null=True)
    name = models.CharField(unique=True, max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'database_sources'


class Datasource(models.Model):
    datasource_id = models.BigAutoField(primary_key=True)
    datasource_name = models.TextField()
    sqlview = models.TextField(blank=True, null=True)
    connection = models.ForeignKey('DatasourceConnections', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'datasource'


class DatasourceConnections(models.Model):
    connection_id = models.IntegerField(primary_key=True)
    connection_name = models.CharField(unique=True, max_length=250)
    connection_createddate = models.DateTimeField(blank=True, null=True)
    sourceid = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'datasource_connections'


class InstanceTbl1(models.Model):
    instance_id = models.AutoField(primary_key=True)
    instance_name = models.CharField(max_length=1000, blank=True, null=True)
    connection_id = models.IntegerField(blank=True, null=True)
    used_in_flow = models.CharField(max_length=20, blank=True, null=True)
    unificater_flow_id = models.IntegerField(blank=True, null=True)
    connection_status = models.CharField(max_length=1000, blank=True, null=True)
    instance_error_message = models.CharField(max_length=1000, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'instance_tbl1'


class ReqConnectionDetails(models.Model):
    field_id = models.IntegerField(blank=True, null=True)
    field_name = models.CharField(max_length=250, blank=True, null=True)
    type = models.CharField(max_length=250, blank=True, null=True)
    isrequired = models.CharField(max_length=250, blank=True, null=True)
    server_name = models.CharField(max_length=250, blank=True, null=True)
    server = models.ForeignKey(DatabaseSources, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'req_connection_details'


class UnificaterFlows1(models.Model):
    unificater_flow_id = models.AutoField(primary_key=True)
    unificater_flow_name = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=100, blank=True, null=True)
    created_date = models.DateTimeField(blank=True, null=True)
    modified_by = models.CharField(max_length=100, blank=True, null=True)
    modified_date = models.DateTimeField(blank=True, null=True)
    unificater_flow_latest_version_id = models.IntegerField(blank=True, null=True)
    sub_flows_exist = models.CharField(max_length=100, blank=True, null=True)
    updatable = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'unificater_flows1'

class UserConnectionParameters(models.Model):
    field_id = models.IntegerField()
    field_name = models.CharField(max_length=100)
    user_value = models.CharField(max_length=1000, blank=True, null=True)
    unificater_flow_id = models.IntegerField(blank=True, null=True)
    instance = models.ForeignKey(InstanceTbl1, models.DO_NOTHING, blank=True, null=True)
    data_instance = models.ForeignKey(DataInstance1, models.DO_NOTHING, blank=True, null=True)
    connection = models.ForeignKey(Connections, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'user_connection_parameters'