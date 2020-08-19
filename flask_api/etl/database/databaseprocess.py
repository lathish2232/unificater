import pandas as pd
import os
import sqlite3
import pyodbc # connect to SQL server 
import mysql.connector # connect to mysql
import psycopg2 # connect to postgrasql
import cx_Oracle # connect to Oracle 
import json
from utility.logger import log
from.save_connection import Connections
from .metadata import  meta_data
from .settings.db_settings import DATABASES



class Databaseprocess(Connections):
    def __init__(self):
        #self.connection = psycopg2.connect(DATABASES['master_connection_str'])
        pass
    def get_datasource(self,id = None):
        if id:
            data = self.showconnectionInfo(id)
        else:
            data = self.showDatasource()
        return data

    def validate_user_connection(self,user_input):
     try:
        conn_values = user_input['connectionInfo']
        source_id = user_input['sourceID']

        login_keys = []
        login_values= []
        for i in conn_values:
            login_keys.append(i['fieldId'])
            login_values.append(i['value'])

        data =dict(zip(login_keys,login_values))
        log.info(data)

        server =int(source_id)
        connection_name =data['1']
        host = data['2']
        server_id = data['3']
        user = data['4']
        password = data['5']
        db_name = data['6']
        port = data['7']
        print(server,host,user,password,db_name)
        # postgrasql connection
        status = "successful"
        if server == 1:
            connect_str = f"dbname='{db_name}'user='{user}' host='{host}' password='{password}'"
            connection = psycopg2.connect(connect_str)
        #Mysql connection
        elif server == 2:
            connection = mysql.connector.connect(host=host, user=user, password=password,database=db_name)
        # Oracle connection
        elif server == 3:
            connection = cx_Oracle.connect(user=user, password=password,dsn=host)
        # SQL server connection
        elif server == 4:
            connection = pyodbc.connect(host=host,user=user, password =password,database =db_name,port=port,Driver='')
        #sql Lite
        elif server == 5:
            connection = sqlite3.connect(host =host)
        else:
            raise Exception("invalid server")

        if status =="successful":
            status= Connections.insert(self,user_input)
     except Exception as e:
        print("Error =%s"%str(e))
        status="Failed"
     finally:
        connection.close()
     return status  
    
    def get_connection_details(self, connection_name=None):
        if not  connection_name:
            data = self.showConnections()
        else:
            data = self.showConnectionData(connection_name)
        return data

    def get_db_object_metadata(self,connectionid):
        data = meta_data.fetch_metadata(self,connectionid)
        return data
