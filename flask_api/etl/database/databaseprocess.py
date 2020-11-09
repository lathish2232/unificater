from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine


from etl.database.save_connection import Connections
from .database_conn_validation import all_db_conn_validations as db_conn
from .settings.db_settings import DATABASE
from .metadata import meta_data
import pandas as pd
import os
import json
import datetime
from utility.logger import log


class Databaseprocess(Connections):
    def __init__(self):
        self.connection = create_engine(DATABASE['db_connection'])
        pass

    def get_datasource(self, id=None):
        if id:
            data = self.showconnectionInfo(id)
        else:
            data = self.showdatabase_Source()
        return data

    def save_user_connection_details(self, user_input):
        try:
            connection =db_conn.validate_user_connection(self,user_input)
            if connection =="successful":
                data =self.insert(user_input)
            
        except:
            print("please check the details. unable to connect")
            raise
        return data

    def get_connection_details(self, connection_name=None):
        if not connection_name:
            data = self.showConnections()
        else:
            data = self.showConnectionData(connection_name)
        return data

    def get_db_object_metadata(self, connectionid, type, schema, table):
        data = meta_data.fetch_metadata(connectionid, type, schema, table)
        return data

    def active_connections(self ,active_id):
        data = self.active_connections_validation(active_id)
        return data
    def update_datasource(self, connection_id, user_input):
        records = user_input['connectionInfo']
    # fetch datasource tables and server_id
        with self.connection.begin() as conn:
            database_id = conn.execute(
                f"select sourceid from datasource_connections where connection_id ={connection_id} ").fetchall()[0][0]
            Tables = conn.execute(
                f"select datasource_name from datasource where connection_id ={connection_id}").fetchall()[0]
        datasource_tables = [i for i in Tables]
        print(datasource_tables)
        db_keys = self.get_datasource(database_id)
        login_keys = [i['field_name'].upper()for i in db_keys]
        login_values = [i['value'] for i in records]
        data = dict(zip(login_keys, login_values))
        log.info(data)

        server = int(database_id)
        connection_name = data.get('CONNECTIONNAME', None)
        host = data.get('HOSTADDRESS', None)
        user = data.get('USERNAME', None)
        password = data.get('PASSWORD', None)
        db_name = data.get('DATABASE', None)
        portno = data.get('PORT NO', None)
        port = int(portno)if portno else portno
        server_id = data.get('SERVICE_ID', None)
        try:
            status = "successful"
            if server == 1:
                connect_str = f"dbname='{db_name}'user='{user}' host='{host}' password='{password}'"
                connection = psycopg2.connect(connect_str)
# Mysql connection
            elif server == 2:
                connection = mysql.connector.connect(
                    host=host, user=user, password=password, database=db_name)
# Oracle connection
            elif server == 3:
                connection = cx_Oracle.connect(
                    user=user, password=password, dsn=host)
# SQL server connection
            elif server == 4:
                connection = pyodbc.connect(
                    host=host, user=user, password=password, db_name=db_name, port=port, Driver='SQL Server')
# sql Lite
            elif server == 5:
                connection = sqlite3.connect(database=host)
            else:
                raise Exception("invalid server")

            if status == "successful":
                meta_cursor = connection.cursor()
                conn_name = records[0]['value']
                cur_date = datetime.datetime.now()
                with self.connection.begin() as conn:
                    update_sql = f"""update  datasource_connections
                                    set connection_name = '{conn_name}', connection_createddate ='{cur_date}'
                                    where connection_id= 28"""
                    conn.execute(update_sql)

                    for i in records:
                        rowvalue = i['value']
                        fieldid = int(i['field_Id'])
                        update_sql1 = f"""update  user_connection_details set value = '{rowvalue}'
                                        where connection_id= 28 and field_id= {field_id} """
                        conn.execute(update_sql1)
            if server in (1, 2, 4):
                select_sql = 'SELECT Distinct TABLE_NAME FROM INFORMATION_SCHEMA.columns where TABLE_NAME in' + \
                    str(tuple(datasource_tables))
                query_str = select_sql.replace(',)', ')')
                data = meta_cursor.execute(query_str).fetchall()
                meta_cursor.close()
                status = data
            elif server == 5:
                pass

        except Exception as e:
            print("Error =%s" % str(e))
            status = "Failed"
        finally:
            connection.close()
        return status

    def __del__(self):
        self.connection.dispose()
