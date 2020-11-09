import pyodbc
import mysql.connector
import psycopg2
import cx_Oracle
import sqlite3

from etl.database.save_connection import Connections
from utility.logger import log

class all_db_conn_validations(Connections):
    def validate_user_connection(self, user_input):
        try:
            conn_values = user_input['connectionInfo']
            source_id = user_input['id']
            db_keys = self.showconnectionInfo(source_id)
        
            login_keys=[i['field_name'].upper() for i in db_keys]
            login_values=[i['value'] for i in conn_values]

            data = dict(zip(login_keys, login_values))
            log.info(data)

            server = int(source_id)
            connection_name = data.get('CONNECTIONNAME', None)
            host = data.get('HOSTADDRESS', None)
            user = data.get('USERNAME', None)
            password = data.get('PASSWORD', None)
            db_name = data.get('DATABASE', None)
            portno = data.get('PORT NO', None)
            port = int(portno)if portno else portno
            service_id = data.get('SERVICE_ID', None)
            print(server, host, user, password, db_name)

# postgrasql connection
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
                status =" server not listed "
        except Exception as e:
            print(e)
            raise
        finally:
            connection.close()
        return status
            