import sqlite3
import pyodbc # connect to SQL connection_type 
import mysql.connector # connect to mysql
import psycopg2 # connect to postgrasql
from psycopg2.extras import execute_values
import cx_Oracle # connect to Oracle 
import json
from utility.logger import log 
from .settings.db_settings import DATABASES
from .save_connection import Connections

class meta_data:
    def __init__(self):
        #self.connection = psycopg2.connect(DATABASES['master_connection_str'] )
        pass
    def fetch_metadata(self,connectionid):
        try:
            master_conn_str = Connections()
            cursor=master_conn_str.connection.cursor()
            select_sql=f"""
                        select dc.source_id,dc.connection_name,DCP.fieldId,value ,DCP.connection_id from public.datasource_connections as dc
                        inner join public.datasource as ds
                        on source_id = id
                        inner join datasource_conn_params as DCP
                        on dc.source_id = DCP.sourceid
                        where DCP.connection_id ={connectionid} and dc.connection_id = {connectionid}
                        """
            cursor.execute(select_sql)
            Tbldata=cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            result=[]
            for row in Tbldata:
                result.append(dict(zip(colnames,row)))
            data=result
 
            if len(data)==0:
                return "connection details not available"
            else:
                login_keys =[]
                login_values=[]
                for i in data:
                    source_id = i['source_id']
                    login_keys.append(i['fieldid'])
                    login_values.append(i['value'])
                data =dict(zip(login_keys,login_values))

                server_id =source_id
                host = data[2]
                user = data[4]
                password = data[5]
                db_name = data[6]
                port = data[7]

                print(server_id,host,user,password,db_name)
    #postgres metadata   
                if server_id ==1:
                    connect_str = f"dbname= '{db_name}' user='{user}' host='{host}' password='{password}' "
                    connection = psycopg2.connect(connect_str)
                    cursor = connection.cursor()
                    select_sql ="""
                                SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE FROM INFORMATION_SCHEMA.columns
                                WHERE table_schema = 'public' ORDER BY table_name ,ORDINAL_POSITION
                                """
                    cursor.execute(select_sql)
                    result=cursor.fetchall()
                    columnnames = [desc[0] for desc in cursor.description]
                    colnames = [i.upper() for i in columnnames] #convert columnnames to upper
                    result=[]
                    for row in result:
                        result.append(dict(zip(colnames,row)))
                    data=result
                    cursor.close()
                    connection.close()
                if not connection:
                    status ="failed"
                    return status
    #Mysql metadata
                elif server_id == 2:
                    connection = mysql.connector.connect(user=user,password=password,host=host,database=db_name)
                    cursor = connection.cursor(dictionary=True)
                    select_sql =r"""
                                SELECT  TN.TABLE_SCHEMA,TN.TABLE_TYPE,TN.TABLE_NAME,TC.COLUMN_NAME,TC.DATA_TYPE  FROM INFORMATION_SCHEMA.columns as TC
                                inner join INFORMATION_SCHEMA.Tables as TN
                                on TC.TABLE_CATALOG =TN.TABLE_CATALOG and TC.Table_name = Tn.Table_name
                                WHERE  TN.Table_type in ('BASE TABLE','VIEW') AND 
                                TN.TABLE_SCHEMA not in('mysql','information_schema','performance_schema','sys') 
                                order by Table_name;
                                """
                    cursor.execute(select_sql)
                    result=cursor.fetchall()
                    data= result
                    cursor.close()
                if not connection:
                    status = "failed"
                    return status

                elif server_id ==4:

                    connection = pyodbc.connect(host=host,user=user,password=password,db_name=db_name,port=port,Trusted_connection='yes',Driver='ODBC Driver 13 for SQL Server')
                    cursor=connection.cursor()
                    select_sql =f"""
                                    SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION, DATA_TYPE FROM {db_name}.INFORMATION_SCHEMA.columns order by TABLE_NAME, ORDINAL_POSITION
                                    """
                    cursor.execute(select_sql)
                    data =cursor.fetchall()
                    colnames = [desc[0] for desc in cursor.description]
                    result=[]
                    for row in data:
                        result.append(dict(zip(colnames,row)))
                    data= result
                    print(data)
                    cursor.close()
                    connection.close()
                if not connection:
                    status = "failed"

                return data

        except Exception as e:
            print(e)
            return 'quesry issue' 


