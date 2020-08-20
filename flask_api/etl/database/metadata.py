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

class meta_data(Connections):
    def __init__(self):
        pass

    def fetch_metadata(connectionid,type,schema,table):
        try:
            metadata_connection = psycopg2.connect(DATABASES['master_connection_str'] )
            metadata_cursor=metadata_connection.cursor()
            select_sql=f"""
                        select dc.source_id,dc.connection_name,DCP.fieldId,value ,DCP.connection_id from public.datasource_connections as dc
                        inner join public.datasource as ds
                        on source_id = id
                        inner join datasource_conn_params as DCP
                        on dc.source_id = DCP.sourceid
                        where DCP.connection_id ={connectionid} and dc.connection_id = {connectionid}
                        """
            metadata_cursor.execute(select_sql)
            Tbldata=metadata_cursor.fetchall()
            colnames = [desc[0] for desc in metadata_cursor.description]
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
                meta_data=[]
                if server_id ==1:
                    try:
                        connect_str = f"dbname= '{db_name}' user='{user}' host='{host}' password='{password}' "
                        postgres_conn = psycopg2.connect(connect_str)
                        if type=="schemas":
                            metadata_select_sql ="""
                                    SELECT Distinct TABLE_SCHEMA FROM INFORMATION_SCHEMA.columns
                                    ORDER BY  TABLE_SCHEMA
                                    """
                        elif type=="tables":
                             metadata_select_sql =f"""
                                    SELECT Distinct TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.columns
                                    WHERE table_schema = '{schema}' ORDER BY table_name 
                                    """
                        elif type =="columns":
                            print("i am in ----------------")
                            metadata_select_sql =f"""
                                   SELECT TABLE_SCHEMA,TABLE_NAME ,COLUMN_NAME FROM INFORMATION_SCHEMA.columns
                                   WHERE table_schema = '{schema}' and TABLE_NAME ='{table}' ORDER BY ORDINAL_POSITION
                                    """
                        print(metadata_select_sql)
                        metadata_cursor.execute(metadata_select_sql)
                        result=metadata_cursor.fetchall()
                        columnnames = [desc[0] for desc in metadata_cursor.description]
                        colnames = [i.upper() for i in columnnames] #convert columnnames to upper
                        for row in result:
                            meta_data.append(dict(zip(colnames,row)))
                        metadata_cursor.close()
                        postgres_conn.close()
                    except Exception as e:
                        print(str(e))
    #Mysql metadata
                elif server_id == 2:
                    try:

                        Mysql_conn = mysql.connector.connect(user=user,password=password,host=host,database=db_name)
                        mysql_cursor = Mysql_conn.cursor(dictionary=True)
                        if type=="schemas":
                            select_sql =r"""
                                    Select Distinct TABLE_SCHEMA from INFORMATION_SCHEMA.columns;
                                    """
                        elif type=="tables":
                            select_sql =f"""
                                    Select Distinct TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.columns where TABLE_SCHEMA ='{schema}' ;
                                    """
                        elif type =="columns":
                            select_sql =f"""
                                    Select Distinct TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME from INFORMATION_SCHEMA.columns 
                                    where TABLE_SCHEMA ='{schema}' and TABLE_NAME='{table}' ;
                                    """
                        mysql_cursor.execute(select_sql)
                        meta_data=mysql_cursor.fetchall()
                        print(meta_data)
                        mysql_cursor.close()
                        Mysql_conn.close()
                    except:
                        print("failed to fetch")
                        raise                  
#sql server
                elif server_id ==4:
                    try:
                        mssql_conn = pyodbc.connect(host=host,user=user,password=password,db_name=db_name,port=port,Trusted_connection='yes',Driver='ODBC Driver 13 for SQL Server')
                        mssql_cursor=mssql_conn.cursor()
                        select_sql =f"""
                                        SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION, DATA_TYPE FROM {db_name}.INFORMATION_SCHEMA.columns order by TABLE_NAME, ORDINAL_POSITION
                                        """
                        mssql_cursor.execute(select_sql)
                        data =mssql_cursor.fetchall()
                        colnames = [desc[0] for desc in mssql_cursor.description]
                        for row in data:
                            meta_data.append(dict(zip(colnames,row)))
                        mssql_cursor.close()
                        mssql_conn.close()
                    except:
                        print("failed to fetch")
                        raise
                return meta_data
        except Exception as e:
            print("connection failed=%s"%str(e))
            return "connection failed server closed the connection unexpectedly"  
        finally:
            metadata_cursor.close()
            metadata_connection.close()
