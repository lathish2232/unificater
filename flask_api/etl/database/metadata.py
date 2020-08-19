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

    def fetch_metadata(self,connectionid):
        try:
            metadata_cursor=self.connection.cursor()
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
            metadata_cursor.close()
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
                connection=None
                if server_id ==1:
                    try:
                        connect_str = f"dbname= '{db_name}' user='{user}' host='{host}' password='{password}' "
                        postgres_conn = psycopg2.connect(connect_str)
                        postgres_cursor = postgres_conn.cursor()
                        select_sql ="""
                                    SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE FROM INFORMATION_SCHEMA.columns
                                    WHERE table_schema = 'public' ORDER BY table_name ,ORDINAL_POSITION
                                    """
                        postgres_cursor.execute(select_sql)
                        result=postgres_cursor.fetchall()
                        columnnames = [desc[0] for desc in postgres_cursor.description]
                        colnames = [i.upper() for i in columnnames] #convert columnnames to upper
                        result=[]
                        for row in result:
                            result.append(dict(zip(colnames,row)))
                        postgres_cursor.close()
                        postgres_conn.close()
                        data=result
                    except:
                        status = 'postgres fetch failed'
                    return status
    #Mysql metadata
                elif server_id == 2:
                    try:
                        Mysql_conn = mysql.connector.connect(user=user,password=password,host=host,database=db_name)
                        mysql_cursor = Mysql_conn.cursor(dictionary=True)
                        select_sql =r"""
                                    SELECT  TN.TABLE_SCHEMA,TN.TABLE_TYPE,TN.TABLE_NAME,TC.COLUMN_NAME,TC.DATA_TYPE  FROM INFORMATION_SCHEMA.columns as TC
                                    inner join INFORMATION_SCHEMA.Tables as TN
                                    on TC.TABLE_CATALOG =TN.TABLE_CATALOG and TC.Table_name = Tn.Table_name
                                    WHERE  TN.Table_type in ('BASE TABLE','VIEW') AND 
                                    TN.TABLE_SCHEMA not in('mysql','information_schema','performance_schema','sys') 
                                    order by Table_name;
                                    """
                        mysql_cursor.execute(select_sql)
                        result=mysql_cursor.fetchall()
                        data= result
                        mysql_cursor.close()
                        Mysql_conn.close()
                    except:
                        status = 'mysql_data fetch failed'
                    return status
                   
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
                        result=[]
                        for row in data:
                            result.append(dict(zip(colnames,row)))
                        data= result
                        mssql_cursor.close()
                        mssql_conn.close()
                    except:
                        status = 'mysql_data fetch failed'
                    return status
                print(data)
                return data

        except:
            print("connection failed")
            return "connection failed server closed the connection unexpectedly"  
            

