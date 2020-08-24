import sqlite3
import pyodbc # connect to SQL connection_type 
import cx_Oracle # connect to Oracle 
import json
from utility.logger import log 
from .settings.db_settings import DATABASE
from .save_connection import Connections

from sqlalchemy import create_engine  
from sqlalchemy import Column, String  
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

class meta_data(Connections):
    def __init__(self):
        pass

    def fetch_metadata(connectionid,type,schema,table):
        try:
            metadata_connection = create_engine(DATABASE['db_connection'] )
            select_sql=f"""
                        select dc.source_id,dc.connection_name,DCP.fieldId,value ,DCP.connection_id from public.datasource_connections as dc
                        inner join public.datasource as ds
                        on source_id = id
                        inner join datasource_conn_params as DCP
                        on dc.source_id = DCP.sourceid
                        where DCP.connection_id ={connectionid} and dc.connection_id = {connectionid}
                        """
            Tbldata=metadata_connection.execute(select_sql)
            colnames = metadata_connection.execute(select_sql).keys()
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
                #port = int(data[7])

                print(server_id,host,user,password,db_name)
    #postgres metadata   
                meta_data=[]

                if server_id ==1:
                    try:
                        connect_str = f"postgres://{user}:{password}@{host}/{db_name}"
                        postgres_conn = create_engine(connect_str)
                        if type=="schemas":
                            metadata_select_sql ="""
                                    SELECT Distinct TABLE_SCHEMA FROM INFORMATION_SCHEMA.columns
                                    ORDER BY  TABLE_SCHEMA;
                                    """
                        elif type=="tables":
                             metadata_select_sql =f"""
                                    SELECT Distinct TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.columns
                                    WHERE table_schema = '{schema}' ORDER BY table_name;
                                    """
                        elif type =="columns":
                            metadata_select_sql =f"""
                                   SELECT TABLE_SCHEMA,TABLE_NAME ,COLUMN_NAME FROM INFORMATION_SCHEMA.columns
                                   WHERE table_schema = '{schema}' and TABLE_NAME ='{table}' ORDER BY ORDINAL_POSITION;
                                    """
                        result=postgres_conn.execute(metadata_select_sql)
                        columnnames = postgres_conn.execute(metadata_select_sql).keys()
                        colnames = [i.upper() for i in columnnames] #convert columnnames to upper
                        for row in result:
                            meta_data.append(dict(zip(colnames,row)))
                    except:
                        print(" fetching method was failed")
                    finally:
                        postgres_conn.dispose() 
    #Mysql metadata
                elif server_id == 2:
                    try:
                        connect_str=f"mysql://{user}:{password}@{host}/{db_name}"
                        mysql_conn = create_engine(connect_str)
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
                                    where TABLE_SCHEMA ='{schema}' and TABLE_NAME='{table}';
                                    """
                        result=mysql_conn.execute(select_sql)
                        columnnames = mysql_conn.execute(select_sql).keys()
                        colnames = [i.upper() for i in columnnames]
                        for row in result:
                            meta_data.append(dict(zip(colnames,row)))
                        
                    except:
                        print("fetching method was failed from Mysql")
                        raise
                    finally:
                        mysql_conn.dispose()
# Ms-sql server metadata
                elif server_id ==4:
                    try:
                        connect_str =f"mssql+pyodbc://{user}:{password}@{host}/{db_name}?driver=SQL+Server+Native+Client+11.0"
                        mssql_conn=create_engine(connect_str)
                        if type=="schemas":
                            select_sql =f"""
                                        SELECT Distinct TABLE_SCHEMA FROM {db_name}.INFORMATION_SCHEMA.columns order by TABLE_SCHEMA;
                                        """
                        elif type=="tables":
                            select_sql =f"""
                                        SELECT TABLE_SCHEMA,TABLE_NAME FROM {db_name}.INFORMATION_SCHEMA.columns
                                        Where TABLE_SCHEMA ='{schema}' order by TABLE_NAME;
                                        """
                        elif type =="columns":
                            select_sql =f"""
                                        SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.columns Where TABLE_SCHEMA ='{schema}' and TABLE_NAME='{table}'
                                        order by ORDINAL_POSITION;
                                        """
                        result =mssql_conn.execute(select_sql)
                        columnnames = mssql_conn.execute(select_sql).keys()
                        for row in result:
                            print(row)
                            meta_data.append(dict(zip(columnnames,row)))
                    except:
                        print("fetching method was failed from MS-SQLSERVER")
                        raise
                    finally:
                        mssql_conn.dispose()

                return meta_data                 

        except Exception as e:
            print("connection failed=%s"%str(e))
            return "connection failed server closed the connection unexpectedly"  
        finally:
            metadata_connection.dispose()
