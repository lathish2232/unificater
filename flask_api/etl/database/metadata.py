import sqlite3
import pyodbc  # connect to SQL connection_type
import cx_Oracle  # connect to Oracle
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

    def fetch_metadata(connectionid, type, schema, table):
        try:
            metadata_connection = create_engine(DATABASE['db_connection'])
            select_sql = f"""
                        select dc.sourceid,dc.connection_name,RCD.field_name,value ,uc.connection_id 
                        from public.datasource_connections as dc
                        inner join public.database_Sources as ds
                        on dc.sourceid = ds.id
                        inner join user_connection_details as uc
                        on dc.sourceid = uc.id
                        inner join req_connection_details RCD
                        on RCD.field_id = uc.field_id and RCD.server_id =uc.id
                        where uc.connection_id ={connectionid} and dc.connection_id = {connectionid}
                        """
            Tbldata = metadata_connection.execute(select_sql)
            colnames = metadata_connection.execute(select_sql).keys()
            result = []
            for row in Tbldata:
                result.append(dict(zip(colnames, row)))
            data = result
            if len(data) == 0:
                return "connection details not available"
            else:
                login_keys = [i['field_name'].upper() for i in data]
                login_values = [i['value'] for i in data]
                server_id = data[0]['sourceid']
                
                data = dict(zip(login_keys, login_values))
                host = data.get('HOSTADDRESS', None)
                user = data.get('USERNAME', None)
                password = data.get('PASSWORD', None)
                db_name = data.get('DATABASE', None)
                portno = data.get('PORT NO', None)
                port = int(portno)if portno else portno
                service_id = data.get('SERVICE_ID', None)

                print(server_id, host, user, password, db_name, port)
    # postgres metadata
                meta_data = []

                if server_id == 1:
                    try:
                        connect_str = f"postgres://{user}:{password}@{host}/{db_name}"
                        postgres_conn = create_engine(connect_str)
                        if type == "schemas":
                            metadata_select_sql = """
                                    SELECT Distinct TABLE_SCHEMA FROM INFORMATION_SCHEMA.columns
                                    ORDER BY  TABLE_SCHEMA;
                                    """
                        elif type == "tables":
                            metadata_select_sql = f"""
                                    SELECT Distinct TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.columns
                                    WHERE table_schema = '{schema}' ORDER BY table_name;
                                    """
                        elif type == "columns":
                            metadata_select_sql = f"""
                                   SELECT TABLE_SCHEMA,TABLE_NAME ,COLUMN_NAME FROM INFORMATION_SCHEMA.columns
                                   WHERE table_schema = '{schema}' and TABLE_NAME ='{table}' ORDER BY ORDINAL_POSITION;
                                    """
                        result = postgres_conn.execute(metadata_select_sql)
                        columnnames = postgres_conn.execute(
                            metadata_select_sql).keys()
                        # convert columnnames to upper
                        colnames = [i.upper() for i in columnnames]
                        for row in result:
                            meta_data.append(dict(zip(colnames, row)))
                    except:
                        print(" fetching method was failed")
                        raise
                    finally:
                        postgres_conn.dispose()
    # Mysql metadata
                elif server_id == 2:
                    try:
                        connect_str = f"mysql://{user}:{password}@{host}/{db_name}"
                        mysql_conn = create_engine(connect_str)
                        if type == "schemas":
                            select_sql = r"""
                                    Select Distinct TABLE_SCHEMA from INFORMATION_SCHEMA.columns;
                                    """
                        elif type == "tables":
                            select_sql = f"""
                                    Select Distinct TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.columns where TABLE_SCHEMA ='{schema}' ;
                                    """
                        elif type == "columns":
                            select_sql = f"""
                                    Select Distinct TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME from INFORMATION_SCHEMA.columns 
                                    where TABLE_SCHEMA ='{schema}' and TABLE_NAME='{table}';
                                    """
                        result = mysql_conn.execute(select_sql)
                        columnnames = mysql_conn.execute(select_sql).keys()
                        colnames = [i.upper() for i in columnnames]
                        for row in result:
                            meta_data.append(dict(zip(colnames, row)))

                    except:
                        print("fetching method was failed from Mysql")
                        raise
                    finally:
                        mysql_conn.dispose()
# Ms-sql server metadata
                elif server_id == 3:
                    pass
                elif server_id == 4:
                    try:
                        connect_str = f"mssql+pyodbc://{user}:{password}@{host}/{db_name}?driver=SQL Server"
                        mssql_conn = create_engine(connect_str)
                        if type == "schemas":
                            select_sql = f"""
                                        SELECT Distinct TABLE_SCHEMA FROM {db_name}.INFORMATION_SCHEMA.columns order by TABLE_SCHEMA;
                                        """
                        elif type == "tables":
                            select_sql = f"""
                                        SELECT TABLE_SCHEMA,TABLE_NAME FROM {db_name}.INFORMATION_SCHEMA.columns
                                        Where TABLE_SCHEMA ='{schema}' order by TABLE_NAME;
                                        """
                        elif type == "columns":
                            select_sql = f"""
                                        SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.columns Where TABLE_SCHEMA ='{schema}' and TABLE_NAME='{table}'
                                        order by ORDINAL_POSITION;
                                        """
                        result = mssql_conn.execute(select_sql)
                        colnames = mssql_conn.execute(select_sql).keys()
                        for row in result:
                            meta_data.append(dict(zip(colnames, row)))

                    except:
                        print("fetching method was failed from MS-SQLSERVER")
                        raise
                    finally:
                        mssql_conn.dispose()
                elif server_id == 5:
                    try:
                        sqlite_conn = create_engine(f"sqlite:///{host}")
                        if type == "schemas":
                            select_sql = f'select distinct type as TABLE_SCHEMA from sqlite_master'
                        elif type == "tables":
                            select_sql = f'select name as TABLE_NAME from sqlite_master'
                        elif type == "columns":
                            select_sql = f'select * from {table} where 1=0'
                        result = sqlite_conn.execute(select_sql)
                        colnames = sqlite_conn.execute(select_sql).keys()
                        if type == "columns":
                            meta_data = colnames
                        else:
                            for row in result:
                                meta_data.append(dict(zip(colnames, row)))
                    except:
                        print("fetching method was failed from sqlite")
                    finally:
                        sqlite_conn.dispose()
                return meta_data

        except Exception as e:
            print("connection failed=%s" % str(e))
            return "connection failed server closed the connection unexpectedly"
            raise
        finally:
            metadata_connection.dispose()
