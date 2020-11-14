from sqlalchemy import create_engine
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from .settings.db_settings import DATABASE
import pyodbc
import mysql.connector
import psycopg2
import cx_Oracle
import sqlite3

class Instance_metadata():
    def __init__(self):
        self.db = create_engine(DATABASE['db_connection'])
    
    def insert(self, record):
    #insert into instance_tbl
        source_id=int(record['connection_details'][0]['connection_type_id'])
        instance_name = record['parameters'][0]['user_value']
        connection_id = int(record['connection_details'][0]['connection_id'])
        used_in_flow='no'
        unificater_flow_id=record['unificater_flow_id']
        connection_status ='valid'
        instance_error_message =None
        query_text =None
        datainstance_error_message=None
        
        if source_id==1:
            filetype =record['filetype']
            file =record['parameters'][1]['user_value']
            parameter_key=[i['field_name'] for i in record['parameters'][2:]]
            parameter_value=[i['user_value'] for i in record['parameters'][2:]]
            dict_v ={k: v for k, v in dict(zip(parameter_key ,parameter_value)).items() if v is not None}
            filepath = "r'"+file+"',"+str(dict_v).replace(':'," =").replace('{','').replace('}','').replace("'",'')
            if filetype =='csv':
                load_dataframe=eval('pd.read_csv'+'('+filepath+')')
            elif filetype== 'fwf':
                load_dataframe=eval('pd.read_fwf'+'('+filepath+')')
            with self.db.begin() as conn:
                try:
                    instance_name_check= conn.execute(f"select instance_name from instance_tbl where instance_name ='{instance_name}'").fetchall()
                    if len(instance_name_check)<=0:
                        insert_instance_tbl =f"""
                                INSERT INTO instance_tbl(instance_name,connection_id,used_in_flow,unificater_flow_id,connection_status,instance_error_message)VALUES('{instance_name}','{connection_id}','{used_in_flow}','{unificater_flow_id}','{connection_status}','{instance_error_message}')
                                """
                        conn.execute(insert_instance_tbl)
                    #insert into data_instance_table 
                        instance_id = conn.execute('select instance_id from  instance_tbl where instance_id =(select max(instance_id) from instance_tbl)').fetchall()[0][0]
                        data_instance_object_name =file.split('/')[-1].split('.')[0]
                        insert_data_instance = f"""
                                INSERT INTO data_instance(instance_id,unificater_flow_id,data_instance_object_name,connection_id,data_instance_object_type,query_text,data_instance_error_message)VALUES('{instance_id}','{unificater_flow_id}','{data_instance_object_name}','{connection_id}','{filetype}','{query_text}','{datainstance_error_message}')"""
                        conn.execute(insert_data_instance)
                    #insert into user_connection_parameters
                        data_instance_id=conn.execute(f"""
                            select data_instance_id from data_instance where instance_id= {instance_id}""").fetchall()[0][0]
                        for row in record['parameters']:
                            field_id=row['field_id']
                            field_name=row['field_name']
                            user_value=row['user_value']
                            insert_conn_parems=f"""
                            INSERT INTO user_connection_parameters(field_id,field_name,user_value,unificater_flow_id,instance_id,data_instance_id, connection_id)VALUES({field_id},'{field_name}','{user_value}',{unificater_flow_id},{instance_id},{data_instance_id},{connection_id})"""
                            conn.execute(insert_conn_parems)
                        conn.execute("delete from user_connection_parameters where user_value='None'")
                        select_sql =f"""
                        SELECT it.instance_id, instance_name, it.connection_id,c.connection_name as type,di.data_instance_object_type as sub_type,used_in_flow, it.unificater_flow_id, 
                        connection_status, instance_error_message FROM public.instance_tbl it  inner join  connections c on it.connection_id=c.connection_id inner join  public.data_instance di on di.instance_id = it.instance_id and it.connection_id =di.connection_id where it.instance_id ={instance_id}"""
                        tbl_data=conn.execute(select_sql)
                        Column =conn.execute(select_sql).keys()
                        data =[dict(zip(Column, row)) for row in tbl_data]
                    else:
                        data ="Please change the instance name, this name already available"
                except Exception as e:
                    raise e 
                finally:  
                    return data
        elif source_id==3:
            data_instance_id=0
            parameter_key=[i['field_name'].upper() for i in record['parameters']]
            parameter_value=[i['user_value'] for i in record['parameters']]
            parmes ={k: v for k, v in dict(zip(parameter_key ,parameter_value)).items() if v is not None}
            host = parmes.get('HOSTADDRESS', None)
            user = parmes.get('USERNAME', None)
            password = parmes.get('PASSWORD', None)
            db_name = parmes.get('DATABASE', None)
            portno = parmes.get('PORT NO', None)
            port = int(portno)if portno else portno
            service_id = parmes.get('SERVICE_ID', None)
            print(host,user,password,db_name,portno,service_id)
            try:
                if connection_id ==3:
                    connect_str = f"dbname='{db_name}'user='{user}' host='{host}' password='{password}'"
                    connection = psycopg2.connect(connect_str)
                elif connection_id==4:
                    connection = mysql.connector.connect(host=host, user=user, password=password, database=db_name)
                elif connection_id ==6:
                    connection = pyodbc.connect(host=host, user=user, password=password, db_name=db_name, port=port, Driver='SQL Server')
                else:
                    data =" server not listed "
                with self.db.begin() as conn:
                    instance_name_check= conn.execute(f"select instance_name from instance_tbl where instance_name ='{instance_name}'").fetchall()
                    if len(instance_name_check)<=0:
                        insert_instance_tbl =f"""
                            INSERT INTO instance_tbl(instance_name,connection_id,used_in_flow,unificater_flow_id,connection_status,instance_error_message)VALUES('{instance_name}','{connection_id}','{used_in_flow}','{unificater_flow_id}','{connection_status}','{instance_error_message}')
                                    """
                        conn.execute(insert_instance_tbl)
                        instance_id = conn.execute('select instance_id from  instance_tbl where instance_id =(select max(instance_id) from instance_tbl)').fetchall()[0][0]
                        
                        for row in record['parameters']:
                            field_id=row['field_id']
                            field_name=row['field_name']
                            user_value=row['user_value']
                            insert_conn_parems=f"""
                            INSERT INTO user_connection_parameters(field_id,field_name,user_value,unificater_flow_id,instance_id, connection_id)VALUES({field_id},'{field_name}','{user_value}',{unificater_flow_id},{instance_id},{connection_id})"""
                            conn.execute(insert_conn_parems)
                            conn.execute("delete from user_connection_parameters where user_value='None'")
                            select_sql =f"""
                            SELECT it.instance_id, instance_name, it.connection_id,ct.connection_type as type,c.connection_name as sub_type,used_in_flow, it.unificater_flow_id, connection_status, instance_error_message FROM public.instance_tbl it  inner join  connections c 
                            on it.connection_id=c.connection_id inner join connection_types ct on ct.connection_type_id=c.connection_type_id
							 where it.instance_id ={instance_id}"""
                            tbl_data=conn.execute(select_sql)
                            Column =conn.execute(select_sql).keys()
                            data =[dict(zip(Column, row)) for row in tbl_data]
                    else:
                        data ="Please change the instance name, this name already available"    
            except Exception as e:
                print(f"Error :- {e}")
            finally:
                connection.close()
            return  data
                     
    
    def delete(self, id):
        delete_sql = """
                   DELETE FROM instances WHERE instance_id= {}
                   """.format(id)

        with self.db.begin() as conn:
            conn.execute(delete_sql)  
            
    def select_instances(self,flow_id,instance_id):
        with self.db.begin() as conn:
            if flow_id and instance_id:
                select_sql=f"""
                SELECT it.instance_id, instance_name, it.connection_id,ct.connection_type as type,
                case when c.connection_name = 'Text File' then 'csv' else c.connection_name End as sub_type,used_in_flow, 
                it.unificater_flow_id, connection_status, instance_error_message FROM public.instance_tbl it  inner join  connections c on it.connection_id=c.connection_id inner join connection_types ct on ct.connection_type_id=c.connection_type_id where unificater_flow_id ={flow_id} and instance_id ={instance_id}"""
            elif flow_id:
                select_sql=f"""
                SELECT it.instance_id, instance_name, it.connection_id,ct.connection_type as type,
                case when c.connection_name = 'Text File' then 'csv' else c.connection_name End as sub_type,used_in_flow, 
                it.unificater_flow_id, connection_status, instance_error_message FROM public.instance_tbl it  inner join  connections c on it.connection_id=c.connection_id inner join connection_types ct on ct.connection_type_id=c.connection_type_id where it.instance_id ={flow_id}"""
            elif instance_id:
                select_sql=f"""
                SELECT it.instance_id, instance_name, it.connection_id,ct.connection_type as type,
                case when c.connection_name = 'Text File' then 'csv' else c.connection_name End as sub_type,used_in_flow, 
                it.unificater_flow_id, connection_status, instance_error_message FROM public.instance_tbl it  inner join  connections c on it.connection_id=c.connection_id inner join connection_types ct on ct.connection_type_id=c.connection_type_id where it.instance_id ={instance_id}"""
            else:
                select_sql= """
                SELECT it.instance_id, instance_name, it.connection_id,ct.connection_type as type,
                case when c.connection_name = 'Text File' then 'csv' else c.connection_name End as sub_type,used_in_flow, 
                it.unificater_flow_id, connection_status, instance_error_message FROM public.instance_tbl it  inner join  connections c on it.connection_id=c.connection_id inner join connection_types ct on ct.connection_type_id=c.connection_type_id"""
            tbl_data=conn.execute(select_sql)
            Column =conn.execute(select_sql).keys()
            data =[dict(zip(Column, row)) for row in tbl_data]

        return data
    
    def select_data_instances(self,flow_id,instance_id):
        with self.db.begin() as conn:
            if flow_id and instance_id:
                select_sql= f"select * from data_instance where instance_id ={instance_id} and unificater_flow_id ={flow_id} "
            tbl_data=conn.execute(select_sql)
            Column =conn.execute(select_sql).keys()
            data =[dict(zip(Column, row)) for row in tbl_data]
        return data


    def __del__(self):
        self.db.dispose()
