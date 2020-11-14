from sqlalchemy import create_engine
from sqlalchemy import Column, String, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime

from tkinter import *
from tkinter import filedialog 
import asyncio
import os 

from .settings.db_settings import DATABASE

class uni_metadata():

    def __init__(self):
        self.db = create_engine(DATABASE['db_connection'])
        
    def create_unificaterflow(self):
        with self.db.begin() as conn:
            flow_name ='unificater flow '+str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            created_by=None
            insert_sql=f"""
                insert into unificater_flows(unificater_flow_name,created_by,created_date) 
                values('{flow_name}','{created_by}','{created_date}')
            """
            conn.execute(insert_sql)
            select_sql= """select unificater_flow_id as id,unificater_flow_name as name, created_by from unificater_flows
                            where unificater_flow_id=(select MAX(unificater_flow_id) from unificater_flows)
                        """
            tbl_data = conn.execute(select_sql)
            Column  = conn.execute(select_sql).keys()
            data =[dict(zip(Column, row)) for row in tbl_data]
        return data
    
    def select_unificaterflow(self,id):
        with self.db.begin() as conn:
            if id:
                select_sql =f"""select unificater_flow_id as id,unificater_flow_name as name, created_by 
                    from unificater_flows where unificater_flow_id ={id}"""
            else:
                select_sql =f"""select unificater_flow_id as id,unificater_flow_name as name, created_by 
                    from unificater_flows """
            tbl_data=conn.execute(select_sql)
            Column =conn.execute(select_sql).keys()
            data =[dict(zip(Column, row)) for row in tbl_data]
        return data
           
    def rename_unificaterflow(self,id,record):
        with self.db.begin() as conn:
            select_sql=f"""select unificater_flow_name from unificater_flows 
                            where unificater_flow_name='{record['flow_name']}'"""
            result =conn.execute(select_sql).fetchall()
            if len(result)==0:
                update_sql=f"""update unificater_flows
                                set unificater_flow_name ='{record['flow_name']}'
                                where unificater_flow_id={id}
                            """
                conn.execute(update_sql)
                data ='Name updated successfully'
            else:
                data ="Name already available in unificater flow, please provide diffrent name"
        return data
    
    def select_connection_types(self):
        with self.db.begin() as conn:
            select_sql ="select * from connection_types order by connection_type_id"
            tbl_data=conn.execute(select_sql)
            Column =conn.execute(select_sql).keys()
            data =[dict(zip(Column, row)) for row in tbl_data]
        return data
    
    def select_connections(self,id):
        with self.db.begin() as conn:
            select_sql =f"select * from connections where connection_type_id ={id} order by connection_id"
            tbl_data=conn.execute(select_sql)
            Column =conn.execute(select_sql).keys()
            data =[dict(zip(Column, row)) for row in tbl_data]
        return data
    def select_connection_and_connectionTypes(self,id,connection_id):
        with self.db.begin() as conn:
            select_sql =f"""select ct.connection_type_id,ct.connection_type,c.connection_id, c.connection_name 
            from connection_types ct inner join connections c on c.connection_type_id =ct.connection_type_id
            where ct.connection_type_id= {connection_id} and connection_id={id}
                """
            tbl_data=conn.execute(select_sql)
            Column =conn.execute(select_sql).keys()
            data =[dict(zip(Column, row)) for row in tbl_data]
        return data
        
    
    async def file_browser(self, d, id,connection_id):
        try:
            filename=None
            root =Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            filename=filedialog.askopenfilenames(parent=root,title="unificater file Browser",filetypes=[("All Files/pattern", "*")])
            filename=filename[0]
            root.destroy()
            d["filename"]=filename
            d["filetype"] =os.path.splitext(filename)[1][1:]
            d["connection_details"]=self.select_connection_and_connectionTypes(id,connection_id)
            d["parameters"]=self.get_connection_paraeters(id,connection_id,isfwf=None)
        except:
            print("Failure")
        finally:
            return d
    
    def get_connection_paraeters(self,id,connection_id,isfwf):
        with self.db.begin() as conn:
            if isfwf =='true':
                select_sql=f"""
                select distinct cp.field_id,cp.field_name,cp.return_type as user_value, cp.parameter_data_type,
                cp.type,cp.is_required,cp.parameter_default_value,cp.help 
                from connection_parameters cp
                inner join connections c on c.connection_id = {connection_id}
                inner join connection_types ct on c.connection_type_id ={id}
                where cp.connection_id ={connection_id} and is_fixed_width='{isfwf}' order by field_id"""
            elif connection_id ==1:
                select_sql= f"""
                select distinct cp.field_id,cp.field_name,cp.return_type as user_value, cp.parameter_data_type,
                cp.type,cp.is_required,cp.parameter_default_value,cp.help 
                from connection_parameters cp
                inner join connections c on c.connection_id = {connection_id}
                inner join connection_types ct on c.connection_type_id ={id}
                where cp.connection_id ={connection_id} and is_fixed_width='false'  order by field_id"""
            else:
                select_sql= f"""
                select distinct cp.field_id,cp.field_name,cp.return_type as user_value,cp.parameter_data_type,
                cp.type,cp.is_required,cp.parameter_default_value,cp.help 
                from connection_parameters cp
                inner join connections c on c.connection_id = {connection_id}
                inner join connection_types ct on c.connection_type_id ={id}
                where cp.connection_id ={connection_id} order by field_id"""
                
            tbl_data=conn.execute(select_sql)
            Column =conn.execute(select_sql).keys()
            data =[dict(zip(Column, row)) for row in tbl_data]
        return data
    
    def __del__(self):
        self.db.dispose()
