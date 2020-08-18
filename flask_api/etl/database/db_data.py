import sqlite3
import pyodbc # connect to SQL connection_type 
import mysql.connector # connect to mysql
import psycopg2 # connect to postgrasql
import cx_Oracle # connect to Oracle 
import json
from utility.logger import log 
from .settings.db_settings import DATABASES

class table_data:
    def __init__(self):
        self.connection = psycopg2.connect(DATABASES['postgresql'])
    
    def dict_factory(self,cursor, row):
        #this function fech column names in sqllite3
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def get_table_data(self,connection_name,connection_type,table_name):
        try:
            cursor=self.connection.cursor()
            select_sql="""
                        select * from Guid_Connections where (Connection_name=:connection_name and connection_type=:connection_type)
                        """
            cursor.row_factory=self.dict_factory
            self.result=cursor.execute(select_sql,{'connection_name':connection_name,'connection_type':connection_type})
            data ={}
            for i in self.result:
                data.update(i)
            if len(data)==0:
                return "connection details not available"
            else:
                connection_name = data['connection_name']
                connection_type = data['connection_type']
                host = data['host']
                user = data['username']
                password = data['password']
                db_name = data['database_name']
                port = data['port']
                Driver = data['driver']
    # ms_sql fetch table values
                if connection_type =='sqlserver':
                    connection = pyodbc.connect(host=host,user=user,password=password,db_name=db_name,port=port,Trusted_connection='yes',Driver=Driver)
                    cursor=connection.cursor()
                    select_sql =f"""
                                    SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME FROM {db_name}.INFORMATION_SCHEMA.Tables
                                    where TABLE_NAME='{table_name}'
                                    """
                    cursor.execute(select_sql)
                    data =cursor.fetchall()
                    if len(data)==0:
                        return"please check the table name"
                    else:
                        database =data[0][0]
                        schema =data[0][1]
                        table_name = data[0][2]
                        select_sql =f"""
                                    select Top 10 * from {database}.{schema}.{table_name} ;
                                    """
                        print(select_sql)
                        result=cursor.execute(select_sql)
                        colnames = [desc[0] for desc in cursor.description]
                        self.result=[]
                        for row in result:
                            self.result.append(dict(zip(colnames,row)))
                        data= self.result

    #postgres fetch table values   
                elif connection_type =='postgresql':

                    connect_str = f"dbname= '{db_name}' user='{user}' host='{host}' password='{password}' "
                    connection = psycopg2.connect(connect_str)
                    cursor = connection.cursor()
                    select_sql =f"""
                                select *  from {db_name}.information_schema.Tables
                                where table_name ='{table_name}' 
                                """
                    cursor.execute(select_sql)
                    data =cursor.fetchall()
                    if len(data)==0:
                        return"please check the table name"
                    else:
                        print('i am in')
                        database =data[0][0]
                        schema =data[0][1]
                        table_name = data[0][2]
                        select_sql =f"""
                                    select * from {database}.{schema}.{table_name} Limit 10;
                                    """
                        cursor.execute(select_sql)
                        result=cursor.fetchall()
                        colnames = [desc[0] for desc in cursor.description]
                        self.result=[]
                        for row in result:
                            self.result.append(dict(zip(colnames,row)))
                        data=self.result
                    
    #Mysql fetch table values
                elif connection_type =='mysql':
                    connection = mysql.connector.connect(user=user,password=password,host=host,database=db_name)
                    cursor = connection.cursor(dictionary=True)
                    select_sql =f"""
                                select TABLE_SCHEMA,TABLE_NAME from information_schema.Tables
                                where TABLE_SCHEMA ='{db_name}'and table_name ='{table_name}';
                                """
                    cursor.execute(select_sql)
                    data =cursor.fetchall()
                    if len(data)==0:
                        return"please check the table name"
                    else:
                        database =data[0]['TABLE_SCHEMA']
                        table_name =data[0]['TABLE_NAME']
                        select_sql =f"""
                                    select * from {database}.{table_name} ;
                                    """
                        cursor.execute(select_sql)
                        print('hello i am in ')
                        self.result=cursor.fetchall()
                        data= self.result 
                        print(data)   
                if not connection:
                    status = "failed"
                    return status  
                connection.close()
                return data     
        except Exception as e:
            print(e)
            return 'quesry issue'             