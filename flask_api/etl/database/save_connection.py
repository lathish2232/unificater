
"""
Connection class for insert,update ,delete and select
functions for metadata management
"""
import psycopg2
from psycopg2.extras import execute_values
from .settings.db_settings import DATABASE
from utility.logger import log
import os 
import json
import datetime 

class Connections():
    def __init__(self):
        #self.create_connection()
        pass
    def create_connection(self):
        global connection,cursor
        connection = psycopg2.connect(DATABASE['db_connection'])
        cursor = connection.cursor()
    def close_connection(self):
        cursor.close()
        connection.close()
    def insert(self,user_input):
        self.create_connection()
        # get_connection name from user input  
        Connection_name=user_input['connectionInfo'][0]['value']
        source_id = int(user_input['sourceID'])
        cur_date =datetime.datetime.now()

        try:           
            # user input storing into Two tables and genariting Connection_id
#---------------------------------------------------------------------------------------------------------------
            #checking user passing connection name in database table if it's not avilable go to if block
                #cursor =self.connection.cursor()
                
                cursor.execute(f"select connection_id from datasource_connections where Connection_name ='{Connection_name}'")
                data = cursor.fetchall()
#-----------------------------------------------------------------------------------------------------------------------    
                if len(data)==0:
                    insert_sql=f"""
                        insert into datasource_connections(Connection_name,connection_createddate,source_id) values
                        ('{Connection_name}','{cur_date}','{source_id}')
                    """
                    cursor.execute(insert_sql)

          # getting system genarated connection_id to pass UI 
                    connectio_ID_sql=f"""
                    select connection_id from datasource_connections 
                    where source_id = {source_id} and connection_name ='{Connection_name}'
                    """
                    cursor.execute(connectio_ID_sql)
                    data=cursor.fetchall()
                    colnames = [desc[0] for desc in cursor.description]
                    result=[]
                    for row in data:
                        result.append(dict(zip(colnames,row)))
                    connection_id = result[0]
#---------------------------------------------------------------------------------------------------------------------
            # Serialize user input(json object) to store values in table  
                    user_values =[]
                    for i in user_input['connectionInfo']:
                        i.update({"sourceID":user_input["sourceID"]})
                        i.update(connection_id)
                        user_values.append(i)
                    for i in user_values:
                        for key in i:
                            i['fieldId'] =int(i['fieldId'])
                            i['sourceID'] =int(i['sourceID'])
                        
                    columns = user_values[0].keys()
                    query = "INSERT INTO datasource_conn_params ({}) VALUES %s".format(','.join(columns))
                    values = [[value for value in rows.values()] for rows in user_values]

                    execute_values(cursor, query, values)
                    connection.commit()
                    result ={'result': {'success': 'true', 'error': None}, }
                    result.update(connection_id)
                    return result
                else:
                    return "connection name already available Please change the connection name"
                cursor.close()
        except Exception as e:
            self.operation_status=1
        finally:
             self.close_connection()

    def showDatasource(self):
        try:
            self.create_connection()
            select_sql="""
                      select * from datasource
                       """
            cursor.execute(select_sql)
            result=cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            self.result=[]
            for row in result:
                self.result.append(dict(zip(colnames,row)))
            return self.result
        except Exception as e:
           self.operation_status=0
           return "oops something thing went wrong, unable to fetch datasource"
        finally:
           self.close_connection() 

    def showconnectionInfo(self,id):
        try:
            self.create_connection()
            select_sql=f"""
                      select field_id,field_name,type,isrequired from Req_connection_details where server_id ={id}
                       """
            cursor.execute(select_sql)
            result=cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            res=[]
            for row in result:
                res.append(dict(zip(colnames,row)))
            return res
        except Exception as e:
           self.operation_status=0
           return "failed"
        finally:
           self.close_connection() 
  
    def showConnections(self):#show active connections
        try:
            self.create_connection()
            select_sql="""
                      select * from datasource_conn_params
                       """
            cursor.execute(select_sql)
            data=cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            result=[]
            for row in data:
                result.append(dict(zip(colnames,row)))
            return result
        except:
           self.operation_status=0
           return "unable to connect database"

    def __del__(self):
        pass
        #self.connection.close()