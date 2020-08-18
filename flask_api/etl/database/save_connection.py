
"""
Connection class for insert,update ,delete and select
functions for metadata management
"""
import psycopg2
from psycopg2.extras import execute_values
from .settings.db_settings import DATABASES
from utility.logger import log
import os 
import json
import datetime 

class Connections():
    def __init__(self):
        self.connection = psycopg2.connect(DATABASES['master_connection_str'])	

    def insert(self,user_input):

        # get_connection name from user input  
        Connection_name=user_input['connectionInfo'][0]['value']
        source_id = int(user_input['sourceID'])
        cur_date =datetime.datetime.now()

        try:
            with self.connection:
            
            # user input storing into Two tables and genariting Connection_id
#---------------------------------------------------------------------------------------------------------------
            #checking user passing connection name in database table if it's not avilable go to if block
                cursor =self.connection.cursor()
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
                    print(columns)
                    print (values)
                    execute_values(cursor, query, values)
                    self.connection.commit()
                    result ={'result': {'success': 'true', 'error': None}, }
                    result.update(connection_id)
                    return result
                else:
                    return "connection name already available Please change the connection name"
                cursor.close()
                self.connection.close()
        except:
            self.operation_status=1
            print("insert failed")
            raise


    def update(self,connection_name,Connection_parameters):
       try:
            with self.connection:
                update_sql="""
                           update Datasource_conn_params set Connection_parameters=:Connection_parameters
                           where connection_name=:connection_name
                           """
                self.connection.execute(update_sql,{'connection_name':connection_name,'Connection_parameters':Connection_parameters})
       except:
            self.operation_status=1

    def delete(self,connection_name):
       try:
            with self.connection:
                 delete_sql="""
                            delete from Datasource_conn_params where Source_ID=:Source_ID
                            """
                 self.connection.execute(delete_sql,{'Source_ID':Source_ID})
       except:
            self.operation_status=1

    def showDatasource(self):
        try:
            cursor=self.connection.cursor()
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
        except:
           self.operation_status=0
           return "failed"
        finally:
            self.connection.close() 

    def showconnectionInfo(self,id):
        try:
            cursor=self.connection.cursor()
            select_sql=f"""
                      select field_id,field_name,type,isrequired from Req_connection_details where server_id ={id}
                       """
            cursor.execute(select_sql)
            result=cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            self.result=[]
            for row in result:
                self.result.append(dict(zip(colnames,row)))
            return self.result
        except:
           self.operation_status=0
           return "failed"
        finally:
            self.connection.close()  


    def showConnections(self):
        try:
            cursor=self.connection.cursor()
            select_sql="""
                      select * from datasource_conn_params
                       """
            cursor.execute(select_sql)
            result=cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            self.result=[]
            for row in result:
                self.result.append(dict(zip(colnames,row)))
            return self.result
        except:
           self.operation_status=0
           return "failed"
        finally:
            self.connection.close()
            
    def showConnectionData(self,connection_name):
        try:
            cursor=self.connection.cursor()
            select_sql=f"""
                       select * from Guid_Connections where Connection_name='{connection_name}'
                       """
            cursor.execute(select_sql)
            result=cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            self.result=[]
            for row in result:
                self.result.append(dict(zip(colnames,row)))
            if len(self.result)==0:
                return "connection_name not available"
            else:
                return self.result
        except:
            self.operation_status=0
            return "failed"
        finally:
            self.connection.close()
    def __del__(self):
        pass
        #self.connection.close()