
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
        # self.create_connection()
        pass

    def create_connection(self):
        global connection, cursor
        connection = psycopg2.connect(DATABASE['db_connection'])
        cursor = connection.cursor()

    def close_connection(self):
        cursor.close()
        connection.close()

    def insert(self, user_input):
        self.create_connection()
        # get_connection name from user input
        Connection_name = user_input['connectionInfo'][0]['value']
        source_id = int(user_input['id'])
        cur_date = datetime.datetime.now()

        try:
            # user input storing into Two tables and genariting Connection_id
            # checking user passing connection name in database table if it's not avilable go to if block
            self.create_connection()
            cursor.execute(
                f"select connection_id from datasource_connections where Connection_name ='{Connection_name}'")
            data = cursor.fetchall()
# ---------------------------------------------------------------------------------------------------------------------
            if len(data) == 0:
                insert_sql = f"""
                    insert into datasource_connections(Connection_name,connection_createddate,sourceid) values
                    ('{Connection_name}','{cur_date}',{source_id})
                """
                cursor.execute(insert_sql)

        # getting system genarated connection_id to pass UI
                connectio_ID_sql = f"""
                select connection_id from datasource_connections 
                where sourceid = {source_id} and connection_name ='{Connection_name}'
                """
                cursor.execute(connectio_ID_sql)
                data = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                result = []
                for row in data:
                    result.append(dict(zip(colnames, row)))
                connection_id = result[0]
# ---------------------------------------------------------------------------------------------------------------------
        # Serialize user input(json object) to store values in table
                user_values = []
                for i in user_input['connectionInfo']:
                    i.update({"id": user_input["id"]})
                    i.update(connection_id)
                    user_values.append(i)
                for i in user_values:
                    for key in i:
                        i['field_id'] = int(i['field_id'])
                        i['id'] = int(i['id'])

                columns = user_values[0].keys()
                query = "INSERT INTO user_connection_details ({}) VALUES %s".format(
                    ','.join(columns))
                values = [[value for value in rows.values()]
                          for rows in user_values]
                execute_values(cursor, query, values)
                connection.commit()
                result = {'result': {'success': 'true', 'error': None}, }
                result.update(connection_id)
                return result
            else:
                return "connection name already available Please change the connection name"
            cursor.close()
        except Exception as e :
            return "Error:- %s" %str(e)
            raise
        finally:
            self.close_connection()

    def showdatabase_Source(self):
        try:
            self.create_connection()
            select_sql = """
                      select * from database_Sources
                       """
            cursor.execute(select_sql)
            result = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            self.result = []
            for row in result:
                self.result.append(dict(zip(colnames, row)))
            return self.result
        except Exception as e:
            return "oops something thing went wrong, unable to fetch database_Sources"
            raise
        finally:
            self.close_connection()

    def showconnectionInfo(self, id):
        try:
            self.create_connection()
            select_sql = f"""
                      select field_id,field_name,type,isrequired from Req_connection_details where server_id ={id}
                      order by server_id ,"field_id"
                       """
            cursor.execute(select_sql)
            result = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            res = []
            for row in result:
                res.append(dict(zip(colnames, row)))
            return res
        except Exception as e:
            return "failed"
            raise
        finally:
            self.close_connection()

    def showConnections(self):  # show active connections
        try:
            self.create_connection()
            select_sql = """
                    select connection_id,ds.id,connection_name,ds.name from datasource_connections dc
                    inner join database_Sources ds
                    on ds.id =dc.sourceid
                       """
            cursor.execute(select_sql)
            data =cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            result = []
            for row in data:
                result.append(dict(zip(colnames, row)))
            return result
        except:
            return "unable to connect database"
            raise
        finally:
            self.close_connection()
        
    
    def active_connections_validation(self ,active_id):
        try:
            self.create_connection()
            select_sql=f"""
                        select uc.field_id, rc.field_name, uc.value,uc.id from user_connection_details uc
                        inner join req_connection_details rc
                        on uc.field_id =rc.field_id and uc.id =rc.server_id
                        where connection_id ={active_id}
                        """
            cursor.execute(select_sql)
            data=cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            result = [ dict(zip(colnames,row))for row in data]
            return result
            
        except:
            pass
        finally:
            self.close_connection()
        

    def __del__(self):
        pass
        # self.connection.close()
