from .utility import Mongo_DB_Connector
import pandas as pd
from datetime import datetime
import random,copy,asyncio,os
from pymongo import MongoClient, DESCENDING
from tkinter import *
from tkinter import filedialog 

loop = asyncio.get_event_loop()
class DataInstances(object):
    pass
class Instances(DataInstances):
    pass
class unificaterflows(Instances, Mongo_DB_Connector):
    def __init__(self):
        self.document ={'ufid':None, 'flow_name':None,'created_by':None,'created_date':None,'instances':[]}
        self.collection_name ="unificater_flow"
        self.collection_handle =self.db[self.collection_name]
    # sequence_generator Block --------------------
    def ufid_sequence_generator(self):
        record =self.collection_handle.find_one(sort=[('ufid',DESCENDING)])
        if record is None:
            id =1
        else:
            id =record['ufid']+1
        return id
    def instances_sequence_generator(self):
        record =self.collection_handle.find_one(sort=[('ufid',DESCENDING)])
        for row in record['instances']:
            if row['instance_id'] is None:
                id =str(record['ufid'])+'.'+str(1)
            else:
                ins_id=int(str(max([float(row['instance_id']) for row in record['instances']if row['instance_id']!=None])).split('.')[-1])+1
                id=float(str(max([float(row['instance_id']) for row in record['instances']if row['instance_id']!=None])).split('.')[0]+'.'+str(ins_id))
        return id
    
    def datainstance_sequence_generator(self):
        record =self.collection_handle.find_one(sort=[('ufid',DESCENDING)])
        for row in record['instances']:
            ins_id=int(str(max([float(row['instance_id']) for row in record['instances']if row['instance_id']!=None])).split('.')[-1])
            if len (row['dataInstances'])==0:
                id = str(record['ufid'])+'.'+str(ins_id)+'.'+str(1)
            else:
                pass
        return id
    #-------------------------------------------------------------------------------------
    def create_flow(self):
        flow_name ='unificater flow '+str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
        id =self.ufid_sequence_generator()
        self.document['ufid'] =id
        self.document['flow_name'] =flow_name
        self.document['created_date']= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.collection_handle.insert(self.document)
        del self.document['_id']
        return self.document

    def update(self,flow_input):
        myquery =flow_input['ufid']
        newvalues ={"$set":{"flow_name":flow_input["flow_name"]}}
        self.collection_handle.update_one(myquery,newvalues)
        return{"status":1,"data":"flow updated Successfuly"}
    
    async def file_browser(self,type,isfwf):
        try:
            filename=None
            d={'filename':None,'type':None,'data_parameters':None}
            instances_record=[{'instance_id':None,'connection_type':None,"connection_status":None,"error_message":None,'display_name':None,'connection_parameters':None,'dataInstances':[]}]
            conn_parameters=[{'field_id':1,'value':None}]
            root =Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            filename=filedialog.askopenfilenames(parent=root,title="unificater file Browser",filetypes=[("All Files/pattern", "*")])
            filename=filename[0]
            root.destroy()
            #add connection_type to flow 
            record=self.collection_handle.find_one(sort=[('ufid',DESCENDING)])
            instances_record[0]['connection_type']=type
            instances_record[0]['display_name'] =filename.split('/')[-1]
            conn_parameters[0]['value'] =filename
            instances_record[0]['connection_parameters']=conn_parameters
            for row in instances_record:    
                record['instances'].append(row)
            ufid =record['ufid']
            self.collection_handle.update_one({'ufid':ufid},{"$set":record})
            #-----------------------------------------------------
            d["filename"]=filename
            d["type"] =os.path.splitext(filename)[1][1:]
            d["data_parameters"]=self.get_connection_paraeters(type,isfwf=None)
        except Exception as e:
            print("Failure")
        finally:
            return d
    def select_collections(self,type,isfwf):
        if type == "single_file":
            if type and isfwf:
                record=self.get_connection_paraeters(type,isfwf)
            else:
                record=loop.run_until_complete(self.file_browser(type,isfwf))
        elif type =="database":
            record=self.get_database_types(type)         
        else:
            record =[row for row in self.db.connection_types.find({},{'_id':0,'connection_type_id':0})]
        return{"status":1,"data":record}
    def get_database_types(self,type):
        record=self.collection_handle.find_one(sort=[('ufid',DESCENDING)])
        instances_record=[{'instance_id':None,'connection_type':None,"connection_status":None,"error_message":None,'display_name':None,'connection_parameters':None,'dataInstances':[]}]
        instances_record[0]['connection_type']=type
        for row in instances_record:
                record['instances'].append(row)
        ufid =record['ufid']
        self.collection_handle.update_one({'ufid':ufid},{"$set":record})
        record =[row for row in self.db.connections.find({},{'_id':0})]
        return record
    
    def get_db_connection_paraeters(self,id):
        data={'status':1,'data_parameters':None}
        type_id=int(id)
        record =[row for row in self.db.connection_parameters.find({'connection_type_id':type_id},{'_id':0,'connection_type_id':0,'function_name':0,'return_type':0,'connection_type':0})]
        data['data_parameters']=record
        return data
    
    def get_connection_paraeters(self,type,isfwf):
        if type and isfwf:
            record =[row for row in self.db.connection_parameters.find({'connection_type':type,'is_fixed_width': True},{'_id':0,'connection_type_id':0,'function_name':0,'return_type':0,'connection_type':0,'source_connection_name':0,'is_fixed_width':0})]
        else:
            record =[row for row in self.db.connection_parameters.find({'connection_type':type,'is_fixed_width': False},{'_id':0,'connection_type_id':0,'function_name':0,'return_type':0,'connection_type':0,'source_connection_name':0,'is_fixed_width':0})]
        return record
    
    def create_instance(self,request_body):
        type = request_body.get('type',None).lower()
        if type=='csv':
            file_path =request_body['data_parameters'][1]['user_value']
            parameter_key=[i['field_name'] for i in request_body['data_parameters'][2:]]
            parameter_value=[i['user_value'] for i in request_body['data_parameters'][2:]]
            dict_v = {k: v for k, v in dict(zip(parameter_key ,parameter_value)).items() if v is not None}
            data_parameters = "r'"+file_path+"',"+str(dict_v).replace(':'," =").replace('{','').replace('}','').replace("'",'')
            load_dataframe=eval('pd.read_csv'+'('+data_parameters+')')
        # Load_ instance
        record=self.collection_handle.find_one(sort=[('ufid',DESCENDING)])
        
        for row in record['instances']:
            if row['instance_id'] ==None:
                record['instances'][0]['instance_id'] = self.instances_sequence_generator()
        ufid =record['ufid']
        self.collection_handle.update_one({'ufid':ufid},{"$set":record})
    #data_instance--------------------
        record=self.collection_handle.find_one(sort=[('ufid',DESCENDING)])
        data_instance =[{"dataInstance_id":1,"display_name":None ,"connection_status": "valid","error_message":None,'data_parameters':None}]
        data_instance[0]['dataInstance_id'] = self.datainstance_sequence_generator()
        data_instance[0]['display_name'] = file_path.split('/')[-1]
        data_instance[0]['data_parameters'] =request_body['data_parameters']
        for row in record['instances']:
            if len(row['dataInstances'])==0:
                row['dataInstances'].append(data_instance)
                
        self.collection_handle.update_one({'ufid':ufid},{"$set":record})
        del record['_id']
        
        if type=='database':
            parameter_key=[i['field_name'] for i in request_body['data_parameters'][1:]]
            parameter_value=[i['user_value'] for i in request_body['data_parameters'][1:]]
            dict_v = {k: v for k, v in dict(zip(parameter_key ,parameter_value)).items() if v is not None}
        return record
            

    #delete functions 
    def delete_ufid(self,ufid):
        self.collection_handle.remove('ufid:ufid')
    def delete_instance(self,instance_id):
        pass
    def delete_data_instance(self,data_instance_id):
        pass