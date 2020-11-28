from .utility import Mongo_DB_Connector
import pandas as pd
from pymongo import MongoClient, DESCENDING
import json

class FlowData(Mongo_DB_Connector):
    def __init__(self):
        self.collection_name ="unificater_flow"
        self.collection_handle =self.db[self.collection_name]
    def read_csv_file(self,**function_args):
        df =pd.read_csv(**function_args)
        return df
    def get_allflows(self):
        record = self.collection_handle.find({},{'_id':0})
        data =[row for row in record]
        return data
    
    def read_file_data(self,data_ins_id):
        ids = data_ins_id.split('.')
        ufid=int(ids[0])
        instances_id =int(ids[1])
        datainstances_id =int(ids[2])
        print(ufid,instances_id,datainstances_id)
        record = self.collection_handle.find({'ufid':ufid},{'_id':0})
        data =[row for r in record for row in r['instances'] ] 
        data_parameters= data[0]['dataInstances'][0][0]['data_parameters'][1:]
        parameter_key=[i['field_name'] for i in data_parameters]
        parameter_value=[i['user_value'] for i in data_parameters]
        dict_v = {k: v for k, v in dict(zip(parameter_key ,parameter_value)).items() if v is not None}
        filepath =dict_v['filepath_or_buffer']
        record=json.loads(pd.read_csv(filepath).to_json(orient="records"))
        return record
    

     

