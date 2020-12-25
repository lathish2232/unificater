import json
import pandas as pd

def extract_sub_json(url, json_str):
    isArray = False
    sub_json = json_str
    level = ""
    json_path = []
    start = 1
    if url.startswith("http"):
        start = 3

    for level in url.split("/")[start:]:
        is_match = False
        if isinstance(sub_json, list):
            for i, sub_json_str in enumerate(sub_json):
                if sub_json_str.get("id") == level:
                    is_match = True
                    sub_json = sub_json_str
                    json_path.append(i)
                    break
            if not is_match:
                return "", json_path, level, {}
        elif isinstance(sub_json, dict):
            sub_json = sub_json.get(level)
            json_path.append(level)
    mongodb_path = ""  # '.'.join(json_path)
    return mongodb_path, json_path, level, sub_json

def data_instance(dataInstances_json):
    Data_frame_stmt=[]
    Data_frame_stmt.append("import pandas as pd")
    fun_name=dataInstances_json["functionName"]
    for dataParameters in dataInstances_json['dataParameters']:
        args={dataParameters["fieldName"]:dataParameters["userValue"] for dataParameters in  dataInstances_json['dataParameters'] if dataParameters["fieldName"] != 'Instance Name' and dataParameters["userValue"] }
        conn_str=fun_name+"(**"+str(args)+")"
        Data_frame_stmt.append(dataInstances_json.get('id') + " = pd."+conn_str)
        break
    stmt="\n".join(Data_frame_stmt)
    exec(stmt)
    return json.loads(eval(dataInstances_json.get('id')).to_json(orient="records"))
