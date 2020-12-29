class FlowData():
    def __init__(self):
        pass

  
    def updateMasterJsonItems(self, j, keys, value):
        for key in keys[:-1]:
            if isinstance(key, str):
                j = j.setdefault(key, {})
            else:
                j = j[key]
        j[keys[-1]] = value

    def deleteMasterJsonItems(self, j, keys):
        
        for key in keys[:-1]:
            if isinstance(key, str):
                j = j.setdefault(key, {})
            else:
                j = j[key]
        if isinstance(keys[-1], str):
            del j[keys[-1]]
        else:
            j.pop(keys[-1])

    def insertMasterJsonItems(self, j, keys, newkeyidx, value):
        for key in keys[:-1]:
            if isinstance(key, str):
                j = j.setdefault(key, {})
            else:
                j = j[key]
        if isinstance(j[keys[-1]], dict):
            j[keys[-1]][newkeyidx] = value
        elif isinstance(j[keys[-1]], list):
            if isinstance(newkeyidx, str):
                raise Exception("Sorry, " + str(newkeyidx) + " is not a number")
            j[keys[-1]].insert(newkeyidx, value)
        else:
            pass
            # raise Exception("Sorry, " + str(keys[-1]) + " is neither Dictionary
