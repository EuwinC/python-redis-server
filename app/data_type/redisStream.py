class Redis_Stream:
    def __init__(self,key:str):
        self.consumer_Group: dict[str, dict[str,dict[str, any]]] = {}  
        self.key = key
        self.last_delivered_id = None
        self.pending_ids = None
        self.data = []

    def __len__(self) -> int:
        return len(self.data)
    
    def set_group(self,group_name):
        self.consumer_Group = group_name

    def xadd(self,new_id:str,fields: dict[str,str],entry_id: str) -> str:
        self.data.append((new_id, fields))
        return new_id
streams: dict[str, Redis_Stream] = {}
def get_stream(key:str)-> Redis_Stream:
    return streams.setdefault(key, Redis_Stream(key))    

def check_if_stream(key):
    return key in streams

def xadd(key:str,new_id:str,fields: dict[str,str],entry_id: str = "*"):
    stream = get_stream(key)
    return stream.xadd(new_id,fields,entry_id)