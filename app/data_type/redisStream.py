from datetime import datetime

class Redis_Stream:
    def __init__(self,key:str):
        self.consumer_Group: dict[str, dict[str,dict[str, any]]] = {}  
        self.key = key
        self.last_delivered_id = None
        self.pending_ids = None
        self.data = []
        self.last_sequence = "0-0"

    def __len__(self) -> int:
        return len(self.data)
    
    def set_group(self,group_name):
        self.consumer_Group = group_name

    def xadd(self,new_id:str,fields: dict[str,str]) -> str:
        self.data.append((new_id, fields))
        return new_id
    
streams: dict[str, Redis_Stream] = {}
def get_stream(key:str)-> Redis_Stream:
    return streams.setdefault(key, Redis_Stream(key))    

def check_if_stream(key):
    return key in streams

def last_sequence_Number(stream):
    return stream.last_sequence


def xadd(key:str,new_id:str,fields: dict[str,str]):
    stream = get_stream(key)
    if new_id == "*":
        msTime,seq_no = int(datetime.now().timestamp() * 1000),"*"
    else:
        msTime,seq_no = new_id.split("-")
    lmsTime,lseq_no = last_sequence_Number(stream).split("-")
    if seq_no == "*":
        if msTime == lmsTime:
            seq_no = int(lseq_no) + 1
        else:
            seq_no = 0
    if int(msTime) == 0 and int(seq_no) == 0:
        return "Error code 01"
    if (int(lmsTime) == int(msTime) and int(lseq_no) >= int(seq_no)) or int(lmsTime) > int(msTime):
        return "Error code 02"
    stream.last_sequence = f"{msTime}-{seq_no}"
    return stream.xadd(f"{msTime}-{seq_no}",fields)