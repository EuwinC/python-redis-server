import pickle
import os
from app.convert_commands import convert_resp
from app.commands import redis_command

AOF_FILE = "appendonly.aof"
RDB_FILE = "dump.rdb"

#RDB
def save_rdb(data):
    """Save the in-memory dictionary to a binary file"""
    try:
        # Create a snapshot copy so the dictionary doesn't change during save
        data_snapshot = data.copy() 
        with open(RDB_FILE, "wb") as f:
            pickle.dump(data_snapshot, f)
        print(f"RDB snapshot saved: {RDB_FILE}")
    except Exception as e:
        print(f"RDB Save Error: {e}")

def load_rdb():
    if os.path.exists(RDB_FILE):
        with open(RDB_FILE, "rb") as f:
            return pickle.load(f)
    return {}
        
#AOF
def log_to_aof(resp_cmd):
    with open(AOF_FILE, "a") as f:
        f.write(resp_cmd)

async def load_from_aof(client_state):
    """
    Replays the AOF file on startup to restore server state.
    """
    if not os.path.exists(AOF_FILE):
        print("No Aof File found. Starting with empty state")
        return 
    print(f"Loading AOF: {AOF_FILE}...")
    with open(AOF_FILE,"rb") as f:
        buffer = f.read()
    
    pos = 0
    while pos < len(buffer):
        # 1. Parse the next common from the buffer
        cmd, args, consumed = convert_resp(buffer[pos:])
        
        if cmd:
            # 2. Execute the common internally
            # set is_replic= True to avoid re-logging to AOF or propagating to replicas during recovery
            await redis_command(cmd,args,client_state,is_replica = True)
            pos += consumed
        else:
            break
    print("AOF replay complete.")



