from datetime import datetime
from app.database import RPUSH
store = dict() #[[key,value,time_delete]]
rpush = dict() #[list_name:RPUSH]

def ping_func():
    return "+PONG\r\n"

def echo_func(args):
    return  f"${len(args[0])}\r\n{args[0]}\r\n"

def set_func(args):
    if len(args) == 4:
        key,value,px,exp_time = args
    else:
        key,value = args
        exp_time = 1000000
    store[key] = (value,datetime.now().timestamp()+float(exp_time)/1000)
    return f"+OK\r\n"

def get_func(key):
    value,exp_time = store.get(key,"")
    if exp_time <= datetime.now().timestamp():
        del store[key]
        value = ""
    return f"${len(value)}\r\n{value}\r\n" if value != "" else "$-1\r\n"

def rpush_func(args):
    if args[0] not in rpush:
        rpush[args[0]] = RPUSH(args[0])
    for i in range(1,len(args)):
        rpush[args[0]].append_list(args[i])
    length = rpush[args[0]].get_element_length()
    return f":{length}\r\n" 

def lrange_func(args): #array_name, left index, right index
    key = args[0]
    if key not in rpush:
        return "*0\r\n"
    start, stop = int(args[1]), int(args[2])
    length = rpush[args[0]].get_element_length()
    if start >= length or start > stop:
        return "*0\r\n"
    if stop >= length:
        stop  = length -1
    count = stop - start + 1
    res = f"*{count}\r\n"
    for idx in range(start, stop + 1):
        item = rpush[key].get_elements(idx)
        res += f"${len(item)}\r\n{item}\r\n"
    return res

def redis_command(command,args):
    print(command,args)
    if str(command) == "ping":
        return(ping_func().encode())
    elif str(command) == "echo":
        return(echo_func(args).encode())
    elif str(command) == "set":
        return(set_func(args).encode())
    elif str(command) == "get":
        return(get_func(args[0]).encode())
    elif str(command) == "rpush":
        return(rpush_func(args).encode())
    elif str(command) == "lrange":
        return(lrange_func(args).encode())
    else:
        return("-ERR unknown command or invalid arguments\r\n".encode())  


if __name__ == "__main__":
    print(redis_command("set", ['apple', 'mango', 'px', '100']))
    print(redis_command("get",["apple"]))
            
        
    
