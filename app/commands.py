from datetime import datetime
from app.database import PUSH
store = dict() #[[key,value,time_delete]]
push = dict() #[list_name:PUSH]
def ping_func(args):
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

def get_func(args):
    key = args[0]
    value,exp_time = store.get(key,"")
    if exp_time <= datetime.now().timestamp():
        del store[key]
        value = ""
    return f"${len(value)}\r\n{value}\r\n" if value != "" else "$-1\r\n"

def rpush_func(args):
    key = args[0]
    if key not in push:
        push[key] = PUSH(key)
    for i in range(1,len(args)):
        push[key].append_right(args[i])
    length = push[key].get_element_length()
    return f":{length}\r\n" 

def lpush_func(args):
    key = args[0]
    if key not in push:
        push[key] = PUSH(key)
    for i in range(1,len(args)):
        push[key].append_left(args[i])
    length = push[key].get_element_length()
    return f":{length}\r\n" 

def llen_func(args):
    key = args[0]
    if key not in push:
        return f":0\r\n"
    length = push[key].get_element_length()
    return f":{length}\r\n" 

def lrange_func(args): #array_name, left index, right index
    key = args[0]
    start, stop = int(args[1]), int(args[2])
    if key not in push:
        return f"*0\r\n"
    length = push[key].get_element_length()
    if start < 0:
        start = length + start
    if stop < 0:
        stop = length + stop    
    if start < 0:
        start = 0
    if start >= length or start > stop:
        return f"*0\r\n"    
    if stop >= length:
        stop  = length - 1
    count = stop - start + 1
    res = f"*{count}\r\n"
    for idx in range(start, stop + 1):
        item = push[key].get_elements(idx)
        res += f"${len(item)}\r\n{item}\r\n"
    return res

COMMANDS = {
    "ping":   ping_func,
    "echo":   echo_func,
    "set":    set_func,
    "get":    get_func,
    "rpush":  rpush_func,
    "lpush":  lpush_func,
    "lrange": lrange_func,
    "llen": llen_func,
}

def redis_command(cmd, args):
    print(cmd,args)
    fn = COMMANDS.get(cmd.lower())
    if not fn:
        return b"-ERR unknown command or invalid arguments\r\n"
    return fn(args).encode()


if __name__ == "__main__":
    print(redis_command("set", ['apple', 'mango', 'px', '100']))
    print(redis_command("get",["apple"]))
            
        
    
