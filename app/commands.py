from datetime import datetime
store = dict() #[[key,value,time_delete]]
def ping():
    return "+PONG\r\n"

def echo(args):
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


def redis_command(command,args):
    print(command,args)
    if str(command) == "ping":
        return(ping().encode())
    elif str(command) == "echo" and args:
        return(echo(args).encode())
    elif str(command) == "set":
        return(set_func(args).encode())
    elif str(command) == "get":
        return(get_func(args[0]).encode())
    else:
        return("-ERR unknown command or invalid arguments\r\n".encode())  


if __name__ == "__main__":
    print(redis_command("set", ['apple', 'mango', 'px', '100']))
    print(redis_command("get",["apple"]))
            
        
    
