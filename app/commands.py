store = {}
def ping():
    return "+PONG\r\n"

def echo(args):
    return  f"${len(args[0])}\r\n{args[0]}\r\n"

def set_func(key,value):
    store[key] = value
    return f"+OK\r\n"
def get_func(key):
    result = store.get(key,"")
    return f"${len(result)}\r\n{result}\r\n" if result != "" else "$-1\r\n"


def redis_command(command,args):
    print(command,args)
    if str(command) == "ping":
        return(ping().encode())
    elif str(command) == "echo" and args:
        return(echo(args).encode())
    elif str(command) == "set":
        return(set_func(args[0],args[1]).encode())
    elif str(command) == "get":
        return(get_func(args[0]).encode())
    else:
        return("-ERR unknown command or invalid arguments\r\n".encode())  

            
            
        
    
