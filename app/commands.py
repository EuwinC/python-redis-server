def ping():
    return "+PONG\r\n"

def echo(args):
    return  f"${len(args[0])}\r\n{args[0]}\r\n"


def redis_command(command,arg):
    if str(command) == "ping":
        return(ping().encode())
    elif str(command) == "echo" and arg:
        return(echo(arg).encode())
    else:
        return("-ERR unknown command or invalid arguments\r\n".encode())  

            
            
        
    
