COMMAND_REGISTRY = {}
WRITE_COMMANDS = set()

def redis_cmd(is_write = False):
    def decorator(func):
        cmd_name = func.__name__.replace("_func", "").lower()
        func.is_write = is_write
        
        COMMAND_REGISTRY[cmd_name] = func
        if is_write:
            WRITE_COMMANDS.add(cmd_name)
        return func
    return decorator
        