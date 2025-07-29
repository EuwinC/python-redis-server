def parse_text_command(data):
    """Parse plain text commands separated by \n or \r\n, returning a list of (command, args)."""
    data = data.replace("\r\n", "\n")
    
    # Check if data contains newlines (excluding a trailing newline)
    if '\n' in data.strip():
        # Split by newlines and filter out empty or whitespace-only lines
        lines = [line for line in data.split("\n") if line.strip()]
    else:
        # Split by spaces for a single line
        lines = data.strip().split()
    results = []
    condition,args = "",[]
    functions = {'ping','echo','set','get','rpush','lrange'}
    for line in lines:
        if line.lower() in functions:
            if condition == "":
                condition = line.lower()
            else:
                results.append((condition,args))
                condition,args = line.lower(),[]
        else:
            args.append(line)
    results.append((condition,args))
    return results
        

def convert_resp(data):
    """Parse a RESP array and return the command (first element) and arguments."""
    # If data is bytes, decode it to string
    if isinstance(data, bytes):
        try:
            data = data.decode('utf-8')
        except UnicodeDecodeError:
            return None, []

    if not data.startswith("*"):
        return parse_text_command(data)

    # Split lines and filter out empty ones
    lines = [line for line in data.split("\r\n") if line.strip()]
    if len(lines) < 1:
        return None, []

    try:
        num_elements = int(lines[0][1:])
        if num_elements < 1:
            return None, []

        res = []
        index = 1  # Start after the *N line
        for _ in range(num_elements):
            if index + 1 >= len(lines) or not lines[index].startswith("$"):
                return None, []
            content = lines[index + 1]
            res.append(content)
            index += 2  # Skip $length and content lines

        return res[0].lower() if res else None, res[1:] if len(res) > 1 else []
    
    except (ValueError, IndexError):
        return None, []

def main():
    data1 = "SeT\nfoo\nbar"
    data2 = "redis-cli RPUSH list_key element"
    data3= "SET mango strawberry"
    
    convert = convert_resp(data1)
    convert = convert_resp(data3)
    commands = [item[0] for item in convert]
    args = [item[1] for item in convert] 
    print(commands,args)
    print(convert_resp(data2))

if __name__ == "__main__":
    main()