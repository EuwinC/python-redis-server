
def parse_text_command(data):
    """Parse plain text commands separated by \n or \r\n, returning a list of (command, args)."""
    data = data.replace("\r\n", "\n")
    if '\n' in data.strip():
        lines = [line for line in data.split("\n") if line.strip()]
    else:
        lines = data.strip().split()
    results = []
    condition, args = "", []
    functions = set()
    for line in lines:
        if line.lower() in functions:
            if condition:
                results.append((condition, args))
                condition, args = line.lower(), []
            else:
                condition = line.lower()
        else:
            args.append(line)
    if condition:
        results.append((condition, args))
    return results

def convert_resp(data):
    """Parse a RESP array from bytes and return (command, args, consumed_bytes)."""
    if not isinstance(data, bytes) or not data or data[0:1] != b"*":
        print(f"convert_resp: Invalid input, buffer={data[:100]}")
        return None, [], 0
    
    try:
        # Find end of array length
        end_of_count = data.index(b"\r\n")
        count = int(data[1:end_of_count].decode('ascii'))
        if count < 1:
            print(f"convert_resp: Invalid array count={count}, buffer={data[:100]}")
            return None, [], 0
        
        pos = end_of_count + 2
        args = []
        for _ in range(count):
            if pos >= len(data) or data[pos:pos+1] != b"$":
                print(f"convert_resp: Missing bulk string marker at pos={pos}, buffer={data[:100]}")
                return None, [], 0
            end_of_length = data.index(b"\r\n", pos)
            length = int(data[pos+1:end_of_length].decode('ascii'))
            pos = end_of_length + 2
            if pos + length > len(data):
                print(f"convert_resp: Incomplete bulk string, pos={pos}, length={length}, buffer={data[:100]}")
                return None, [], 0
            arg = data[pos:pos+length].decode('utf-8', errors='replace')
            args.append(arg)
            pos += length + 2  # Skip \r\n
        
        cmd = args[0].lower() if args else None
        print(f"convert_resp: Success, cmd={cmd}, args={args[1:]}, consumed={pos}")
        return cmd, args[1:], pos
    except (ValueError, IndexError, UnicodeDecodeError) as e:
        print(f"convert_resp error: {str(e)}, buffer={data[:100]}")
        return None, [], 0

def build_resp_array(cmd: str, args: list):
    """Build a RESP array from command and arguments."""
    parts = [f"${len(cmd)}\r\n{cmd}\r\n"]
    for arg in args:
        parts.append(f"${len(arg)}\r\n{arg}\r\n")
    return f"*{len(args) + 1}\r\n{''.join(parts)}"
def main():
    data1 = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n"
    data2 = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    print(convert_resp(data1))  # Should print: ('set', ['foo', '123'], 26)
    print(convert_resp(data2))  # Should print: ('get', ['foo'], 18)
    print(convert_resp(b"invalid"))  # Should print: (None, [], 0)
    print(convert_resp(b"\r\n$3\r\nfoo\r\n$3\r\n123\r\n"))  # Should print: (None, [], 0)

if __name__ == "__main__":
    main()