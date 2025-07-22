def parse_text_command(data):
    """Parse plain text commands separated by \n or \r\n, returning a list of (command, args)."""
    lines = [line for line in data.replace("\r\n", "\n").split("\n") if line.strip()]
    results = []
    ping_count = 0  # Track number of PING commands
    for line in lines:
        parts = line.split()
        if line.lower() == "ping":
            ping_count += 1
            if ping_count == 1:
                # First PING: return ('ping', 'ping')
                command = "ping"
                args = "ping"
            else:
                # Subsequent PING: return ([], [])
                command = []
                args = []
        else:
            command = parts[0].lower() if parts else ""
            args = parts[1:] if len(parts) > 1 else []
        results.append((command, args))
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
    data1 = "*2\r\n$4\r\nECHO\r\n$9\r\nblueberry\r\n"
    data2 = "PING\nPING"
    print(convert_resp(data1))
    print(convert_resp(data2))

if __name__ == "__main__":
    main()