from datetime import datetime
from typing import Dict, List, Optional

class Redis_Stream:
    def __init__(self, key: str):
        self.key = key
        self.data: Dict[int, Dict[int, Dict[str, str]]] = {}  # {timestamp: {seq_no: fields}}
        self.timestamp_list: List[int] = []  # Ordered list of timestamps

    def __len__(self) -> int:
        return sum(len(seqs) for seqs in self.data.values())

    def xadd(self, timestamp: int, seq_no: int, fields: Dict[str, str]) -> str:
        if timestamp not in self.data:
            self.data[timestamp] = {}
            self.timestamp_list.append(timestamp)
        self.data[timestamp][seq_no] = fields
        return f"{timestamp}-{seq_no}"

    def timestamp_index(self, timestamp: int) -> int:
        try:
            return self.timestamp_list.index(timestamp)
        except ValueError:
            return -1

streams: Dict[str, Redis_Stream] = {}

def timestamp_generation(stream_data,time):
    if time == "*":
        ms_time = int(datetime.now().timestamp() * 1000)
        seq_no = max(stream_data.get(ms_time, {}).keys(), default=-1) + 1
        if ms_time == 0 and seq_no == 0:
            seq_no = 1 
    else:
        ms_time, seq_no = time.split("-", 1) + ["empty"] * (2 - len(time.split("-", 1)))
        ms_time = int(ms_time)
        if seq_no == "empty":
            seq_no = -1
        elif seq_no == "*":
            seq_no = max(stream_data.get(ms_time, {}).keys(), default=-1) + 1
            if ms_time == 0 and seq_no == 0:
                seq_no = 1 

        seq_no = int(seq_no) 

    return ms_time,seq_no

def get_stream(key: str) -> Redis_Stream:
    return streams.setdefault(key, Redis_Stream(key))

def check_if_stream(key: str) -> bool:
    return key in streams

def xadd(key: str, data_id: str, fields: Dict[str, str]) -> str:
    stream = get_stream(key)
    stream_data = stream.data

    ms_time,seq_no = timestamp_generation(stream_data,data_id)

    # Check for invalid timestamp or sequence number
    if ms_time < 0 or seq_no < 0:
        return "Error code 01"
    
    if ms_time == 0 and seq_no == 0:
        return "Error code 01"

    # Check if the ID is in the past
    for existing_ts in stream_data:
        if existing_ts > ms_time:
            return "Error code 02"
        if existing_ts == ms_time and max(stream_data[existing_ts].keys(), default=0) >= seq_no:
            return "Error code 02"

    return stream.xadd(ms_time, seq_no, fields)

def xrange(key: str, start_time: str, end_time: Optional[str] = None) -> List[List[str]]:
    stream = get_stream(key)
    stream_data = stream.data
    if not end_time:
        end_time = start_time

    if start_time == "-":
        start_timestamp, start_seq_no = int(stream.timestamp_list[0]),0
    else:
        start_timestamp, start_seq_no = timestamp_generation(stream_data,start_time)
        start_seq_no = 0 if start_seq_no == -1 else start_seq_no
    if end_time == "+":
        end_timestamp, end_seq_no = int(stream.timestamp_list[-1]),float("inf")
    else:
        end_timestamp, end_seq_no = timestamp_generation(stream_data,end_time)
        end_seq_no = float("inf") if end_seq_no == -1 else end_seq_no
        
    start_idx = stream.timestamp_index(start_timestamp)
    end_idx = stream.timestamp_index(end_timestamp)

    if start_idx == -1 or end_idx == -1:
        return []  # Timestamps not found

# Collect all entries in the range
    entries = []
    for i in range(start_idx, end_idx + 1):
        timestamp = stream.timestamp_list[i]
        start_seq = start_seq_no if i == start_idx else 0
        end_seq = end_seq_no if i == end_idx else float("inf")

        for seq_no in sorted(stream_data.get(timestamp, {}).keys()):
            if start_seq <= seq_no <= end_seq:
                entries.append((timestamp, seq_no))

    # Build RESP array
    res = f"*{len(entries)}\r\n"
    for timestamp, seq_no in entries:
        message_id = f"{timestamp}-{seq_no}"
        fields = stream_data[timestamp][seq_no]
        res += f"*2\r\n${len(message_id)}\r\n{message_id}\r\n*{len(fields)*2}\r\n"
        for key, value in fields.items():
            res += f"${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"

    return res

def xread(key, data_id):
    stream = get_stream(key)
    stream_data = stream.data

    # split as strings...
    ms_str, seq_str = data_id.split("-", 1)

    # ...then convert to ints for keys/lookups
    try:
        ms_time = int(ms_str)
    except ValueError:
        ms_time = 0
    try:
        seq_no_int = int(seq_str)
    except ValueError:
        seq_no_int = 0

    entries = []
    # Check if ms_time exists in stream_data and has entries
    if ms_time in stream_data:
        valid_seqs = sorted(stream_data[ms_time].keys())  # Get sorted sequence numbers
        # Find the next sequence number greater than seq_no_int
        next_seq_no = next((x for x in valid_seqs if x > seq_no_int), None)
        if next_seq_no is not None:
            # Found a valid sequence number in the current timestamp
            message_id = f"{ms_time}-{next_seq_no}"
            fields = stream_data[ms_time][next_seq_no]
            entries.append((message_id, fields))
        else:
            # No valid sequence number in current ms_time, try next timestamp
            try:
                timestamp_list = stream.timestamp_list
                current_index = stream.timestamp_index(ms_time)
                if current_index + 1 < len(timestamp_list):
                    next_ms_time = timestamp_list[current_index + 1]
                    valid_seqs = sorted(stream_data[next_ms_time].keys())
                    next_seq_no = valid_seqs[0]  # Take the first sequence number
                    message_id = f"{next_ms_time}-{next_seq_no}"
                    fields = stream_data[next_ms_time][next_seq_no]
                    entries.append((message_id, fields))
            except (IndexError, KeyError):
                pass  # No next timestamp or entries, return empty entries

    # Format response in Redis protocol
    if not entries:
        return f"*1\r\n*2\r\n${len(key)}\r\n{key}\r\n*0\r\n"
    
    resp = f"*1\r\n"  # One stream
    resp += f"*2\r\n"  # Stream key and entries
    resp += f"${len(key)}\r\n{key}\r\n"  # Stream key
    resp += f"*{len(entries)}\r\n"  # Number of entries
    for message_id, fields in entries:
        resp += f"*2\r\n"  # Message ID and fields
        resp += f"${len(message_id)}\r\n{message_id}\r\n"
        resp += f"*{len(fields)*2}\r\n"  # Number of field-value pairs
        for fname, fval in fields.items():
            resp += f"${len(fname)}\r\n{fname}\r\n"
            resp += f"${len(fval)}\r\n{fval}\r\n"
    
    return resp
if __name__ == "__main__":
    xadd("raspberry", "0-3", {"temperature": "63"})
    xread('raspberry', '0-0')
    xadd("123", "0-0", {"abc": "cdef"})
    xadd("123", "123-3", {"abc": "cdefg"})
    xadd("123", "124-1", {"abc": "cdefgh"})
    xadd("123", "123-5", {"abc": "cdefghi"})
    xadd("123", "124-3", {"abc": "cdefghij"})
    xread("123","124-2")
    result = xrange("123", "-", "124")
    for timestamp_data in result:
        for entry in timestamp_data:
            print(entry)