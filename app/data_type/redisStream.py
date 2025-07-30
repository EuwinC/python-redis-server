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

def get_stream(key: str) -> Redis_Stream:
    return streams.setdefault(key, Redis_Stream(key))

def check_if_stream(key: str) -> bool:
    return key in streams

def xadd(key: str, new_id: str, fields: Dict[str, str]) -> str:
    stream = get_stream(key)
    stream_data = stream.data

    # Handle automatic ID generation
    if new_id == "*":
        ms_time = int(datetime.now().timestamp() * 1000)
        seq_no = max(stream_data.get(ms_time, {}).keys(), default=-1) + 1
        if ms_time == 0 and seq_no == 0:
            seq_no = 1 
    else:
        try:
            ms_time, seq_no = new_id.split("-")
            ms_time = int(ms_time)
            if seq_no == "*":
                seq_no = max(stream_data.get(ms_time, {}).keys(), default=-1) + 1
                if ms_time == 0 and seq_no == 0:
                    seq_no = 1 
            seq_no = int(seq_no) 

        except ValueError:
            return "Error code 01"

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

    
    start_split  = start_time.split("-")
    end_split  = end_time.split("-")
    if start_time == "-":
        start_timestamp, start_seq_no = int(stream.timestamp_list[0]),0
    elif len(start_split) == 2:
        start_timestamp, start_seq_no = int(start_split[0]),int(start_split[1])
    else:
        start_timestamp, start_seq_no = int(start_split[0]),0
    if end_time == "+":
        end_timestamp, end_seq_no = int(stream.timestamp_list[-1]),float("inf")
    elif len(end_split) == 2:
        end_timestamp, end_seq_no = int(end_split[0]),int(end_split[1])
    else:
        end_timestamp, end_seq_no = int(end_split[0]),float("inf")
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

if __name__ == "__main__":
    xadd("123", "0-3", {"raspberry": "pear"})
    xadd("123", "0-0", {"abc": "cdef"})
    xadd("123", "123-3", {"abc": "cdefg"})
    xadd("123", "124-1", {"abc": "cdefgh"})
    xadd("123", "123-5", {"abc": "cdefghi"})
    xadd("123", "124-3", {"abc": "cdefghij"})
    result = xrange("123", "-", "124")
    for timestamp_data in result:
        for entry in timestamp_data:
            print(entry)