from datetime import datetime
from typing import Dict, List, Optional
import asyncio

class Redis_Stream:
    def __init__(self, key: str):
        self.key = key
        self.data: Dict[int, Dict[int, Dict[str, str]]] = {}  # {timestamp: {seq_no: fields}}
        self.timestamp_list: List[int] = []  # Ordered list of timestamps
        self._update_event = asyncio.Event()
        
    def clear_update(self) -> None:
        self._update_event.clear()

    def __len__(self) -> int:
        return sum(len(seqs) for seqs in self.data.values())

    def xadd(self, timestamp: int, seq_no: int, fields: Dict[str, str]) -> str:
        if timestamp not in self.data:
            self.data[timestamp] = {}
            self.timestamp_list.append(timestamp)
        self.data[timestamp][seq_no] = fields
        self._update_event.set()
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



async def xread(keys: List[str], data_ids: List[str],block_ms: Optional[int] = None) -> str:
    """
    XREAD streams <key> [<key> ...] <id> [<id> ...]
    returns an array of [ stream, [ [id, [field, value...]] ... ] ] per key.
    """   
    async def check_stream(key,data_id):
        stream = get_stream(key)
        stream_data = stream.data
        print(key,data_id)
        # Parse the last‐seen ID
        ms_str, seq_str = data_id.split("-")
        try:
            ms_time = int(ms_str)
        except ValueError:
            ms_time = 0
        try:
            seq_no_int = int(seq_str)
        except ValueError:
            seq_no_int = 0

        # Collect **only** the next message for this stream
        stream_entries: list[tuple[str, dict[str, str]]] = []
        if ms_time in stream_data:
            seqs = sorted(stream_data[ms_time].keys())
            # next seq in same timestamp
            next_seq = next((s for s in seqs if s > seq_no_int), None)
            if next_seq is not None:
                stream_entries.append((f"{ms_time}-{next_seq}", stream_data[ms_time][next_seq]))
            else:
                # try the first message in the next timestamp
                idx = stream.timestamp_index(ms_time)
                if 0 <= idx + 1 < len(stream.timestamp_list):
                    nxt_ts = stream.timestamp_list[idx + 1]
                    nxt_seqs = sorted(stream_data.get(nxt_ts, {}).keys())
                    if nxt_seqs:
                        s = nxt_seqs[0]
                        stream_entries.append((f"{nxt_ts}-{s}", stream_data[nxt_ts][s]))
        return key,stream_entries
    start_time = asyncio.get_event_loop().time() * 1000
    timeout = block_ms / 1000.0 if block_ms is not None else 0
    streams = [get_stream(key) for key in keys]
    for stream in streams:
        stream.clear_update()
    
    while True:
        resp = f"*{len(keys)}\r\n"
        has_data = False
        for key, data_id in zip(keys, data_ids):    
            key, stream_entries = await check_stream(key, data_id)     
            # Now serialize this stream’s block
            resp += f"*2\r\n"                     # [ key, entries ]
            resp += f"${len(key)}\r\n{key}\r\n"
            resp += f"*{len(stream_entries)}\r\n"
            for msg_id, fields in stream_entries:
                has_data = True
                resp += f"*2\r\n"               # [ id, [ field,value... ] ]
                resp += f"${len(msg_id)}\r\n{msg_id}\r\n"
                resp += f"*{len(fields) * 2}\r\n"
                for fname, fval in fields.items():
                    resp += f"${len(fname)}\r\n{fname}\r\n"
                    resp += f"${len(fval)}\r\n{fval}\r\n"

        if has_data or block_ms is None:
            return resp
        # If timed out, return nil
        if (asyncio.get_event_loop().time() * 1000) - start_time >= block_ms:
            return "$-1\r\n"
        elapsed = (asyncio.get_event_loop().time() * 1000) - start_time
        remaining = max(0, timeout - (elapsed / 1000.0))
        # Wrap each wait() in a Task
        tasks = [asyncio.create_task(s._update_event.wait()) for s in streams]
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
            timeout=remaining
        )
        # Cancel any still‐pending waits
        for t in pending:
            t.cancel()
        # Clear events on streams that actually fired
        for s in streams:
            if s._update_event.is_set():
                s.clear_update()
        # If we timed out (no tasks done), return nil bulk
        if not done:
            return "$-1\r\n"   


if __name__ == "__main__":    

    xadd("apple", "0-1", {"temperature": "0"})
    xadd("blueberry", "0-2", {"temperature": "1"})
    xread(['apple', "blueberry"], ["0-0" ,"0-1"])
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