# Custom Redis Server (Python)

A high-performance, **asynchronous Redis-compatible server** built from scratch using `asyncio`.

This project is an educational / experimental implementation that demonstrates core systems programming concepts:

- Low-level network protocols (TCP + RESP)
- Data persistence strategies
- Replication mechanics
- Transaction handling
- Single-threaded high-concurrency design

<grok-card data-id="1691ea" data-type="image_card"  data-arg-size="LARGE" ></grok-card>



<grok-card data-id="a609b1" data-type="image_card"  data-arg-size="LARGE" ></grok-card>


## 🚀 Key Features

- **3-Layer Architecture** — clean separation of concerns  
  Networking (TCP/RESP) ↔ Middleware (guards/routing) ↔ Execution (command logic)
- **Hybrid Persistence** — 100% durability via **RDB snapshots** + **AOF logging**
- **Sub-millisecond latency** — single-threaded `asyncio` event loop
- **Master-Slave Replication** — custom handshake + async command forwarding
- **Transactions** — full support for `MULTI` / `EXEC` / `DISCARD`
- Supported data types: Strings, Lists, Streams (with consumer groups planned)

<grok-card data-id="211b23" data-type="image_card"  data-arg-size="LARGE" ></grok-card>



<grok-card data-id="962ccd" data-type="image_card"  data-arg-size="LARGE" ></grok-card>


## 🏗️ Architecture

| Layer              | File          | Responsibility                                                                 |
|--------------------|---------------|--------------------------------------------------------------------------------|
| **Networking**     | `main.py`     | TCP server, socket handling, RESP parsing & serialization                      |
| **Middleware**     | `router.py`   | Permission checks, read-only slave mode, transaction queuing, persistence & replication triggers |
| **Execution**      | `commands.py` | Pure command logic + metadata-driven registry, operates on in-memory structures |

<grok-card data-id="d65f49" data-type="image_card"  data-arg-size="LARGE" ></grok-card>


## 💾 Persistence Engine

Two complementary mechanisms for maximum durability:

- **AOF (Append-Only File)**  
  Every mutating command is appended in raw RESP format → replay on startup

- **RDB (Snapshot)**  
  Periodic binary dumps of the entire keyspace (using Python serialization) → fast recovery for large datasets

## 🛠️ Technical Stack

- **Language**: Python 3.10+
- **Concurrency**: `asyncio` single-threaded event loop
- **Data structures**:
  - Strings → dict
  - Lists → collections.deque
  - Streams → nested dicts
  - TTL → min-heap (heapq)
- **Protocol**: RESP (Redis Serialization Protocol)

## 🚦 Getting Started

### Prerequisites

- Python 3.10 or higher

### Installation

```bash
git clone https://github.com/yourusername/redis-python.git
cd redis-python
# Optional: create virtual environment
python3 -m venv venv
source venv/bin/activate    # Linux/macOS
venv\Scripts\activate       # Windows

```
## Running the Server

### Start as Master (default)
```bash
python3 main.py --port 6379
```
### Start as Slave / Replica
```bash
python3 main.py --port 6380 --replicaof "127.0.0.1 6379"
```
### You can connect using any Redis client:
```bash
redis-cli -p 6379
# or
redis-cli -p 6380
```

## 📋 Planned / Future Improvements

- Pub/Sub
- More data types (Sets, Sorted Sets, Hashes)
- Configuration file support
- Better monitoring / INFO command
- RDB compression
- Authentication (ACL / password)

## ⚠️ Disclaimer
This is not production-ready Redis.
Use only for learning, experimentation, or specific lightweight use cases.
Inspired by Redis — built with ❤️ in Python.
Happy hacking! 🛠️
