Custom Redis Server (Python)
A high-performance, asynchronous Redis-compatible server built from scratch using asyncio. This project demonstrates core systems programming concepts, including network protocols, data persistence, and distributed state management.

🚀 Key Features
3-Layer Architecture: Decoupled Networking (TCP/RESP), Middleware (Guards/Routing), and Execution layers for high maintainability.

Hybrid Persistence: Achieves 100% data durability by combining RDB snapshots (binary state recovery) and AOF logging (command replay).

Sub-Millisecond Performance: Leverages a single-threaded event loop (asyncio) to handle concurrent client connections with minimal latency.

Master-Slave Replication: Supports distributed state via a custom handshake protocol and asynchronous command propagation.

Transaction Support: Implements atomic execution via MULTI, EXEC, and DISCARD with a dedicated transaction queue.

🏗️ Architecture
The server is structured into three distinct layers to ensure a clean separation of concerns:

Networking Layer (main.py): Handles raw TCP sockets and parses the RESP (Redis Serialization Protocol).

Middleware/Guard Layer (router.py): Centralized engine that manages permissions (Slave-Read-Only), transaction queuing, and triggers persistence/replication.

Execution Layer (commands.py): Pure logic functions that manipulate in-memory data structures (Strings, Lists, Streams) through a metadata-driven registry.

💾 Persistence Engine
To guarantee durability, the server implements two complementary strategies:

Append-Only File (AOF): Every write-intent command is logged to disk in raw RESP format. On startup, the server replays the AOF to restore the most recent state.

Redis Database (RDB): Periodic binary snapshots of the entire keyspace using Python serialization, allowing for rapid recovery of large datasets.

🛠️ Technical Stack
Language: Python 3.10+

Concurrency: asyncio (Event Loop)

Structures: Min-heaps (TTL), Deques (Lists), Nested Dicts (Streams)

Protocol: RESP (Redis Serialization Protocol)

🚦 Getting Started
Prerequisites
Python 3.10 or higher

Installation
Bash

git clone https://github.com/yourusername/redis-python.git
cd redis-python
Running the Server
Start as Master:

Bash

python3 main.py --port 6379
Start as Slave:

Bash

python3 main.py --port 6380 --replicaof "127.0.0.1 6379"
