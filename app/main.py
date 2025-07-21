import socket  # noqa: F401
import threading

def handle_ping(client_socket):
    try:
        while True:
            request: bytes = client_socket.recv(512)
            data: str = request.decode()

            # print(data)
            if "ping" in data.lower():
                client_socket.send("+PONG\r\n".encode())
    finally:
        client_socket.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    try:
        while True:
            client_socket, addr = server_socket.accept()  # Wait for a client
            print(f"New connection from {addr}")
            # Start a new thread for each client
            client_thread = threading.Thread(target=handle_ping, args=(client_socket,))
            client_thread.start()
    finally:
        server_socket.close()


    
if __name__ == "__main__":
    main()
