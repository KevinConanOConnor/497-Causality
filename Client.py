import socket
import selectors
import types
import struct
import json
import threading
import queue

HOST = "127.0.0.1"  # Server hostname or IP address
PORT = 65432        # Server port
sel = selectors.DefaultSelector()
input_queue = queue.Queue()  # Queue to handle user input
PRESET_SERVER = (HOST, PORT)  # Preset data center

# Global states
server_socket = None

# Print utility
def print_with_timestamp(message):
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# JSON message utilities
def send_message_json(sock, message_dict):
    json_message = json.dumps(message_dict)
    json_message_byte_encoded = json_message.encode('utf-8')
    message_length = len(json_message_byte_encoded)
    header = struct.pack('!I', message_length)
    final_message = header + json_message_byte_encoded

    key = sel.get_key(sock)
    key.data.outgoing_buffer += final_message

def unpack_json_message(received_message):
    json_message = received_message.decode('utf-8')
    return json.loads(json_message)

# Register a socket with the selector
def register_socket_selector(sock, selector=sel, connection_type="server"):
    data = types.SimpleNamespace(
        type=connection_type,
        incoming_buffer=b'',
        message_length=None,
        outgoing_buffer=b'',
    )
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(sock, events, data=data)

# Connect to the preset server
def open_server_connection():
    global server_socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(PRESET_SERVER)
        sock.setblocking(False)
        register_socket_selector(sock, connection_type="server")
        server_socket = sock
        print_with_timestamp(f"Connected to server at {PRESET_SERVER}")
    except Exception as e:
        print_with_timestamp(f"Failed to connect to server: {e}")

# Handle message reception and reaction
def handle_message_reaction(sock, message):
    message_type = message.get("type")
    message_content = message.get("content")

    if message_type == "READREPLY":
        print(f"Server Response to Read: {message_content}")
    elif message_type == "WRITEREPLY":
        print(f"Server Response to Write: {message_content}")
    else:
        print(f"Unknown message type received: {message_type}")

# Handle socket events
def handle_connection(key, mask):
    sock = key.fileobj
    data = key.data

    if mask & selectors.EVENT_READ:
        try:
            received = sock.recv(4096)
            if received:
                data.incoming_buffer += received
                # Process the message length header
                if data.message_length is None and len(data.incoming_buffer) >= 4:
                    data.message_length = struct.unpack('!I', data.incoming_buffer[:4])[0]
                    data.incoming_buffer = data.incoming_buffer[4:]

                # Process the full message
                if data.message_length and len(data.incoming_buffer) >= data.message_length:
                    message = data.incoming_buffer[:data.message_length]
                    data.incoming_buffer = data.incoming_buffer[data.message_length:]
                    data.message_length = None

                    # Handle the message
                    message = unpack_json_message(message)
                    handle_message_reaction(sock, message)

            else:
                print("Connection closed by server.")
                sel.unregister(sock)
                sock.close()

        except Exception as e:
            print(f"Error receiving data: {e}")
            sel.unregister(sock)
            sock.close()

    if mask & selectors.EVENT_WRITE:
        if data.outgoing_buffer:
            sent = sock.send(data.outgoing_buffer)
            data.outgoing_buffer = data.outgoing_buffer[sent:]

# Process user commands format: write("key", "value") read("key")
def handle_user_command(command):
    global server_socket

    try:
        if not server_socket:
            print("Not connected to server. Cannot send commands.")
            return

        if command.startswith('read'):
            # Extract key for reading
            key = command.split('"')[1]
            message = {"type": "READ", "content": key}
            send_message_json(server_socket, message)
            print_with_timestamp(f"Sent Read Request for key: {key}")

        elif command.startswith('write'):
            # Extract key and value for writing
            key_value = command.split('"')[1::2]
            if len(key_value) != 2:
                print("Invalid write command format. Use: write(\"key\",\"value\")")
                return

            key, value = key_value
            message = {"type": "WRITE", "content": {"key": key, "value": value}}
            send_message_json(server_socket, message)
            print_with_timestamp(f"Sent Write Request for key: {key}, value: {value}")

        else:
            print("Unknown command. Use: read(\"key\") or write(\"key\",\"value\")")
    except Exception as e:
        print(f"Error processing command: {e}")

# User input handler
def receive_user_input():
    while True:
        user_input = input("Enter command: ")
        input_queue.put(user_input)

# Main event loop
def event_loop():
    threading.Thread(target=receive_user_input, daemon=True).start()

    try:
        while True:
            # Process user input
            while not input_queue.empty():
                user_command = input_queue.get()
                handle_user_command(user_command)

            # Process socket events
            events = sel.select(timeout=1)
            for key, mask in events:
                handle_connection(key, mask)

    except KeyboardInterrupt:
        print("Caught keyboard interrupt. Exiting.")
    finally:
        print("Closing connections.")
        for key in sel.get_map().values():
            sel.unregister(key.fileobj)
            key.fileobj.close()
        sel.close()

# Main entry point
if __name__ == "__main__":
    open_server_connection()
    event_loop()
