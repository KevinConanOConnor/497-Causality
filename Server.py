import socket
import threading
import selectors
import types
import struct
import json
import time
from datetime import datetime

sel = selectors.DefaultSelector()


PRESET_DATA_CENTERS = [
    {"name": "DATA_CENTER_WEST", "host": "127.0.0.1", "port": 65432, "delay": .05},
    {"name": "DATA_CENTER_CENTRAL", "host": "127.0.0.1", "port": 65472, "delay": .05},
    {"name": "DATA_CENTER_EAST", "host": "127.0.0.1", "port": 65502, "delay": .05},
]


name = "DATA_CENTER_WEST"
print(name)

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)

vectorClock = {
    "DATA_CENTER_WEST": 0,
    "DATA_CENTER_CENTRAL": 0,
    "DATA_CENTER_EAST": 0,
}

#Use Dictionaries to store messages
data = {
    #Example format: I.e. key, value, vector clock at time message was sent and source, though I imagine the last 2 should only matter for debugging
    "x": {"value": "lost", "version": [1, 0, 0], "source": "DATA_CENTER_EXAMPLE"},
}


### CUTOFF POINT - ALL CODE PAST HERES STAYS UNIFORM ON EACH DATA CENTER
#############################################################################################################################################
connections = 0

lsock =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
lsock.bind((HOST, PORT))
    
print("Listening for connections on " +  str(PORT))
lsock.listen()

lsock.setblocking(False)
sel.register(lsock, selectors.EVENT_READ, data=None)

#For convenience
def print_with_timestamp(message):
    """
    Print a message with a timestamp.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


#############################################################################################################################################
# CODE TO HANDLE OPENING UP A CONNECTION WITH ANOTHER SERVER
#############################################################################################################################################
def register_socket_selector(sock, selector = sel, connection_type = "client", broadcasting = False):
    """
    Register socket connection to be handled by a selector
    """
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
        
    #Store extra state data for client connections (File + which chunks are available over this connection)
    data = types.SimpleNamespace(
        type = connection_type,
        broadcast = broadcasting,

        incoming_buffer = b'',
        messageLength = None, #to record how many bytes we should expect an incoming message to be (to make sure we receive messages in their entirety)

        outgoing_buffer =  b'',
    )
    
    sel.register(sock, events, data = data)



def open_server_connection(server_name, ip, port_number, timeout = 5):
    """
    Connect to a server using specified ip and port number. Server name is only here for extra info
    """
    server_addr = (ip, port_number)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect(server_addr)
        print(f"Connected to Server: {server_name} at {server_addr}")

        # Change connection to non blocking after connection is opened
        sock.setblocking(False)
        
        register_socket_selector(sock = sock, connection_type = "center", broadcasting = "true")
        return sock

    except Exception as e:
        print(f"Error Occured trying to establish connection to server {server_name}: {e}")




#####################################################################################################################################
# MESSAGE QUEUEING AND PROCESSING CODE
#####################################################################################################################################

import heapq

message_queue = []  # Priority queue for incoming messages

def commit_message(message):
    """
    Commit a message to the data store and update the vector clock.
    """
    global vectorClock

    key = message["key"]
    value = message["value"]
    source = message["source"]

    incoming_clock = message["vector_clock"]

    # Merge the incoming vector clock with the local one
    for dc in vectorClock:
        vectorClock[dc] = max(vectorClock[dc], incoming_clock.get(dc))

    # Update the data store
    data[key] = {"value": value, "version": incoming_clock, "source": source}

    print(f"Committed message: {message}")
    print(f"Updated vector clock: {vectorClock}")

    global message_queue
    if message_queue:
        process_message_queue();

def process_message_queue():
    """
    Process messages in the input buffer if their vector clock dependencies are satisfied.
    """
    global message_queue

    if message_queue:
        # Get the message with the lowest vector clock sum (priority)
        clock_sum, message = heapq.heappop(message_queue)

        # Check if the message before processing its results
        if clock_sum <= clock_sum + 1:
            commit_message(message)

        else:
            # If not ready, re-add the message to the buffer
            print(f"Message not ready: {message}")
            heapq.heappush(message_queue, (clock_sum, message))
            # Exit loop as higher-priority messages depend on this one


def add_to_message_queue(message):
    """
    Add a message to the input buffer based on the total sum of its vector clock.
    """
    # Calculate the priority as the sum of the vector clock values.
    clock_sum = sum(message["vector_clock"].values())

    # Push the message into the priority queue with the sum as the priority
    heapq.heappush(message_queue, (clock_sum, message))
    print(f"Added message with vector clock sum {clock_sum} to input buffer: {message}")
    process_message_queue()


##############################################################################################################################################
# MESSAGING CODE

def send_message_json(sock, message_json):
    """
    Adds a length header to the inputted JSON message and packages that message into bytes to be sent. Proceeds to add the message to the socket which it will be sent through's buffer
    """

    #Convert Dictionary into JSON format for delivery
    json_message = json.dumps(message_json)
    json_message_byte_encoded = json_message.encode('utf-8')

    message_length = len(json_message_byte_encoded)
    header = struct.pack('!I', message_length)
    finalMessage = header + json_message_byte_encoded;

    key = sel.get_key(sock)
    data = key.data

    key.data.outgoing_buffer += finalMessage;


def unpack_json_message(received_message):
    json_message = received_message.decode('utf-8')

    return json.loads(json_message)

#With the decoded message and type passed in, this function should handle the Server's reaction to the message based on the type and content
def handle_message_reaction(sock, data, message):
    """
    Function Needs To:
    1. Queue message in our server's local buffer.
    """
    #Need to get cid
    cid = data.cid

    message_type = message["type"]
    

    if message_type == "center_data_update":
        add_to_message_queue(message)


    #else:
        #print(f"Unknown message Type received: {message_type}: {message_content} ", )
########################################################################################################################################
# Data specific messaging code

def broadcast_write(key, value):
    print(f"Start Write: {key}, {value}")
    global name
    global vectorClock

    vectorClock[name] += 1

    message = {
        "type": "center_data_update",  # Type of the message
        "key": key,            # Key being updated
        "value": value,       # Value of the key
        "vector_clock": vectorClock,  # Current vector clock
        "source": name         # Name of the sending data center
    }

    #handle update message to a single center
    def send_update_to_center(data_center):
        try:
            time.sleep(data_center["delay"])  # Simulate network latency
            sock = open_server_connection(
                server_name=data_center["name"],
                ip=data_center["host"],
                port_number=data_center["port"],
            )
            if sock:
                send_message_json(sock, message)
                print_with_timestamp(f"Broadcasted message to {data_center['name']}: {message}")
        except Exception as e:
            print_with_timestamp(f"Failed to broadcast message to {data_center['name']}: {e}")


    # Open a connection to each data center. Broadcast. Then ignore.
    # Start a thread for each data center (excluding self)
    threads = []
    for data_center in PRESET_DATA_CENTERS:
        if data_center["name"] == name:
            continue  # Skip self

        thread = threading.Thread(target=send_update_to_center, args=(data_center,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()


def accept_incoming_connection(sock):
    conn, addr = sock.accept() #Socket should already be read to read if this fn is called
    conn.setblocking(False)

    global connections
    connections += 1
    
    newCid = 'conn' + str(connections)
    print(f"Accepted connection from {addr}, gave cid: {newCid}")



    #Buffers will be registered to each socket for incoming and outgoing data to ensure no data is lost from incomplete sends and receives.
    data = types.SimpleNamespace(
            cid = newCid,
            broadcast = 0,
            incoming_buffer = b'',
            messageLength = None, #to record how many bytes we should expect an incoming message to be (to make sure we receive messages in their entirety)

            outgoing_buffer =  b'',
        )
    events = selectors.EVENT_READ | selectors.EVENT_WRITE

    sel.register(conn, events, data = data)


def handle_connection(key, mask):
    sock = key.fileobj
    data = key.data

    #print(data)
    if mask & selectors.EVENT_READ: #Ready to read data
        received = sock.recv(1024)

        if received:
            print(f"Received: {data}")
            data.incoming_buffer += received

            #If we don't know the incoming message length yet. We should try to read it
            if data.messageLength is None and len(data.incoming_buffer) >= 4:
                #We can extract first 4 bytes as this is the message length prefix
                data.messageLength = struct.unpack('!I', data.incoming_buffer[:4])[0] #
                data.incoming_buffer = data.incoming_buffer[4:]
                print(f"Expected Message Length {data.messageLength} bytes")

            #If we do know the message length, we should process/clear incoming buffer once it has been fully received
            if data.messageLength is not None and len(data.incoming_buffer) >= data.messageLength:
                message = data.incoming_buffer[:data.messageLength]

                message = unpack_json_message(message)
                print(message)
                
                #Server's reaction to message
                handle_message_reaction(sock, data, message)

                data.incoming_buffer = data.incoming_buffer[data.messageLength: ] #Clear the message from buffer
                data.messageLength = None #Reset message length so that we know there's no message currently


            # For demonstration, we immediately echo back the received data
            #data.outgoing_buffer += received  # Add it to outgoing buffer to echo it back
        else: #If 0 bytes received, client closed connection
            print(f"Closing connection to {sock}")
            sel.unregister(sock)
            sock.close()
            
    if mask & selectors.EVENT_WRITE and data.outgoing_buffer:
        sent = sock.send(data.outgoing_buffer) #Non-blocking send (Hopefully the message should have already been encoded prior to being put into the buffer)
        data.outgoing_buffer = data.outgoing_buffer[sent: ] #Remove sent part from the buffer

        if not data.outgoing_buffer:
            if data.broadcast:
                sel.unregister(sock)
                sel.close()


#####################################################################################################################################
# Testing

try:
    while True:
        try:
            events = sel.select(timeout = None)
            for key, mask in events:
                try:
                    if key.data is None:
                        accept_incoming_connection(key.fileobj)
                    else:
                        handle_connection(key, mask)
                except (ConnectionResetError, BrokenPipeError) as e:
                    # Handle client disconnection errors
                    print(f"Client forcibly closed connection: {e}")
                    sel.unregister(key.fileobj)  # Unregister the socket
                    key.fileobj.close() 
                except Exception as e:
                    print(f"Exception during connection handling: {e}")
                    sel.unregister(key.fileobj)  # Unregister on other errors
                    key.fileobj.close()  # Close the client socket
        except Exception as e:
            print(f"Rawr, exception on server: {e}")
except KeyboardInterrupt:
    print("Caught keyboard interrupt, exiting")
finally:
    sel.close()




