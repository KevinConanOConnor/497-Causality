import socket
import sys
import selectors
import types
import errno
import struct
import os
import hashlib
import json
import math
import random
import traceback

#User input handled on a separate thread from sockets
import threading
import queue

HOST = "127.0.0.1"  # The server's hostname or IP address, 127.0.0.1 is localhost 
PORT = 65432  # The port used by the server
FILEPATH = ""
CHUNKSIZE = 4096 #Bytes

dir = os.path.dirname(os.path.abspath(__file__))

#print(f'Current Directory:')
#print(dir)
FILEPATH = os.path.join(dir, 'files') #For where to share files from and download shared files to
#print(FILEPATH)
    
sel = selectors.DefaultSelector()
input_queue = queue.Queue() #Queue to handle user requests
###########################################################################################################################################
#Keep a global state of our current file downloads
downloads = {
    "thisisanexamplefilename.txt":{ #To clarify format:
        "chunk_count": 100,
        "received_chunks": set(),
        "missing_chunks": set(range(100)),
    },
}



###########################################################################################################################################
#HASHING CODE

def calc_file_hash(file_path):
    hash_sha256 = hashlib.sha256()

    try:
        with open(file_path, "rb") as file:
            while chunk := file.read(4096):
                hash_sha256.update(chunk)
    except Exception as e:
        print(f"Error hashing file at {file_path}")
        return
    return hash_sha256.hexdigest()


def calc_chunk_hash(chunk):
    hash_sha256 = hashlib.sha256()
    try:
        hash_sha256.update(chunk)
    except Exception as e:
        print(f"Error hashing chunk: {chunk}")
        return
    return hash_sha256.hexdigest()
        

###########################################################################################################################################
# CLIENT SIDE LISTENING SOCKET CODE
lsock =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
lsock.bind((HOST, 0))
    
lsock_address = lsock.getsockname()
print("Listening on ", lsock_address)
lsock.listen()

lsock.setblocking(False)
sel.register(lsock, selectors.EVENT_READ, data=types.SimpleNamespace(type = 'lsock'))


###########################################################################################################################################
# CLIENT SIDE LOCAL FILE MODIFICATION CODE
def adjust_for_storage_directory(fileName, path = FILEPATH):
    """
    Code for adjusting fileName's to include the filepath of our storage directory since I will need to reuse this a lot.
    """
    return os.path.join(path, fileName)


def get_files_in_directory(directory = FILEPATH):
    try:
        with os.scandir(directory) as files:
            for file in files: # Print the name of all files in the directory
                if file.is_file():
                    print(file.name)

    except Exception as e:
        print(f"Error accessing directory: {directory}")
        print(f"{e}")


def initialize_file(file_path, file_size):
    #Check that file path is not already in use.
    if os.path.exists(file_path):
        print(f"There already exists a file at {file_path}. User should check before downloading over.")
        return

    #otherwise open a new file there in write mode
    with open(file_path, 'wb') as file:
        file.seek(file_size - 1)
        file.write(b'\0') #Setting a null byte at the end will give us a file of the correct size
    
    print(f"File of {file_size} bytes has been created at {file_path}")


def peek_chunk(file_path, chunk_index, chunk_size = CHUNKSIZE):
    try:
        with open(file_path, 'rb') as file:
            spot = chunk_index * chunk_size
            file.seek(spot)
            
            chunk_content = file.read(chunk_size)

            #Debugging
            #print(f"Peeked chunk {chunk_index} at {file_path}")

            return chunk_content

    except OSError as e:
        print(f"Error reading chunk {chunk_index} in {file_path}: {e}")
        return None

def set_chunk(file_path, chunk_index, chunk_content, chunk_size = CHUNKSIZE):
    try:
        with open(file_path, 'r+b') as file:
            spot = chunk_index * chunk_size
            file.seek(spot)
            
            chunk_content = file.write(chunk_content)

            #Debugging
            #print(f"Set chunk {chunk_index} at {file_path}")

            return chunk_content

    except OSError as e:
        print(f"Error setting chunk {chunk_index} in {file_path}: {e}")
        return None


def calc_number_of_chunks(file_path, chunk_size = CHUNKSIZE):
    if not os.path.exists(file_path):
        print(f"There does not exist a file at {file_path}.")
        return
    try:
        file_size = os.path.getsize(file_path)
        #print(file_size)
        number_of_chunks = math.ceil(file_size / chunk_size)
        #print(number_of_chunks)
        return (file_size, number_of_chunks)


    except Exception as e:
        print(f"Problem calculating chunk count for {file_path}: {e}")
        return None

#Testing that raeding and writing an entire file works on single system (the current client duh)
def chunksFunctionsTest():
    initialize_file("testfile", 4096)
    set_chunk("testfile", 0 , b"hey dude the empire is pretty cool, maybe you should like, join it or something")
    print(peek_chunk("testfile", 0))


def register_socket_selector(sock, selector = sel, connection_type = "client"):
    """
    Register socket connection to be handled by a selector

    Arguments:
          sock: socket to be registered
          selector: selector object to register socket to (default is sel, a global selector)
          connection_type: Data to be stored for each connection so we can differentiate types of connection (probably just client vs server)
    
    Returns: N/A
    """
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
        
    #Store extra state data for client connections (File + which chunks are available over this connection)
    if connection_type == 'client':
        data = types.SimpleNamespace(
                type = connection_type,

                incoming_buffer = b'',
                messageLength = None, #to record how many bytes we should expect an incoming message to be (to make sure we receive messages in their entirety)

                outgoing_buffer =  b'',

                filename = None,
                peer_chunks = set(),
                ongoing_chunk_request = None, #Keep track of what chunk should be requested on this connection
                file_path = None,
            )
        sel.register(sock, events, data = data)
        
    else:
        data = types.SimpleNamespace(
                type = connection_type,

                incoming_buffer = b'',
                messageLength = None, #to record how many bytes we should expect an incoming message to be (to make sure we receive messages in their entirety)

                outgoing_buffer =  b'',
            )
        sel.register(sock, events, data = data)

    
    


def open_server_connection(ip = HOST, port_number = PORT, timeout = 5):
    """
    Connect to a server using the host and port specified as file constants. This connection will be handled in a blocking manner at first,
    as there is no reason to worry about blocking other messages before we have even established a connection with the central server

    Arguments:
        ip:
        port_number:
        timeout: The number of seconds you would like to wait before timing out the server    
    
    Returns:
        socket: The connected socket if successful, or None if the connection fails.

    Raises:
        socket.timeout: If the connection attempt exceeds the specified timeout.
        ConnectionRefusedError: If the server is not accepting connections (Server.py probably not running on it)
    """
    server_addr = (ip, port_number)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect(server_addr)
        print(f"Connected to Main Server at {server_addr}")

        # Change connection to non blocking after connection is opened
        sock.setblocking(False)

        register_socket_selector(sock = sock, connection_type = "server")
        
        return sock

    except Exception as e:
        print(f"Error Occured trying to establish connection to main server: {e}")


#This function should take in an ip and port number and return a TCP socket connection to that IP/Port
def open_connection(ip, port_number):
    """
    open_connection is for connecting to other client on a specified ip and port_number. This function returns the generated socket connection.
    """
    client_addr = (ip, port_number)
    print(f"Starting Connection to {client_addr}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)

    #Attempt to connect
    result = sock.connect_ex(client_addr)

    if result == 0:
        print(f"Connection to {client_addr} successful immediately")

    elif result == errno.EINPROGRESS or result == errno.EWOULDBLOCK:
        print(f"Connection to {client_addr} in progress")

    else:
        print(f"Error connecting to {client_addr}")
        sock.close()
        return None

    register_socket_selector(sock = sock)

    return sock

def close_connection(sock):
    """
    close_connection takes in an open socket connection, removes from the global Selector sel and closes the socket.
    """
    sel.unregister(sock)
    sock.close()

def send_message_json(sock, message_dict):
    """
    Turn inputted dictionary into a JSON object, give it a length header, and package that message into bytes to be sent. Proceeds to add the message to the socket which it will be sent through's buffer
    """
    json_message = json.dumps(message_dict)
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


#Function to receive input from the user (e.g. ask server what files are available, ask to download files)
def receive_user_input():
    while True:
        print(f"")
        print(f"Files that you can share:")
        get_files_in_directory()

        print(f"")
        print(f"Possible commands:")
        print(f"1 - Request File List from Server FORMAT: 1")
        print(f"2 - Register File with Server. FORMAT: Register filename.txt")
        print(f"3 - Register Directory of files with server. FORMAT: DirectoryRegister folder")
        print(f"4 - Initiate Download of File from Server FORMAT: Download filename.txt (from list given by server)")
        print(f"5 - Check Progress of Downloads. FORMAT: progress")
        print(f"")

        user_input = input("Enter command:")
        if user_input:
            input_queue.put(user_input) #Send input to this queue, when this queue is full it should be handled


def debug_selector_map():
    # Iterate over the registered sockets in the selector
    for key, value in sel.get_map().items():
        print(f"File Object (key): {key}, SelectorKey (value): {value}, Data: {value.data}")

def get_server_socket():
    """
    """
    # Iterate over registered sockets in the selector
    for key, value in sel.get_map().items():
        #print(f"File Object (key): {key}, SelectorKey (value): {value}, Data: {value.data}")
        # Check if the registered data marks this as the server socket
        if isinstance(value.data, types.SimpleNamespace) and value.data.type == "server":
            return key  # Return the LITERAL server socket (key.fileobj) NOT THE SELECTORKEY
    print("Server socket not found.")
    return None

def register_file(filename, serverSocket, file_path = None):
    outgoing_message = {
    }
    print(f"Attempting to register file {filename} with server")
    outgoing_message["type"] = "FILEREG"

    #words[1] will be filename
    outgoing_message["content"] = filename
    
    #need to attach address of listening socket to file so server can tell other clients where to bind
    outgoing_message["listening_address"] = lsock_address
    
    final_file_path = adjust_for_storage_directory(filename)

    #Change filepath if not using base directory
    if file_path is not None:
        final_file_path = os.path.join(FILEPATH, file_path, filename)
        outgoing_message["file_path"] = final_file_path

    #print(final_file_path)
    file_size, number_of_chunks = calc_number_of_chunks(final_file_path)

    outgoing_message["file_size"] = file_size
    outgoing_message["chunk_count"] = number_of_chunks
    #print(outgoing_message)
    send_message_json(serverSocket, outgoing_message)

def get_filenames_in_directory(directory):
    filenames = []
    for _, _, files in os.walk(directory):
        filenames.extend(files)  # Just append the filenames, ignoring paths
    return filenames

#Function to handle/process user input requests
def handle_user_command(command):

    server_sock = get_server_socket()

    outgoing_message = {
    "type": "",
    "content":"",
    }

    #print(f"User requested completion of {command}")
    words = command.strip().split()


    if len(words) == 0:
        print(f"No command entered.")
        return

    action = words[0].lower()

    if action == "1":
        print(f"Requesting File List from Server:")
        outgoing_message["type"] = "FILELISTREQ"
        send_message_json(server_sock, outgoing_message)

    elif action == "progress":
        for file_name, file_data in downloads.items():
            total_chunks = file_data["chunk_count"]
            received_chunks = len(file_data["received_chunks"])
            percentage = (received_chunks / total_chunks) * 100
            print(f"File: {file_name} - Progress: {received_chunks}/{total_chunks} chunks ({percentage:.2f}%)")

    elif len(words) != 2:
        print(f"Command Not In Correct Format")
    
    elif action ==  "register":
        register_file(words[1],server_sock)
        
    elif action == "directoryregister":
        print(f"Attempting to register directory {words[1]} with server")

        directory_path = adjust_for_storage_directory(words[1])
        files = get_filenames_in_directory(directory_path)
        for file_name in files:
            print(directory_path)
            register_file(file_name, server_sock, directory_path)


    elif action == "download":
        (f"Attempting to download file {words[1]} from system")
        outgoing_message["type"] = "FILELOCREQ"
        outgoing_message["content"] = words[1]

        send_message_json(server_sock, outgoing_message)



def send_chunk_request(sock, filename, chunk_index, file_path = None):
    """
    send a chunk request to a peer
    """

    if not file_path:
        message = {
            "type" : "CHUNKREQ",
            "content": {
                "filename": filename,
                "chunks": [chunk_index]
            }
        }
        send_message_json(sock, message)

    else:
        message = {
            "type" : "CHUNKREQ",
            "content": {
                "filename": filename,
                "chunks": [chunk_index],
                "file_path": file_path
            }
        }
        send_message_json(sock, message)
    #print(f"Requested chunk {chunk_index} from peer {sock.getpeername()}.")

def send_chunk(sock, filename, chunk_index, file_path = None):
    path = adjust_for_storage_directory(filename)

    if file_path:
        path = file_path

    chunk_content = peek_chunk(path,chunk_index)
    hash = calc_chunk_hash(chunk_content)

    if chunk_content:
        message = {
            "type": "CHUNKSEND",
            "content": 
            {
                "filename": filename,
                "chunk_index": chunk_index,
                "data": chunk_content.hex(),#Hex to JSON-compatible format
                "hash": hash,
            }
        }
        send_message_json(sock, message)
    else:
        message = {
            "type": "ERROR",
            "content": f"Chunk {chunk_index} of '{filename}' not found on this client."
        }
        send_message_json(sock, message)



#With the decoded message and type passed in, this function should handle the Server's reaction to the message based on the type and content
def handle_message_reaction(sock, message, data):
    """

        Arguments:
            sock: Which connection sent the message (needed to send a return message)
            message_type: Decoded int representing what type of message we are reacting to
            message_content: Decoded content of message we are reacting to
    """
    message_type = message["type"]
    message_content = message["content"]


    outgoing_message = {
        "type": "",
        "content": ""
    }

    #File Registration Reply from Server. No outgoing message neccessary.
    if message_type == "FILEREGREPLY":
        if message_content["success"] == True:
            print(f"{message_content['filename']} was registered successfully with the server")
        else:
            print(f"{message_content['filename']} was not able to be registered with the server. Mayhaps it was already registered?")
        return

    #Chunk Registration Reply. No outgoing message neccessary.
    if message_type == "CHUNKREGACK":
        filename = message["filename"]

        if message["content"] != "SUCCESS":
            print(f"Chunks from {filename} could not be registered as it {filename} was not found on the server")
        return
    
    #File List Reply from Server. No outgoing message neccessary.
    elif message_type == "FILELISTREPLY":
        print(f"File List Received From Server:")
        for each in message_content:
            print(each)
        return
    
    #File Location Reply from Server No outgoing message neccessary.
    elif message_type == "FILELOCREPLY":
        if message_content.get('error'):
            print(message_content['error'])

        filename = message_content['filename']
        filesize = message_content['filesize']
        total_chunks = message_content["filechunkcount"]

        print(f"Owners of {filename} have been received. Initiating connections.")
        users_to_connect_to = message_content['users']
        initialize_file(adjust_for_storage_directory(filename), filesize)

        for user in users_to_connect_to:
            address = user['address'][0]
            port = user['address'][1]
            
            new_connection = open_connection(address, port)
            #Initialize connection's state data
            if new_connection:
                try:
                    peer_data = sel.get_key(new_connection).data
                    peer_data.filename = filename
                    peer_data.peer_chunks = set(user['chunks'])

                    #Assign file_path if neccesary
                    file_path = user.get("file_path")
                    if(file_path):
                        peer_data.file_path = file_path
                    #print(f"Set filename and peer_chunks for connection {new_connection.getpeername()}.")

                except Exception as e:
                    print(f"Error setting peer data for {new_connection}: {e}")

        if filename not in downloads:
            downloads[filename] = {
                "chunk_count": total_chunks,
                "received_chunks": set(),
                "missing_chunks": set(range(total_chunks)),
            }

        #now that connections are open, requesting and handling requests will be done in event loop.
        return
    
    #handle chunk requests from other peers. outgoing message neccesary
    elif message_type == "CHUNKREQ":
        filename = message_content["filename"]
        chunks = message_content["chunks"]
        file_path = message_content.get("file_path")

        for chunk_index in chunks:
            send_chunk(sock, filename, chunk_index, file_path)
        return

    #Received chunk from other client. No outgoing message neccessary.
    elif message_type == "CHUNKSEND":
        filename = message_content["filename"]
        chunk_index = message_content["chunk_index"]
        chunk_data = bytes.fromhex(message_content["data"]) #Convert data from string form back to bytes
        senderside_hash = message_content["hash"]

        #print(f"Received Chunk {chunk_index} of {filename} with expected hash: {senderside_hash} from {sock}")
        receiverside_hash = calc_chunk_hash(chunk_data)

        #Check chunk validity before registering it
        if senderside_hash == receiverside_hash:
            #print(f"Chunk {chunk_index} accepted")

            try:
                set_chunk(adjust_for_storage_directory(filename), chunk_index, chunk_data)
                downloads[filename]["received_chunks"].add(chunk_index)
                downloads[filename]["missing_chunks"].discard(chunk_index)
                data.ongoing_chunk_request = None

            
            except Exception as e:
                print(f"Error saving chunk {chunk_index} from {filename}: {e}")
                data.ongoing_chunk_request = None
            
            #register chunk with server
            try:
                outgoing_message["type"] = "CHUNKREG"
                outgoing_message["content"] = chunk_index
                outgoing_message["filename"] = filename
                outgoing_message["listening_address"] = lsock_address


                serverSocket = get_server_socket()
                send_message_json(serverSocket, outgoing_message)

            except Exception as e:
                print(f"Failed to register chunk {chunk_index} from {filename} with server: {e}")


        #DIscard the chunk and remove it from the list of chunks we are waiting on/return it to missing chunks
        else:
            data.ongoing_chunk_request = None


        return
    
    else:
        print(f"Unknown message Type received: {message_type}: {message_content} ", )


def handle_connection(key, mask):
    sock = key.fileobj
    data = key.data

    #print(data)
    if data.type == 'lsock':
        try:
            #handle accepting connections on the listening socket
            conn, addr = sock.accept();
            print(f"Accepted connection from {addr}")
            conn.setblocking(False)
            register_socket_selector(conn)
        except Exception as e:
            print(f"Problem with accepting connection to peer")
    else:
        if mask & selectors.EVENT_READ: #Ready to read data
            received = sock.recv(8192)

            if received:
                #print(f"Received: {data}")
                data.incoming_buffer += received

                #If we don't know the incoming message length yet. We should try to read it
                if data.messageLength is None and len(data.incoming_buffer) >= 4:
                    #We can extract first 4 bytes as this is the message length prefix
                    data.messageLength = struct.unpack('!I', data.incoming_buffer[:4])[0] #
                    data.incoming_buffer = data.incoming_buffer[4:]
                    #print(f"Expected Message Length {data.messageLength} bytes")

                #If we do know the message length, we should process/clear incoming buffer once it has been fully received
                if data.messageLength is not None and len(data.incoming_buffer) >= data.messageLength:
                    message = data.incoming_buffer[:data.messageLength]

                    message = unpack_json_message(message)
                    #NEED TO PROCESS MESSAGE HERE (For now just print the processed message)
                    #print(message)

                    handle_message_reaction(sock, message, data)

                    data.incoming_buffer = data.incoming_buffer[data.messageLength: ] #Clear the message from buffer
                    data.messageLength = None #Reset message length so that we know there's no message currently


                # For demonstration, we immediately echo back the received data
                #data.outgoing_buffer += received  # Add it to outgoing buffer to echo it back
            else: #If 0 bytes received, client closed connection
                print(f"Closing connection to {sock}")
                sel.unregister(sock)
                sock.close()
                
        if mask & selectors.EVENT_WRITE:
            if data.outgoing_buffer:
                sent = sock.send(data.outgoing_buffer) #Non-blocking send (Hopefully the message should have already been encoded prior to being put into the buffer)
                data.outgoing_buffer = data.outgoing_buffer[sent: ] #Remove sent part from the buffer
            
            elif data.type == "client":
                if data.ongoing_chunk_request is None:
                    if downloads.get(data.filename): #Check if file is still in the list of files we need to download
                        if downloads[data.filename]["missing_chunks"]:#Check if the file still has missing chunks
                            options = downloads[data.filename]["missing_chunks"] & data.peer_chunks
                            if options:
                                next_chunk = random.choice(list(options))
                                data.ongoing_chunk_request = next_chunk
                                send_chunk_request(sock, data.filename, next_chunk, data.file_path)
                                
                                


#Main event loop to handle checking sockets and user input
def event_loop():
    #Run user input on a separate thread so that socket related code is not blocked and vice versa.
    threading.Thread(target = receive_user_input, daemon=True).start()


    try:
        while True:
            try:
                #Check for user input
                while not input_queue.empty():
                    userCommand = input_queue.get()
                    handle_user_command(userCommand)


                events = sel.select(timeout=None)
                for key, mask in events:
                    try:
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
    except Exception as e:
            print(e)
            traceback.print_exc()

    finally:
        print("Closing all connections...")
        for value in list(sel.get_map().values()):  # Iterate through registered sockets
            sel.unregister(value.fileobj)  # Unregister socket
            value.fileobj.close()  # Close the socket
        print("All connections closed.")
        sel.close()


if __name__ == "__main__":
    #Start connection to Server(Tracker)
    serverSock = open_server_connection()

    #chunksFunctionsTest();


    event_loop()



#print(f"Received {data!r}")