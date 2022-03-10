import threading
import socket, sys

# socket.gethostname --> host name
# socket.gethostbyname --> host ip


# class Content_Server:
#     def __init__(self):
#         parse_result = parse_conf(path)
#         this.uuid = parse_result[0]

DEFAULT_PORT = 18346
port = DEFAULT_PORT
BUFSIZE = 1024

# uuid: distance map
uuid_distance_map = dict()


def parse_conf(path):
    conf_file = open(path, "r")
    lines = conf_file.readlines()
    count_line = 0
    for line in lines:
        count_line += 1
        split_result = line.split(" = ")
        if split_result[0] == "uuid":
            uuid = split_result[1].strip()
        elif split_result[0] == "name":
            name = split_result[1].strip()
        elif split_result[0] == "backend_port":
            backend_port = split_result[1].strip()
        elif split_result[0] == "peer_count":
            peer_count = split_result[1].strip()
    if count_line >= 4:
        peer_nodes = lines[4:]
    return (uuid, name, port, peer_count, peer_nodes)

def print_uuid(uuid):
    dict_uuid = {"uuid": uuid}
    print(dict_uuid)

def validate_input():
    if len(sys.argv) < 3:
        print('Format: python3 /path/content_server.py <server_ip_address> <server_port_number>\nExiting...')
        sys.exit(-1)
    else:
        server_address = sys.argv[1]
        server_port = int(sys.argv[2])

def client_handle(srv, msg, client_addr):
    print("Message: ", msg.decode('utf-8'))
    print("Client addr: ", client_addr)
    ADDR = (client_addr, port)
    reply_message = "Server Received: ACK"
    srv.sendto(reply_message.encode(), client_addr)

    # is_connected = True
    # while is_connected:
    #     msg = conn.recv(BUFSIZE).decode('utf-8')
    #     if msg == "DISCONNECT":
    #         is_connected = False
    #     reply_message = msg + "[FROM]" + str(addr)
    #     conn.send(reply_message.encode())
    # conn.close()

def add_neighbors(cmd_line):
    args = cmd_line.split(" ")[1:]
    count_line = 0
    for line in args:
        count_line += 1
        split_result = line.split("=")
        print(split_result)
        if split_result[0] == "uuid":
            uuid = split_result[1].strip()
        elif split_result[0] == "host":
            host_name = split_result[1].strip()
        elif split_result[0] == "backend_port":
            backend_port = int(split_result[1].strip())
        elif split_result[0] == "metric":
            metric = int(split_result[1].strip())
    print(uuid, host_name, backend_port, metric)
    uuid_distance_map[uuid] = metric

def print_active_neighbors():
    pass

def send_keepalive():
    pass

# Used together with send_keepalive to remove inactive nodes
def update_neighbors():
    pass

# Initialize uuid_distance map according to the conf file
def init_uuid_distance_map(peer_count, peer_nodes):
    for i in range(peer_count):
        line = peer_nodes[i]
        line = line.split(" = ")[1]
        line = line.split(",")
        uuid = line[0]
        host_name = line[1].strip()
        port = int(line[2].strip())
        distance = int(line[3].strip())
        uuid_distance_map[uuid] = distance
        

#addneighbor uuid=686f60-1939-4d62-860c-4c703d7a67a6 host=ece006.ece.local.cmu.edu backend_port=18346 metric=30

if __name__ == "__main__":
    if (sys.argv[1] == "-c"):
        conf_path = sys.argv[2]
    print("configure file: " + conf_path)
    parsed_result = parse_conf(conf_path)
    uuid = parsed_result[0]
    name = parsed_result[1]
    backend_port = int(parsed_result[2])
    if len(parsed_result) > 3:
        peer_count = int(parsed_result[3])
        peer_nodes = parsed_result[4]
        init_uuid_distance_map(peer_count, peer_nodes)

    print("parsed successfully", uuid, name, backend_port)
    # Initialize neighbor nodes according to configure file

    # Create socket instance
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # bind socket
    try:
        s.bind((name, backend_port))
    except socket.error as e:
        print('Error when binding.\n\t'+str(e))
        sys.exit(-1)



    # main loop
    while True:
        cmd_line = input("Server: ")
        if cmd_line == "uuid":
            print_uuid(uuid)
        elif cmd_line == "neighbors":
            pass
        elif cmd_line.split(" ")[0] == "addneighbor":
            add_neighbors(cmd_line)




        # HANDLE UDP CLIENT, CREATE NEW THREAD FOR EACH MESSAGE
        # msg_addr = s.recvfrom(BUFSIZE)
        # msg = msg_addr[0]
        # client_addr = msg_addr[1]
        # current_thread = threading.Thread(target = client_handle, args = (s, msg, client_addr))
        # current_thread.start()
        # HANDLE UDP CLIENT