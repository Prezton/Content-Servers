import threading
import socket, sys
import argparse
from Node import Node
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

# uuid: Node map, store all neighbor nodes
uuid_node_map = dict()

# uuid: heartbeat count map, store the latest heartbeat sequence number
# Q: how to decide sequence number? many global variables needed?
uuid_heartbeatcount_map = dict()



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

def client_handle(srv):
    while True:
        msg_addr = s.recvfrom(BUFSIZE)

        msg = msg_addr[0]
        client_addr = msg_addr[1]

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
    # print(cmd_line)
    args = cmd_line.split(" ")[1:]
    count_line = 0
    for line in args:
        count_line += 1
        split_result = line.split("=")
        if split_result[0] == "uuid":
            uuid = split_result[1].strip()
        elif split_result[0] == "host":
            host_name = split_result[1].strip()
        elif split_result[0] == "backend_port":
            backend_port = int(split_result[1].strip())
        elif split_result[0] == "metric":
            metric = int(split_result[1].strip())
    # print(uuid, host_name, backend_port, metric)
    uuid_distance_map[uuid] = metric
    uuid_node_map[uuid] = Node(uuid = uuid, host_name = host_name, backend_port = backend_port, metric = metric)
    # print(uuid_distance_map, uuid_node_map)

def print_active_neighbors():
    result = dict()
    tmp_outer_dict = dict()
    for uuid in uuid_node_map:
        tmp_dict = dict()
        tmp_node = uuid_node_map[uuid]
        tmp_name = tmp_node.name
        tmp_hostname = tmp_node.host_name
        tmp_backendport = tmp_node.backend_port
        tmp_metric = uuid_distance_map[uuid]
        tmp_dict["uuid"] = uuid
        tmp_dict["host"] = tmp_hostname
        tmp_dict["backend_port"] = tmp_backendport
        tmp_dict["metric"] = tmp_metric
        tmp_outer_dict[tmp_name] = tmp_dict
    result["neighbors"] = tmp_outer_dict
    print(result)

def keepalive_handle(srv):
    pass

# Send keepalive signal to neighbor nodes
def send_keepalive(srv):
    msg = "Alive?"
    for uuid in uuid_node_map:
        tmp_node = uuid_node_map[uuid]
        tmp_addr = tmp_node.host_name
        tmp_addr.sendto(msg.encode(), tmp_addr)
    


# Used together with send_keepalive to remove inactive nodes
def update_neighbors():
    pass

# Initialize uuid_distance map uuid_node map according to the .conf's neighbor nodes
def init_map(peer_count, peer_nodes):
    for i in range(peer_count):
        line = peer_nodes[i]
        line = line.split(" = ")[1]
        line = line.split(",")
        uuid = line[0]
        host_name = line[1].strip()
        backend_port = int(line[2].strip())
        distance = int(line[3].strip())
        uuid_distance_map[uuid] = distance
        uuid_node_map[uuid] = Node(uuid, host_name, backend_port, distance)


def advertisement():
    pass

def send_linkstate():
    pass

def forward_linkstate(msg):
    pass
        
def kill_current_node():
    sys.exit(0)

#addneighbor uuid=686f60-1939-4d62-860c-4c703d7a67a6 host=ece006.ece.local.cmu.edu backend_port=18346 metric=30

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", required=True, type=str)
    args = parser.parse_args()
    
    # if (sys.argv[1] == "-c"):
    #     conf_path = sys.argv[2]
    parsed_result = parse_conf(args.c)
    uuid = parsed_result[0]
    name = parsed_result[1]
    backend_port = int(parsed_result[2])
    if len(parsed_result) > 3:
        peer_count = int(parsed_result[3])
        peer_nodes = parsed_result[4]

        # Initialize neighbor nodes and distances according to configure file
        init_map(peer_count, peer_nodes)

    # Create socket instance
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # localip = socket.gethostbyname(socket.gethostname())
    localip = "127.0.0.1"
    # bind socket
    try:
        s.bind((localip, backend_port))
    except socket.error as e:
        sys.exit(-1)

    current_thread = threading.Thread(target = client_handle, args = (s, ))
    current_thread.start()

    # main loop
    while True:
        # msg_addr = s.recvfrom(BUFSIZE)
        # command_line_msg = (msg_addr[0]).decode()
        # client_addr = msg_addr[1]
        command_line_msg = input()

        if command_line_msg == "uuid":
            print_uuid(uuid)
        elif command_line_msg == "neighbors":
            print_active_neighbors()
        elif command_line_msg.split(" ")[0] == "addneighbor":
            add_neighbors(command_line_msg)
        elif command_line_msg == "kill":
            kill_current_node()
        elif command_line_msg == "map":
            pass
        elif command_line_msg == "rank":
            pass




        # HANDLE UDP CLIENT, CREATE NEW THREAD FOR EACH MESSAGE
        # msg_addr = s.recvfrom(BUFSIZE)
        # msg = msg_addr[0]
        # client_addr = msg_addr[1]
        # current_thread = threading.Thread(target = client_handle, args = (s, msg, client_addr))
        # current_thread.start()
        # HANDLE UDP CLIENT