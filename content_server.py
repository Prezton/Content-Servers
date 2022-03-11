import threading
import socket, sys
import argparse
from Node import Node
from datetime import datetime
# socket.gethostname --> host name
# socket.gethostbyname --> host ip


# class Content_Server:
#     def __init__(self):
#         parse_result = parse_conf(path)
#         this.uuid = parse_result[0]

DEFAULT_PORT = 18346
port = DEFAULT_PORT
BUFSIZE = 1024
global self_uuid, self_name, self_backendport, self_hostname
# uuid: distance map
uuid_distance_map = dict()

# uuid: Node map, store all neighbor nodes
uuid_node_map = dict()

# uuid: linkstate count map, store the latest linkstate sequence number
# Q: how to decide sequence number? many global variables needed?
uuid_linkstate_map = dict()

# Synchronized lock used for maps
lock = threading.Lock()


# @brief .conf file parser used to start a node
# @param path path of the .conf
def parse_conf(path):
    conf_file = open(path, "r")
    lines = conf_file.readlines()
    count_line = 0
    peer_count = None
    peer_nodes = None
    for line in lines:
        count_line += 1
        split_result = line.split(" = ")
        if split_result[0] == "uuid":
            uuid = split_result[1].strip()
        elif split_result[0] == "name":
            name = split_result[1].strip()
        elif split_result[0] == "backend_port":
            backend_port = int(split_result[1].strip())
        elif split_result[0] == "peer_count":
            peer_count = split_result[1].strip()
    if count_line >= 4:
        peer_nodes = lines[4:]
    if peer_count == None:
        return (uuid, name, backend_port)
    return (uuid, name, backend_port, peer_count, peer_nodes)

# @brief used for "uuid" command
# @param current node's uuid
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


# @brief thread to handle all incoming messages from peer nodes
# @param socket of this node
def client_handle(srv):
    while True:
        msg_addr = srv.recvfrom(BUFSIZE)
        message_handle_thread = threading.Thread(target = message_handle, args = (msg_addr, srv))
        message_handle_thread.daemon = True
        message_handle_thread.start()




def message_handle(msg_addr, srv):
    msg = msg_addr[0]
    client_addr = msg_addr[1]
    msg = msg.decode()
    if (msg.split("_")[0] == "Alive"):
        keepalive_handle(msg)

    # Handle keepalive signal
    ADDR = (client_addr, port)
    reply_message = "Server Received: ACK"
    srv.sendto(reply_message.encode(), client_addr)


# @brief used for "addneighbors ******" command
# @param cmd_line command line string which contains parameters
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

    uuid_distance_map[uuid] = metric
    tmp_node = Node(uuid = uuid, host_name = host_name, backend_port = backend_port, metric = metric)

    # Set timestamp for keepalive signal
    tmp_node.set_timestamp(datetime.timestamp(datetime.now()))
    uuid_node_map[uuid] = tmp_node
    # print(uuid_distance_map, uuid_node_map)


# @brief Used for "neighbors" command
# @brief iterate through uuid_node_map (neighbors) to print out related values
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
    print(len(uuid_node_map))

# @brief Handle keepalive signal from received msg, if it is a new neighbor, add it to map
# @param msg message sent from other content_server which may contain Alive_uuid_timestamps_hostname_backendport_metric
def keepalive_handle(msg):
    msg = msg.split("_")
    if msg[0] != "Alive":
        return
    uuid = msg[1]
    node_timestamp = float(msg[2])
    hostname = msg[3]
    backend_port = int(msg[4])
    metric = int(msg[5])

    # Update corresponding node's timestamp if it is in the 
    if uuid in uuid_node_map:
        tmp_node = uuid_node_map[uuid]
        if tmp_node.timestamp <= node_timestamp:
            tmp_node.set_timestamp(node_timestamp)
    else:
        # print("keepalive_handle(): Node not in neighbors map---" + uuid + ", Need to add it")
        new_node = Node(uuid, hostname, backend_port, metric)
        new_node.set_timestamp(node_timestamp)
        uuid_node_map[uuid] = new_node
        uuid_distance_map[uuid] = metric    



# @brief Send keepalive signal to neighbor nodes
# @param srv server socket used to send messages
def send_keepalive(srv):
    keepalive_thread = threading.Timer(5, send_keepalive, args = (s, ))
    keepalive_thread.daemon = True
    keepalive_thread.start()
    # Iterate through the whole neighbors map to find out stale nodes
    cur_timestamp = datetime.timestamp(datetime.now())
    # print("send keepalive: " + str(cur_timestamp))
    deleted = set()
    lock.acquire()
    for uuid in uuid_node_map:
        tmp_node = uuid_node_map[uuid]
        if (cur_timestamp - tmp_node.timestamp > 15):
            # Avoid dict change during iteration
            deleted.add(uuid)
    
    for uuid in deleted:
        del uuid_node_map[uuid]
        del uuid_distance_map[uuid]
        # print("deleted: " + uuid)

    msg = "Alive"
    # Send to each neighbor node
    for uuid in uuid_node_map:
        tmp_node = uuid_node_map[uuid]
        tmp_addr = socket.gethostbyname(tmp_node.host_name)

        dt = datetime.now()
        timestamp = datetime.timestamp(dt)
        # protocol: Alive_uuid_timestamp_hostname_backendport_metric
        msg += "_" + self_uuid + "_" + str(timestamp) + "_" + self_hostname + "_" + str(self_backendport) + "_" + str(uuid_distance_map[uuid])
        ADDR = (tmp_addr, tmp_node.backend_port)
        srv.sendto(msg.encode(), ADDR)
        # tmp_node.set_timestamp(timestamp)
    lock.release()
    


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
        tmp_node = Node(uuid, host_name, backend_port, distance)
        tmp_node.set_timestamp(datetime.timestamp(datetime.now()))
        uuid_node_map[uuid] = tmp_node


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
    self_uuid = parsed_result[0]
    self_name = parsed_result[1]
    self_backendport = int(parsed_result[2])
    if len(parsed_result) > 3:
        peer_count = int(parsed_result[3])
        peer_nodes = parsed_result[4]

        # Initialize neighbor nodes and distances according to configure file
        init_map(peer_count, peer_nodes)

    # Create socket instance
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self_hostname = socket.gethostname()

    localip = socket.gethostbyname(socket.gethostname())
    # localip = "127.0.0.1"
    # bind socket
    try:
        s.bind((localip, self_backendport))
    except socket.error as e:
        sys.exit(-1)

    handle_thread = threading.Thread(target = client_handle, args = (s, ))
    handle_thread.daemon = True
    handle_thread.start()

    send_keepalive(s)

    # main loop
    while True:
        # msg_addr = s.recvfrom(BUFSIZE)
        # command_line_msg = (msg_addr[0]).decode()
        # client_addr = msg_addr[1]
        command_line_msg = input()

        if command_line_msg == "uuid":
            print_uuid(self_uuid)
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


