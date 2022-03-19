import threading
import socket, sys
import argparse
from Node import Node
from Linkstate import Linkstate
from datetime import datetime
import json
import copy
import heapq

# socket.gethostname --> host name
# socket.gethostbyname --> host ip


# class Content_Server:
#     def __init__(self):
#         parse_result = parse_conf(path)
#         this.uuid = parse_result[0]

BUFSIZE = 1024
global self_uuid, self_name, self_backendport, self_hostname, s
global seq_num
# uuid: distance map
uuid_distance_map = dict()

# uuid: Node map, store all neighbor nodes
uuid_node_map = dict()

# uuid: linkstate count map, store the latest linkstate sequence number
# Q: how to decide sequence number? many global variables needed?
uuid_linkstate_map = dict()

# Synchronized lock used for maps
# node_map_lock = threading.Lock()
# distance_map_lock = threading.Lock()
# linkstate_map_lock = threading.Lock()
lock = threading.Lock()

def traverse_neighbors():
    print(self_name + "'s Neighbors are: ")
    for uuid in uuid_node_map:
        print(uuid_node_map[uuid].name)

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
            peer_count = int(split_result[1].strip())
    if count_line >= 4:
        peer_nodes = lines[4:]
    if peer_count == None or peer_count < 1:
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
        # print("LISTENING ON ", self_backendport)
        msg_addr = srv.recvfrom(BUFSIZE)
        message_handle_thread = threading.Thread(target = message_handle, args = (msg_addr, srv))
        message_handle_thread.daemon = True
        message_handle_thread.start()




def message_handle(msg_addr, srv):
    msg = msg_addr[0]
    client_addr = msg_addr[1]
    msg = msg.decode()
    # print(msg)
    if (msg.split("_")[0] == "Alive"):
        # print("RECEIVED FROM LOCAL HOST")
        keepalive_handle(msg)

    elif (json.loads(msg)[0] == "LINKSTATE"):
        linkstate_handle(msg)

    # Handle keepalive signal
    # ADDR = (client_addr, port)
    # reply_message = "Server Received: ACK"
    # srv.sendto(reply_message.encode(), client_addr)


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

    lock.acquire()
    uuid_distance_map[uuid] = metric
    tmp_node = Node(uuid = uuid, host_name = host_name, backend_port = backend_port, metric = metric)

    # Set timestamp for keepalive signal
    tmp_node.set_timestamp(datetime.timestamp(datetime.now()))
    uuid_node_map[uuid] = tmp_node
    lock.release()

    # print(uuid_distance_map, uuid_node_map)


# @brief Used for "neighbors" command
# @brief iterate through uuid_node_map (neighbors) to print out related values
def print_active_neighbors():
    # print("INSIDE NEIGHBORS 1")
    result = dict()
    tmp_outer_dict = dict()
    lock.acquire()
    # print("PRINT_ACTIVE_NEIGHBORS", file = sys.stderr)

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
        if (tmp_name != None):
            tmp_outer_dict[tmp_name] = tmp_dict
        else:
            tmp_outer_dict["NOTKNOWN YET:" + uuid] = tmp_dict
            # print("tmp_name is None", tmp_dict,file = sys.stderr)

    result["neighbors"] = tmp_outer_dict
    lock.release()

    print(result)

# @brief Handle keepalive signal from received msg, if it is a new neighbor, add it to map
# @param msg message sent from other content_server which may contain Alive_uuid_timestamps_hostname_backendport_metric
def keepalive_handle(msg):
    # print("KEEPALIVE HANDLE START")
    msg = msg.split("_")
    if msg[0] != "Alive":
        return
    uuid = msg[1]
    node_timestamp = float(msg[2])
    hostname = msg[3]
    backend_port = int(msg[4])
    metric = int(msg[5])
    node_name = msg[6]

    lock.acquire()
    # Update corresponding node's timestamp if it is in the 
    if uuid in uuid_node_map:
        tmp_node = uuid_node_map[uuid]
        if tmp_node.timestamp <= node_timestamp:
            tmp_node.set_timestamp(node_timestamp)
        uuid_node_map[uuid].set_name(node_name)
    else:
        # print("keepalive_handle(): Node not in neighbors map---" + uuid + ", Need to add it")
        new_node = Node(uuid, hostname, backend_port, metric)
        new_node.set_timestamp(node_timestamp)
        new_node.set_name(node_name)
        uuid_node_map[uuid] = new_node
        uuid_distance_map[uuid] = metric
    lock.release()
    # print("KEEPALIVE HANDLE END")



# @brief Send keepalive signal to neighbor nodes
# @param srv server socket used to send messages
def send_keepalive(srv):
    keepalive_thread = threading.Timer(3, send_keepalive, args = (s, ))
    keepalive_thread.daemon = True
    keepalive_thread.start()
    # print("KEEPALIVE SEND START")

    # Iterate through the whole neighbors map to find out stale nodes
    cur_timestamp = datetime.timestamp(datetime.now())
    # print("send keepalive: " + str(cur_timestamp))
    deleted = set()
    lock.acquire()
    for uuid in uuid_node_map:
        tmp_node = uuid_node_map[uuid]
        if (cur_timestamp - tmp_node.timestamp > 10):
            # Avoid dict change during iteration
            deleted.add(uuid)
            
    # lock.acquire()
    for uuid in deleted:
        if uuid in uuid_node_map:
            del uuid_node_map[uuid]
        if uuid in uuid_distance_map:
            del uuid_distance_map[uuid]
        if uuid in uuid_linkstate_map:
            del uuid_linkstate_map[uuid]
        # print("deleted: " + uuid)
    # lock.release()

    msg = "Alive"
    # Send to each neighbor node
    for uuid in uuid_node_map:
        tmp_node = uuid_node_map[uuid]
        tmp_addr = socket.gethostbyname(tmp_node.host_name)
        # print("HOST_NAME: ", tmp_node.host_name)
        # print("SENDING to: ", tmp_addr, "PORT: ", tmp_node.backend_port)
        dt = datetime.now()
        timestamp = datetime.timestamp(dt)
        # protocol: Alive_uuid_timestamp_hostname_backendport_metric_nodename
        msg += "_" + self_uuid + "_" + str(timestamp) + "_" + self_hostname + "_" + str(self_backendport) + "_" + str(uuid_distance_map[uuid]) + "_" + self_name
        ADDR = (tmp_addr, tmp_node.backend_port)
        # print("ADDR: ", ADDR)
        srv.sendto(msg.encode(), ADDR)
        # tmp_node.set_timestamp(timestamp)
    lock.release()
    # print("KEEPALIVE SEND END")

    


# Used together with send_keepalive to remove inactive nodes
def update_neighbors():
    pass

# Initialize uuid_distance map uuid_node map according to the .conf's neighbor nodes
def init_map(peer_count, peer_nodes):
    lock.acquire()
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

    lock.release()


def get_neighbor_name(neighbor_node):
    tmp_addr = socket.gethostbyname(neighbor_node.host_name)
    ADDR = (tmp_addr, neighbor_node.backend_port)
    message = "ASKNAME"
    while True:
        s.sendto(message.encode(), ADDR)
        print("name query sent")
        msg_addr = s.recvfrom(BUFSIZE)
        reply = msg_addr[0]
        client_addr = msg_addr[1]
        reply = reply.decode()
        if(reply.split("_")[0] == "NAME"):
            name_thread
    neighbor_name = reply.split("_")[1]
    return neighbor_name
    
def handle_name(msg, neighbor_addr):
    reply = "NAME" + "_" + self_name
    s.sendto(reply.encode(), neighbor_addr)




# @brief handle linkstate msg from neighbor nodes
# @param msg linkstate msg with the following protocol
# protocol: "LINKSTATE" + uuid + self_name + uuid-metric pairs + sequence # (timestamp)
def linkstate_handle(msg):
    # print("LINKSTATE HANDLE START")

    json_msg = json.loads(msg)
    tmp_uuid = json_msg[1]
    tmp_node_name = json_msg[2]
    tmp_uuid_metric_map = json_msg[3]
    # if(tmp_node_name == "node6"):
    #     print("RECEIVED LINKSTATE FROM NODE 6")
    #     print(tmp_uuid_metric_map)
    tmp_seq_num = json_msg[4]
    lock.acquire()
    # print(tmp_uuid, tmp_node_name, tmp_uuid_metric_map, tmp_seq_num)
    if (tmp_uuid not in uuid_linkstate_map):
        tmp_linkstate = Linkstate(tmp_uuid, tmp_seq_num)
        tmp_linkstate.set_neighbor_metric_map(tmp_uuid_metric_map)
        tmp_linkstate.set_name(tmp_node_name)
        uuid_linkstate_map[tmp_uuid] = tmp_linkstate
    else:
        if uuid_linkstate_map[tmp_uuid].seq_num >= tmp_seq_num:
            # print("LINKSTATE HANDLE END")
            lock.release()
            return
        else:
            # update_linkstate_map(tmp_uuid, tmp_uuid_metric_map)
            tmp_linkstate = uuid_linkstate_map[tmp_uuid]

            tmp_linkstate.set_neighbor_metric_map(tmp_uuid_metric_map)
            tmp_linkstate.set_name(tmp_node_name)
            tmp_linkstate.set_seq_num(tmp_seq_num)
    lock.release()
    # print("LINKSTATE HANDLE END")
    # print("size is: ", len(uuid_linkstate_map))
    # print(self_name + " received #" + str(seq_num) + "linkstate from " + tmp_node_name)
    forward_linkstate(msg, tmp_uuid)

# def update_linkstate_map(node_uuid, uuid_metric_map):
#     lock.acquire()
#     cur_linkstate= uuid_linkstate_map[node_uuid]
#     tmp_map = cur_linkstate.neighbor_metric_map
#     for uuid in tmp_map:
#         if uuid not in uuid_metric_map:
#             if uuid in uuid_linkstate_map:
#                 del uuid_linkstate_map[uuid]
#     lock.release()


# @brief Send linkstate signal to neighbor nodes
# protocol: "LINKSTATE" + uuid + self_name + uuid-[name , metric] pairs + sequence # (timestamp)
def send_linkstate():
    linkstate_thread = threading.Timer(3, send_linkstate)
    linkstate_thread.daemon = True
    linkstate_thread.start()
    global seq_num
    linkstate_msg = []
    tmp_neighbor_distance_map = {}
    linkstate_msg.append("LINKSTATE")
    linkstate_msg.append(self_uuid)
    linkstate_msg.append(self_name)
    # print("LINKSTATE SEND START")
    lock.acquire()
    for uuid in uuid_node_map:
        tmp_neighbor_distance_map[uuid] = []
        # uuid : [neighbor name, metric]
        tmp_neighbor_distance_map[uuid].append(uuid_node_map[uuid].name)
        tmp_neighbor_distance_map[uuid].append(uuid_distance_map[uuid])
    linkstate_msg.append(tmp_neighbor_distance_map)
    seq_num += 1
    linkstate_msg.append(seq_num)
    linkstate_msg = json.dumps(linkstate_msg)

    for uuid in uuid_node_map:
        tmp_node = uuid_node_map[uuid]
        tmp_addr = socket.gethostbyname(tmp_node.host_name)
        ADDR = (tmp_addr, tmp_node.backend_port)
        # if tmp_node.name:
        #     print(self_name + " sent #SEQ NUM" + str(seq_num) + " linkstate to " + tmp_node.name)
        # else:
        #     print(self_name + " sent linkstate to " + uuid)
        s.sendto(linkstate_msg.encode(), ADDR)
    lock.release()
    # print("LINKSTATE SEND END")



def forward_linkstate(linkstate_msg, excluded_node):
    # print("LINKSTATE FORWARD START")
    lock.acquire()
    for uuid in uuid_node_map:
        if (uuid == excluded_node):
            # print("SKIP THIS NODE")
            continue
        tmp_node = uuid_node_map[uuid]
        tmp_addr = socket.gethostbyname(tmp_node.host_name)
        ADDR = (tmp_addr, tmp_node.backend_port)
        # if tmp_node.name:
        #     print(self_name + " forward linkstate to " + tmp_node.name)
        # else:
        #     print(self_name + " forward linkstate to " + uuid)
        s.sendto(linkstate_msg.encode(), ADDR)
    lock.release()
    # print("LINKSTATE FORWARD END")

def print_map():
    result = dict()
    lock.acquire()

    if not uuid_linkstate_map:
        result["map"] = {}
        print(result)
        lock.release()
        return
    self_linkstate = Linkstate(self_uuid, 0)
    self_linkstate.set_name(self_name)
    tmp_neighbor_distance_map = dict()
    for uuid in uuid_node_map:
        tmp_neighbor_distance_map[uuid] = []
        tmp_neighbor_distance_map[uuid].append(uuid_node_map[uuid].name)
        tmp_neighbor_distance_map[uuid].append(uuid_distance_map[uuid])
    self_linkstate.set_neighbor_metric_map(tmp_neighbor_distance_map)
    uuid_linkstate_map[self_uuid] = self_linkstate
    result["map"] = dict()
    inner1 = dict()
    for tmp_linkstate in uuid_linkstate_map.values():
        # tmp_linkstate = uuid_linkstate_map[uuid]
        k1 = copy.deepcopy(tmp_linkstate.name)
        inner1[k1] = dict()
        inner2 = dict()
        for node_metric in tmp_linkstate.neighbor_metric_map.values():
            k2 = copy.deepcopy(node_metric[0])
            inner2[k2] = int(copy.deepcopy(node_metric[1]))
        # print(tmp_linkstate.name, tmp_linkstate.neighbor_metric_map)
        inner1[k1] = copy.deepcopy(inner2)
    result["map"] = copy.deepcopy(inner1)
    print(result)
    lock.release()

def print_rank():
    tmp_dict = find_shortest_path(self_name)
    result = dict()
    result["rank"] = tmp_dict
    print(result)

def find_shortest_path(src):
        priority_queue = []
        heapq.heapify(priority_queue)
        step = 0
        neighbors_graph = get_graph()
        # neighbors_graph = {'node2': {'node3': 20, 'node1': 10}, 'node3': {'node4': 30, 'node1': 20, 'node2': 20},
        #                    'node1': {'node2': 10, 'node3': 20}, 'node4': {'node3': 30}}
        # denotes the distance from source node
        distance = dict()
        if not neighbors_graph:
            return distance
        # denote the nodes have/have not been visited
        visited = set()
        unvisited = set()
        for node in neighbors_graph:
            unvisited.add(node)
            distance[node] = (float("inf"))
        distance[src] = 0
        heapq.heappush(priority_queue, (distance[src], src))

        while priority_queue:
            cur_node = heapq.heappop(priority_queue)
            metric = cur_node[0]
            cur_node_name = cur_node[1]
            if metric > distance[cur_node_name]:
                continue
            neighbors = neighbors_graph[cur_node_name]
            for neighbor_node_name in neighbors:
                # print(neighbors[neighbor_node])
                dist_to_next = neighbors[neighbor_node_name] + distance[cur_node_name]
                if dist_to_next < distance[neighbor_node_name]:
                    distance[neighbor_node_name] = dist_to_next
                    heapq.heappush(priority_queue, (dist_to_next, neighbor_node_name))
        if src in distance:
            del distance[src]
        return distance

def get_graph():

    lock.acquire()
    self_linkstate = Linkstate(self_uuid, 0)
    self_linkstate.set_name(self_name)
    tmp_neighbor_distance_map = dict()
    for uuid in uuid_node_map:
        tmp_neighbor_distance_map[uuid] = []
        tmp_neighbor_distance_map[uuid].append(uuid_node_map[uuid].name)
        tmp_neighbor_distance_map[uuid].append(uuid_distance_map[uuid])
    self_linkstate.set_neighbor_metric_map(tmp_neighbor_distance_map)
    uuid_linkstate_map[self_uuid] = self_linkstate
    inner1 = dict()
    for tmp_linkstate in uuid_linkstate_map.values():
        # tmp_linkstate = uuid_linkstate_map[uuid]
        k1 = copy.deepcopy(tmp_linkstate.name)
        inner1[k1] = dict()
        inner2 = dict()
        for node_metric in tmp_linkstate.neighbor_metric_map.values():
            k2 = copy.deepcopy(node_metric[0])
            inner2[k2] = int(copy.deepcopy(node_metric[1]))
        # print(tmp_linkstate.name, tmp_linkstate.neighbor_metric_map)
        inner1[k1] = copy.deepcopy(inner2)

    lock.release()
    return copy.deepcopy(inner1)


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

    # Create socket instance
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self_hostname = socket.gethostname()

    localip = socket.gethostbyname(socket.gethostname())
    # localip = "127.0.0.1"

    try:
        s.bind((localip, self_backendport))
    except socket.error as e:
        sys.exit(-1)

    if len(parsed_result) > 3:
        peer_count = int(parsed_result[3])
        peer_nodes = parsed_result[4]

        # Initialize neighbor nodes and distances according to configure file
        init_map(peer_count, peer_nodes)
        # print("GET NEIGHBORS FROM CONF", peer_count, peer_nodes)



    handle_thread = threading.Thread(target = client_handle, args = (s, ))
    handle_thread.daemon = True
    handle_thread.start()

    send_keepalive(s)
    seq_num = 0
    send_linkstate()
    # print("MAIN LOOP")
    # main loop
    while True:

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
            print_map()
        elif command_line_msg == "rank":
            print_rank()