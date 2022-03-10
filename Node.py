class Node:
    def __init__(self, uuid, name, port, peer_count = 0, peer_nodes = None):
        self.uuid = uuid
        self.host_name = name
        self.port = port
        self.peer_count = peer_count
        self.peer_nodes = peer_nodes
