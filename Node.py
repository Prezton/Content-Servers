class Node:
    def __init__(self, uuid, host_name, backend_port, metric):
        self.uuid = uuid
        self.host_name = host_name
        self.backend_port = backend_port
        self.metric = metric
        self.name = None

    def set_name(name):
        self.name = name
