class Node:
    def __init__(self, uuid, host_name, backend_port, metric):
        self.uuid = uuid
        self.host_name = host_name
        self.backend_port = int(backend_port)
        self.metric = int(metric)
        self.name = None

    def set_name(self, name):
        self.name = name
    
    def set_timestamp(self, timestamp):
        self.timestamp = float(timestamp)
