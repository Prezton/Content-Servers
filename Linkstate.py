class Linkstate:
    def __init__(self, uuid, seq_num):
        self.uuid = uuid
        self.neighbor_metric_map = None
        self.seq_num = seq_num
        self.name = None

    def set_seq_num(self, seq_num):
        self.seq_num = seq_num
    
    def set_neighbor_metric_map(self, neighbor_metric_map):
        self.neighbor_metric_map = neighbor_metric_map
    
    def set_name(self, name):
        self.name = name