class StrictRedis:
    def __init__(self, host=None, port=None, db=None, password=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
    def get(self, key):
        return None
    def set(self, key, value):
        return True