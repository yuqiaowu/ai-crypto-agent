class AlreadyAcquired(Exception):
    pass

class Lock:
    def __init__(self, redis_t, name, id=None):
    
        self.redis_t = redis_t
        self.name = name
        self.id = id
        self._locked = False
    def acquire(self, timeout=None):
        if self._locked:
            raise AlreadyAcquired()
        self._locked = True
        return True
    def release(self):
        self._locked = False
        return True
    def reset(self):
        self._locked = False
        return True

def reset_all(redis_t):
    return True