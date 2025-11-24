class FileLock:
    def __init__(self, *args, **kwargs):
        self.file = args[0] if args else None
    def acquire(self, timeout=None):
        return True
    def release(self):
        return True
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False