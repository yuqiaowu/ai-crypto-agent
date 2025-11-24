class BaseSettings:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

def SettingsConfigDict(**kwargs):
    return kwargs