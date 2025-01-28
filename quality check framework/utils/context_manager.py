import great_expectations as gx


class ContextManager:
    """
    Singleton to manage a persistent Great Expectations DataContext.
    """
    _instance = None

    @classmethod
    def get_context(cls):
        if cls._instance is None:
            cls._instance = gx.get_context()
        return cls._instance