from utils.context_manager import ContextManager
from utils.decorators import log_execution, handle_exceptions

class BaseManager:
    """
    A base class providing shared functionality for all managers.
    """

    def __init__(self):
        self.context = ContextManager.get_context()

    @log_execution
    @handle_exceptions
    def open_data_docs(self):
        """
        Open the Great Expectations Data Docs.
        """
        self.context.open_data_docs()