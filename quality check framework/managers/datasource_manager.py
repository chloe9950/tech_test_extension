from managers.base_manager import BaseManager
from utils.decorators import log_execution, handle_exceptions

class DatasourceManager(BaseManager):
    """
    Manages the setup of datasources in Great Expectations.
    """

    def __init__(self, connection_string):
        super().__init__()
        self.connection_string = connection_string

    @log_execution
    @handle_exceptions
    def setup_datasource(self, datasource_name="pg_datasource"):
        """
        Set up the PostgreSQL datasource in Great Expectations.
        """
        return self.context.sources.add_postgres(
            name=datasource_name, connection_string=self.connection_string
        )