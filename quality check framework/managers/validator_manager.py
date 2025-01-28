from managers.base_manager import BaseManager
from utils.yaml_reader import YAMLReader
from utils.decorators import log_execution, handle_exceptions


class ValidatorManager(BaseManager):
    """
    Manages validators and application of expectations.
    """

    def __init__(self, datasource):
        super().__init__()
        self.datasource = datasource
        self.assets = {}  # Store assets globally for reuse

    def setup_asset(self, table_name):
        """
        Set up the table asset and store it globally.
        """
        if table_name not in self.assets:
            self.assets[table_name] = self.datasource.add_table_asset(
                name=table_name, table_name=table_name
            )

    def get_asset(self, table_name):
        """
        Retrieve the table asset from the global assets dictionary.
        """
        return self.assets.get(table_name)

    def setup_validator(self, table_name, suite_name):
        """
        Set up a validator for a specific table and expectation suite.
        """
        self.setup_asset(table_name)
        asset = self.get_asset(table_name)
        self.context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
        return self.context.get_validator(
            batch_request=asset.build_batch_request(), expectation_suite_name=suite_name
        )

    def get_batch_request(self, table_name):
        """
        Generate a batch request for the specified table.
        """
        asset = self.get_asset(table_name)
        if not asset:
            raise ValueError(f"Asset for table {table_name} not found.")
        return asset.build_batch_request()

    @log_execution
    @handle_exceptions
    def apply_expectations(self, validator, yaml_file, cross_table_data=None):
        """
        Apply expectations to the validator based on the YAML configuration.
        """
        yaml_reader = YAMLReader(yaml_file)
        yaml_reader.apply_expectations(validator, cross_table_data)

    @log_execution
    @handle_exceptions
    def extract_table_data(self, validator, yaml_file):
        """
        Extract data for cross-table validation based on YAML configuration.
        """
        yaml_reader = YAMLReader(yaml_file)
        expectations = yaml_reader.config.get("expectations", [])
        extracted_data = {}
        for item in expectations:
            if "column" in item:
                column = item["column"]
                extracted_data[column] = validator.head(fetch_all=True)[column].drop_duplicates().tolist()
        return extracted_data