from managers.datasource_manager import DatasourceManager
from managers.validator_manager import ValidatorManager
from managers.checkpoint_manager import CheckpointManager


class CodeRunner:
    """
    Orchestrates the data validation workflow using the framework classes.
    """

    def __init__(self, connection_string):
        self.datasource_manager = DatasourceManager(connection_string)
        self.datasource = self.datasource_manager.setup_datasource()
        self.validator_manager = ValidatorManager(self.datasource)
        self.checkpoint_manager = CheckpointManager()
        self.cross_table_data = {}

    def run(self, table_names):
        """
        Run validations for the specified tables.
        """
        for table_name in table_names:
            print(f"Processing table: {table_name}")
            suite_name = f"{table_name}_suite"
            yaml_file = f"config/{table_name}_expectations.yml"

            validator = self.validator_manager.setup_validator(table_name, suite_name)

            self.validator_manager.apply_expectations(validator, yaml_file, self.cross_table_data)

            extracted_data = self.validator_manager.extract_table_data(validator, yaml_file)
            self.cross_table_data[table_name] = extracted_data

            # Save expectations
            validator.save_expectation_suite(discard_failed_expectations=False)

            batch_request = self.validator_manager.get_batch_request(table_name)

            # checkpoint
            self.checkpoint_manager.run_checkpoint(
                table_name=table_name,
                suite_name=suite_name,
                batch_request=batch_request,
            )

        self.datasource_manager.open_data_docs()