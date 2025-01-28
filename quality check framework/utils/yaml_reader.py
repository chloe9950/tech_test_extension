import yaml


class YAMLReader:
    """
    A class to load and apply YAML-defined expectations and datasource configuration.
    """

    def __init__(self, yaml_file_path):
        """
        Initialize the YAMLReader with a file path.
        """
        self.yaml_file_path = yaml_file_path
        self.config = None
        self._load_yaml()

    def _load_yaml(self):
        """
        Load the YAML file content into the config attribute.
        """
        try:
            with open(self.yaml_file_path, "r") as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML file not found: {self.yaml_file_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")

    def apply_expectations(self, validator, cross_table_data=None):
        """
        Apply expectations from the YAML file to the given validator.
        """
        expectations = self.config.get("expectations", [])
        for item in expectations:
            if "column" in item:
                column = item["column"]
                # Regular expectations
                for rule in item.get("rules", []):
                    expectation_type = rule["type"]
                    params = rule.get("params", {})
                    getattr(validator, expectation_type)(column=column, **params)
                # Cross-table validations
                if "cross_table" in item:
                    cross_rule = item["cross_table"]
                    source_table = cross_rule["source_table"]
                    source_column = cross_rule["source_column"]
                    if cross_table_data and source_table in cross_table_data:
                        value_set = cross_table_data[source_table][source_column]
                        validator.expect_column_values_to_be_in_set(column=column, value_set=value_set)
            elif "table" in item:
                # Table-level rules
                for rule in item["table"].get("rules", []):
                    expectation_type = rule["type"]
                    params = rule.get("params", {})
                    getattr(validator, expectation_type)(**params)