from managers.base_manager import BaseManager
import great_expectations as gx


class CheckpointManager(BaseManager):
    """
    Manages checkpoints in Great Expectations.
    """

    def __init__(self):
        super().__init__()

    def run_checkpoint(self, table_name, suite_name, batch_request):
        """
        Create and run a checkpoint for the specified table.
        """
        checkpoint = gx.checkpoint.Checkpoint(
            name=f"{table_name}_checkpoint",
            data_context=self.context,
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": suite_name,
                     "result_format": {
                        "result_format": "COMPLETE", 
                        "include_unexpected_rows": True,
                    },
                }
            ],
            action_list=[
                {"name": "store_validation_result", "action": {"class_name": "StoreValidationResultAction"}},
                {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
            ],
        )
        result = checkpoint.run()
        print(f"Checkpoint result for {table_name}: {result}")