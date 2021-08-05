import pytest
import sys
from unittest.mock import patch
sys.path.append("src")
from pipelines.integration.aws.step_functions import StepFunctionsService
from pipelines.functions.s3.events.initiate_pipeline import run


@pytest.mark.functions
class TestMoveFile:
    def test_something(self):
        # SETUP

        s3_event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": "bucket-name",
                        },
                        "object": {
                            "key": "object-key",
                        }
                    }
                }
            ]
        }

        state_machine_options = {
            'state_machine_arn': 'my-arn',
            'execution_name': 'execution_name'
        }

        state_machine_input = {
            'location': {
                'bucket_name': 'bucket-name',
                'key': 'object-key'
            }
        }

        # EXECUTE
        with patch.object(StepFunctionsService, 'start', return_value=True) as mock_method:
            received_return = run(s3_event, state_machine_options)

        # ASSERT
        assert received_return
        mock_method.assert_called_once_with(arn='my-arn', name='execution_name', input=state_machine_input)
