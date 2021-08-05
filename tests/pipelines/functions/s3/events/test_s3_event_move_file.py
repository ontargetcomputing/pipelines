import pytest
import sys
from unittest.mock import patch
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.functions.s3.events.s3_event_move_file import run


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

        destination_bucket = 'destination_bucket'

        # EXECUTE
        with patch.object(S3Service, 'copy_file', return_value=True) as mock_method:
            received_return = run(s3_event, destination_bucket)

        # ASSERT
        assert received_return == {
            'bucket_name': destination_bucket,
            'key': "object-key"
        }
        mock_method.assert_called_once_with('bucket-name', 'object-key', destination_bucket, 'object-key')
