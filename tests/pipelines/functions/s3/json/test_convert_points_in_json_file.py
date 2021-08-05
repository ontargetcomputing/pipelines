import json
import pytest
import sys
from unittest.mock import patch
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.functions.s3.json.convert_points_in_json_file import run


@pytest.mark.functions
class TestConvertPointsInJsonFile:
    def test_run_returns_correct_value(self):
        # SETUP
        bucket_name = 'some_bucket'
        filename = 'aa/filename.json'
        destination_bucket = 'new_bucket'
        content = json.dumps({
            'foo': {
                'bar': 'value'
            },
            'whiz': 'bang'
        })

        expected = {
            'bucket_name': bucket_name,
            'key': 'aa/filename.json',
            'content': 'removed to save space'
        }
        # EXECUTE
        with patch.object(S3Service, 'get_file_contents', return_value=content):
            with patch.object(S3Service, 'write_file', return_value={
                'bucket_name': bucket_name,
                'key': 'aa/filename.json',
                'content': content
            }):
                received_return = run(bucket_name, filename, destination_bucket)

        # ASSERT
        assert received_return == expected
