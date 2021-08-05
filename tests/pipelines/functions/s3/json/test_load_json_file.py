import json
import pytest
import sys
from unittest.mock import patch
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.functions.s3.json.load_json_file import run


@pytest.mark.functions
class TestLoadJsonFile:
    def test_run_loads_file(self):
        # SETUP
        bucket_name = 'some_bucket'
        filename = 'aa/filename.json'

        # EXECUTE
        with patch.object(S3Service, 'get_file_contents', return_value=json.dumps({'foo': 'bar'})) as mock_method:
            received_return = run(bucket_name, filename)

        # ASSERT
        assert received_return == {
            'location': {
                'bucket_name': bucket_name,
                'key': 'aa/filename.json',
            },
            'json': {
                'foo': 'bar'
            }

        }
        assert len(mock_method.call_args_list) == 1
        expected = ((bucket_name, 'aa/filename.json'),)
        assert mock_method.call_args_list[0] == expected
