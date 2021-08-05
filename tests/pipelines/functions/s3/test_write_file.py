import pytest
import sys
from unittest.mock import patch
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.functions.s3.write_file import run


@pytest.mark.functions
class TestMoveFile:
    def test_run_returns_correct_value(self):
        # SETUP
        bucket_name = 'some_bucket'
        filename = 'aa/filename.json'
        content = 'dkfjdkfjdkfjdfj'

        return_value = {
            'bucket_name': bucket_name,
            'key': filename,
        }
        # EXECUTE
        with patch.object(S3Service, 'write_file', return_value=return_value) as mock_method:
            received_return = run(bucket_name, filename, content)

        # ASSERT
        assert received_return == return_value
        assert len(mock_method.call_args_list) == 1
        expected = ((bucket_name, filename, content),)
        assert mock_method.call_args_list[0] == expected
