import pytest
import sys
from unittest.mock import patch
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.functions.s3.move_file import run


@pytest.mark.functions
class TestMoveFile:
    def test_run_returns_correct_value(self):
        # SETUP
        source_bucket_name = 'some_bucket'
        source_filename = 'aa/filename.json'
        destination_bucket_name = 'destination'
        destination_filename = 'bb/newfile.bb'

        # EXECUTE
        with patch.object(S3Service, 'copy_file', return_value=True) as mock_method:
            received_return = run(source_bucket_name, source_filename, destination_bucket_name, destination_filename)

        # ASSERT
        assert received_return == {
            'bucket_name': destination_bucket_name,
            'key': destination_filename,
        }
        assert len(mock_method.call_args_list) == 1
        expected = ((source_bucket_name, source_filename, destination_bucket_name, destination_filename),)
        assert mock_method.call_args_list[0] == expected
