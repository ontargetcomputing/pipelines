import pytest
import sys
from unittest.mock import patch
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.functions.s3.filename_exists_with_different_ext import run


@pytest.mark.functions
class TestFilenameExistsWithDifferentExt:
    def test_run_exists_and_filename_has_extension(self):
        # SETUP
        bucket_name = 'some_bucket'
        filename = 'aa/filename.ext'
        extension = 'json'

        # EXECUTE

        with patch.object(S3Service, 'file_exists', return_value=True) as mock_method:
            received_return = run(bucket_name, filename, extension)

        # ASSERT
        assert received_return == {
            'bucket_name': bucket_name,
            'key': 'aa/filename.json',
            'exists': True
        }
        assert len(mock_method.call_args_list) == 1
        expected = ((bucket_name, 'aa/filename.json'),)
        assert mock_method.call_args_list[0] == expected

    def test_run_not_exists_and_filename_has_extension(self):
        # SETUP
        bucket_name = 'some_bucket'
        filename = 'aa/filename.ext'
        extension = 'json'

        # EXECUTE

        with patch.object(S3Service, 'file_exists', return_value=False) as mock_method:
            received_return = run(bucket_name, filename, extension)

        # ASSERT
        assert received_return == {
            'bucket_name': bucket_name,
            'key': 'aa/filename.json',
            'exists': False
        }
        assert len(mock_method.call_args_list) == 1
        expected = ((bucket_name, 'aa/filename.json'),)
        assert mock_method.call_args_list[0] == expected
