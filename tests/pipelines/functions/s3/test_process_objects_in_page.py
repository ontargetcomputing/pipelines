import pytest
import sys
from unittest.mock import patch
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.functions.s3.process_objects_in_page import run


@pytest.mark.functions
class TestProcessObjectsInPage:
    def test_run_when_truncated(self):
        # SETUP
        def process_func(current_key):
            return 1

        options = {
            'page_size': 2,
            'bucket_name': 'awscertprepmanzano-code',
            'starting_token': ''
        }

        # EXECUTE
        mock_return = {
            "IsTruncated": True,
            "Contents": [
                {
                    "Key": "file1.html",
                },
                {
                    "Key": "file2.html",
                }
            ]
        }

        with patch.object(S3Service, 'list_objects_page', return_value=mock_return):
            received_return = run(options, process_func)

        # ASSERT
        assert received_return == {
            'another_page': 'true',
            'starting_token': 'file2.html'
        }

    def test_run_when_not_truncated(self):
        # SETUP
        def process_func(current_key):
            return 1

        options = {
            'page_size': 2,
            'bucket_name': 'awscertprepmanzano-code',
            'starting_token': ''
        }

        # EXECUTE
        mock_return = {
            "IsTruncated": False,
            "Contents": [
                {
                    "Key": "file1.html",
                },
                {
                    "Key": "file2.html",
                }
            ]
        }

        with patch.object(S3Service, 'list_objects_page', return_value=mock_return):
            received_return = run(options, process_func)

        print(received_return)
        # ASSERT
        assert received_return == {
            'another_page': 'false',
            'starting_token': ''
        }
