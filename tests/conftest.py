import boto3
import pytest
from moto import mock_s3


@pytest.fixture
def s3_client():
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn


@pytest.fixture
def s3_resource():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        yield conn
