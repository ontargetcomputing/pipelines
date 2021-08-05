import boto3
import json
import pytest
from moto import mock_iam
from moto import mock_s3
from moto import mock_stepfunctions


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


@pytest.fixture
def sf_client():
    with mock_stepfunctions():
        conn = boto3.client("stepfunctions", region_name="us-east-1")
        yield conn


@pytest.fixture
def iam_admin_role_arn():
    with mock_iam():
        conn = boto3.client("iam")

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": ["stepfunctions.amazonaws.com"]
                    },
                    "Action": ["sts:AssumeRole"]
                }
            ]
        }
        response = conn.create_role(
            Path='/',
            RoleName='AdminRole',
            AssumeRolePolicyDocument=json.dumps(policy)
        )

        yield response['Role']['Arn']
