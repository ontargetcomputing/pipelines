import botocore
import json
import os
import pytest
import sys
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service

REGION = "us-east-1"


@pytest.mark.aws
class TestS3:
    def test_write_file(self, s3_client, s3_resource):
        # SETUP
        bucket_name = "bucket"
        s3_client.create_bucket(Bucket=bucket_name)

        filename = "file.csv"
        text = "foo,bar\nhello,world\n"

        # EXECUTE
        s3_service = S3Service(region=REGION)
        s3_service.write_file(bucket_name, filename, text)

        # ASSERT
        try:
            s3_obj = s3_resource.Object(bucket_name, filename)
            retrieved_text = s3_obj.get()['Body'].read().decode("utf-8")
        except botocore.exceptions.ClientError:
            print("Exception")

        assert retrieved_text == text

    def test_copy_file(self, s3_client, s3_resource):
        # SETUP
        source_bucket_name = "source_bucket"
        s3_client.create_bucket(Bucket=source_bucket_name)

        filename = "source_file.txt"
        file_text = "You can go about your business, move along"

        s3_client.put_object(
            Bucket=source_bucket_name,
            Body=file_text,
            Key=filename
        )
        destination_bucket_name = "destination_bucket"
        s3_client.create_bucket(Bucket=destination_bucket_name)
        os.environ["region"] = "us-east-1"
        os.environ["use_local_s3"] = "false"
        os.environ["local_s3_endpoint_url"] = ""
        os.environ['local_s3_aws_access_key'] = ""
        os.environ['local_s3_aws_secret_access_key'] = ""

        # EXECUTE
        s3_service = S3Service(region=REGION)
        s3_service.copy_file(source_bucket_name, filename, destination_bucket_name, filename)

        # ASSERT
        body = "These aren't the droids you're looking for"
        try:
            obj = s3_resource.Object(destination_bucket_name, filename)
            body = obj.get()['Body'].read().decode("utf-8")
        except botocore.exceptions.ClientError:
            print("Exception")

        assert body == file_text

    def test_file_exists_works_when_not_there(self, s3_client, s3_resource):
        # SETUP
        bucket = "source_bucket"
        s3_client.create_bucket(Bucket=bucket)

        # EXECUTE
        s3_service = S3Service(region=REGION)
        exists = s3_service.file_exists(bucket, "missing_file.txt")

        # ASSERT
        assert (exists is False)

    def test_get_file_contents(self, s3_client, s3_resource):
        # SETUP
        bucket = "source_bucket"
        s3_client.create_bucket(Bucket=bucket)

        key = "file.json"
        obj = {
            "foo": {
                "bar": "whiz",
                "bang": "wow"
            }
        }
        s3_client.put_object(
            Bucket=bucket,
            Body=json.dumps(obj),
            Key=key
        )

        # EXECUTE
        s3_service = S3Service(region=REGION)
        json_object = json.loads(s3_service.get_file_contents(bucket, key))

        # ASSERT
        assert json_object == obj
