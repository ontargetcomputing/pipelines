import boto3
import botocore
import logging


class S3Service:
    def __init__(self, region='us-east-1', s3_local=None):
        self.session = boto3.Session()
        if s3_local is not None:
            endpoint_url = s3_local['endpoint_url']
            local_s3_aws_access_key = s3_local['aws_access_key']
            local_s3_aws_secret_access_key = s3_local['aws_secret_access_key']

            logging.debug(f'\nUsing local s3 with {endpoint_url} in {region}')
            self.client = boto3.client("s3", region_name=region, endpoint_url=endpoint_url, aws_access_key_id=local_s3_aws_access_key, aws_secret_access_key=local_s3_aws_secret_access_key)
            self.resource = self.session.resource('s3', region_name=region, endpoint_url=endpoint_url, aws_access_key_id=local_s3_aws_access_key, aws_secret_access_key=local_s3_aws_secret_access_key)
        else:
            logging.debug('\nNot using local s3')
            self.client = boto3.client("s3", region_name=region)
            self.resource = self.session.resource('s3', region_name=region)

    # def __init__(self, region, s3_local):
    #     # TODO : pass these parameters in
    #     region = os.environ['region']
    #     use_local_s3 = os.environ['use_local_s3']
    #     self.session = boto3.Session()
    #     if use_local_s3 == 'true':
    #         endpoint_url = os.environ['local_s3_endpoint_url']
    #         local_s3_aws_access_key = os.environ['local_s3_aws_access_key']
    #         local_s3_aws_secret_access_key = os.environ['local_s3_aws_secret_access_key']

    #         logging.debug(f'\nUsing local s3 with {endpoint_url} in {region}')
    #         self.client = boto3.client("s3", region_name=region, endpoint_url=endpoint_url, aws_access_key_id=local_s3_aws_access_key, aws_secret_access_key=local_s3_aws_secret_access_key)
    #         self.resource = self.session.resource('s3', region_name=region, endpoint_url=endpoint_url, aws_access_key_id=local_s3_aws_access_key, aws_secret_access_key=local_s3_aws_secret_access_key)
    #     else:
    #         logging.debug('\nNot using local s3')
    #         self.client = boto3.client("s3", region_name=region)
    #         self.resource = self.session.resource('s3', region_name=region)

    def copy_file(self, source_bucket, source_key, destination_bucket, destination_key):
        logging.debug(f'Copying {source_bucket}/{source_key} to {destination_bucket}/{destination_key}')
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }

        bucket = self.resource.Bucket(destination_bucket)
        obj = bucket.Object(destination_key)
        obj.copy(copy_source)
        return True

    def file_exists(self, bucket_name, key):
        bucket = self.resource.Bucket(bucket_name)
        obj = bucket.Object(key)
        try:
            obj.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                logging.debug(f'Unable to load object {e.response["Error"]["Code"]}')

                raise
        else:
            return True

    def get_file_contents(self, bucket_name, key):
        bucket = self.resource.Bucket(bucket_name)
        obj = bucket.Object(key)
        return obj.get()['Body'].read().decode("utf-8")

    def list_objects_page(self, bucket_name, page_size, starting_token=""):
        paginator = self.client.get_paginator('list_objects')
        pagination_config = {
            'MaxItems': page_size,
            'PageSize': page_size
        }
        if starting_token != "":
            pagination_config['StartingToken'] = starting_token

        page_iter = iter(paginator.paginate(Bucket=bucket_name, PaginationConfig=pagination_config))
        return next(page_iter)

    def write_file(self, bucket_name, key, content):
        logging.debug(f'Writing to {bucket_name}/{key}')
        bucket = self.resource.Bucket(bucket_name)

        response = bucket.put_object(
            Body=content,
            Key=key
        )

        return {
            'location': {
                "bucket_name": bucket_name,
                "key": key
            },
            "content": content,
            "content_length": response.content_length
        }

    # TODO : unit test this
    def metadata(self, bucket_name, key):
        logging.debug(f'Obtaining metadata {bucket_name}/{key}')
        return self.resource.Object(bucket_name, key)
