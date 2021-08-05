import logging
import sys
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service

s3_service = S3Service()


def run(bucket_name, filename, content):
    logging.debug(f'Writing {filename} to {bucket_name}')

    return s3_service.write_file(bucket_name, filename, content)
