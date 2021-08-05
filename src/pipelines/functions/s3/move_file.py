import logging
import sys
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service

s3_service = S3Service()


def run(source_bucket_name, source_filename, destination_bucket_name, destination_filename):
    logging.debug(f'Moving {source_bucket_name}/{source_filename} to {destination_bucket_name}/{destination_filename}')

    s3_service.copy_file(source_bucket_name, source_filename, destination_bucket_name, destination_filename)

    return {
        'bucket_name': destination_bucket_name,
        'key': destination_filename
    }
