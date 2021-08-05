import logging
import sys
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service

s3_service = S3Service()


def run(s3_event, destination_bucket_name):
    filename = s3_event['Records'][0]['s3']['object']['key']
    bucket_name = s3_event['Records'][0]['s3']['bucket']['name']
    logging.debug(f'Processing {bucket_name}/{filename}')

    s3_service.copy_file(bucket_name, filename, destination_bucket_name, filename)

    return {
        'bucket_name': destination_bucket_name,
        'key': filename
    }
