"""
This function loops through a page of objects in S3 and "processes" them
"""
import logging
import sys
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service

s3_service = S3Service()


def run(options, process_func):
    page_size = options.get('page_size', 990)
    bucket_name = options['bucket_name']
    starting_token = options['starting_token']

    logging.debug(f'Loading {page_size} object from {bucket_name} starting at {starting_token}')
    page = s3_service.list_objects_page(bucket_name, page_size, starting_token)
    total_files_this_page = 0
    current_key = ''
    for metadata in page['Contents']:
        current_key = metadata['Key']
        total_files_this_page = total_files_this_page + 1
        logging.debug(f'Processing {total_files_this_page}. {current_key}')
        process_func(current_key)

    if page['IsTruncated'] is True:
        return {
            'another_page': 'true',
            'starting_token': current_key
        }
    else:
        return {
            'another_page': 'false',
            'starting_token': ''
        }
