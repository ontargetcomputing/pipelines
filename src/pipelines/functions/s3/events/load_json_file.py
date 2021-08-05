import json
import logging
import sys
sys.path.append("src")
from pipelines.aws.s3 import S3Service
from pipelines.lang.filename_investigator import FilenameInvestigator

s3_service = S3Service()
filename_investigator = FilenameInvestigator()


def load_json_file(bucket_name, filename):
    logging.debug(f'Loading {bucket_name}/{filename}')

    return {
        'location': {
            'bucket_name': bucket_name,
            'key': filename,
        },
        'base_filename': filename_investigator.determine_base_filename(filename),
        'json': json.loads(s3_service.get_file_contents(bucket_name, filename))
    }
