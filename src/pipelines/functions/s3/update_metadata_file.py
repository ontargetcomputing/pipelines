import json
import logging
from datetime import datetime
import sys
sys.path.append("src")
from pipelines.format.json.json_mutator import JsonMutator
from pipelines.integration.aws.s3 import S3Service
from pipelines.lang.filename_investigator import FilenameInvestigator

s3_service = S3Service()
filename_investigator = FilenameInvestigator()

# TODO - refactor this and make it more reusable


def run(bucket_name, filename, metadata_bucket_name, non_json_bucket_name):
    json_filename = f'{filename_investigator.determine_base_filename(filename)}.json'

    logging.debug(f'Updating {metadata_bucket_name}/{json_filename}')
    json_obj = json.loads(s3_service.get_file_contents(metadata_bucket_name, json_filename))
    json_mutator = JsonMutator(json_obj)
    json_mutator.insert_value_to_path('old_location', f's3://{bucket_name}/{filename}')
    json_mutator.insert_value_to_path('new_location', f's3://{non_json_bucket_name}/{filename}')
    now = datetime.now()
    json_mutator.insert_value_to_path('moved_at', now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    s3_service.write_file(metadata_bucket_name, json_filename, json.dumps(json_mutator.json()))
    return json_mutator.json()
