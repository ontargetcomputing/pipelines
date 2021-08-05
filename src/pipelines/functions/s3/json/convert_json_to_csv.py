import json
import sys
sys.path.append("src")
from pipelines.format.json.json_mutator import JsonMutator
from pipelines.integration.aws.s3 import S3Service
from pipelines.lang.filename_investigator import FilenameInvestigator

s3_service = S3Service()
filename_investigator = FilenameInvestigator()


def run(bucket_name, filename, destination_bucket_name):
    json_obj = json.loads(s3_service.get_file_contents(bucket_name, filename))
    json_mutator = JsonMutator(json_obj)
    stripped_filename = filename_investigator.determine_base_filename(filename)
    csv_filename = f'{stripped_filename}.csv'

    response = s3_service.write_file(destination_bucket_name, csv_filename, json_mutator.csv())
    # This method is used for large json files, we don't want to return the whole state
    response['content'] = 'removed to save space'
    return response
