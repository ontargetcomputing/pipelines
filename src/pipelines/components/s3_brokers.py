from collections import OrderedDict
from datetime import datetime
import json
import logging
import sys
sys.path.append("src")
from pipelines.json.json_mutator import JsonMutator
from pipelines.aws.s3 import S3Service
from pipelines.lang.filename_investigator import FilenameInvestigator

s3_service = S3Service()
filename_investigator = FilenameInvestigator()


def corresponding_json_file_exists(bucket_name, non_json_filename, exists_check):
    stripped_filename = filename_investigator.determine_base_filename(non_json_filename)
    json_filename = f'{stripped_filename}.json'
    logging.debug(f'Checking for existence of {bucket_name}/{json_filename}, corresponding to {non_json_filename}')
    exists = s3_service.file_exists(bucket_name, json_filename)

    count = 0
    if exists_check is not None:
        count = exists_check['count']

    count = count + 1

    return {
        'bucket_name': bucket_name,
        'key': json_filename,
        'exists': exists,
        'count': count
    }


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


def move_file(source_bucket_name, source_filename, destination_bucket_name, destination_filename):
    logging.debug(f'Moving {source_bucket_name}/{source_filename} to {destination_bucket_name}/{destination_filename}')

    s3_service.copy_file(source_bucket_name, source_filename, destination_bucket_name, destination_filename)

    return {
        'bucket_name': destination_bucket_name,
        'key': destination_filename
    }


def update_metadata_file(bucket_name, filename, metadata_bucket_name, non_json_bucket_name):
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


def write_file(bucket_name, filename, content):
    logging.debug(f'Writing {filename} to {bucket_name}')

    return s3_service.write_file(bucket_name, filename, content)


def convert_points_in_json_file(bucket_name, filename, destination_bucket_name):
    json_obj = json.loads(s3_service.get_file_contents(bucket_name, filename))
    json_mutator = JsonMutator(json_obj)
    json_mutator.replace_points_with_lat_long()
    response = s3_service.write_file(destination_bucket_name, filename, json.dumps(json_mutator.json()))
    # This method is used for large json files, we don't want to return the whole state
    response['content'] = 'removed to save space'
    return response


def flatten_json_file(bucket_name, filename, destination_bucket_name):
    json_obj = json.loads(s3_service.get_file_contents(bucket_name, filename))
    json_mutator = JsonMutator(json_obj)
    json_mutator.flatten()
    response = s3_service.write_file(destination_bucket_name, filename, json.dumps(json_mutator.json()))
    # This method is used for large json files, we don't want to return the whole state
    response['content'] = 'removed to save space'
    return response


def convert_json_file_to_csv(bucket_name, filename, destination_bucket_name):
    json_obj = json.loads(s3_service.get_file_contents(bucket_name, filename))
    json_mutator = JsonMutator(json_obj)
    stripped_filename = filename_investigator.determine_base_filename(filename)
    csv_filename = f'{stripped_filename}.csv'

    response = s3_service.write_file(destination_bucket_name, csv_filename, json_mutator.csv())
    # This method is used for large json files, we don't want to return the whole state
    response['content'] = 'removed to save space'
    return response


def gather_and_write_telemetry(external_landing_bucket_name, filename, internal_landing_bucket_name, final_landing_bucket_name,
                               consumption_bucket_name, consumption_final_extension, telemetry_bucket_name):
    original_metadata = s3_service.metadata(external_landing_bucket_name, filename)

    telemetry = OrderedDict()

    telemetry['original_landed_time'] = original_metadata.last_modified
    telemetry['original_e_tag'] = original_metadata.e_tag
    telemetry['original_filesize'] = original_metadata.content_length
    telemetry['original_filetype'] = original_metadata.content_type

    internal_metadata = s3_service.metadata(internal_landing_bucket_name, filename)
    telemetry['internal_landed_time'] = internal_metadata.last_modified
    final_metadata = s3_service.metadata(final_landing_bucket_name, filename)
    telemetry['final_landed_time'] = final_metadata.last_modified
    telemetry['final_filesize'] = final_metadata.content_length
    if consumption_final_extension is not None:
        stripped_filename = filename_investigator.determine_base_filename(filename)
        consumption_filename = f'{stripped_filename}{consumption_final_extension}'
    else:
        consumption_filename = filename
    consumption_metadata = s3_service.metadata(consumption_bucket_name, consumption_filename)
    telemetry['consumption_landed_time'] = consumption_metadata.last_modified
    telemetry['consumption_filesize'] = consumption_metadata.content_length

    json_mutator = JsonMutator(telemetry)
    csv = json_mutator.csv()

    filename_to_write = filename.replace(".", "_")
    return s3_service.write_file(telemetry_bucket_name, f'{filename_to_write}.csv', csv)
