from collections import OrderedDict
from datetime import datetime
import json
import logging
import sys
sys.path.append("src")
from pipelines.json.json_mutator import JsonMutator
from pipelines.aws.dynamodb import DynamoDBService
from pipelines.aws.s3 import S3Service
from pipelines.lang.filename_investigator import FilenameInvestigator

dynamodb_service = DynamoDBService()
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


def gather_and_write_telemetry(dataset, filename, external_landing_bucket_name, internal_landing_bucket_name, final_landing_bucket_name,
                               consumption_bucket_name, consumption_final_extension, telemetry_bucket_name, error=None):
    original_metadata = s3_service.metadata(external_landing_bucket_name, filename)

    telemetry = OrderedDict()

    telemetry['guid'] = filename_investigator.determine_simple_base_filename(filename)
    telemetry['filetype'] = original_metadata.content_type
    telemetry['table_name'] = '{}_{}'.format(dataset, filename_investigator.determine_root_directory(filename))
    telemetry['external_location'] = 's3://{}/{}'.format(external_landing_bucket_name, filename)
    telemetry['version'] = original_metadata.version_id
    telemetry['external_received'] = original_metadata.last_modified
    telemetry['external_filesize'] = original_metadata.content_length

    internal_metadata = s3_service.metadata(internal_landing_bucket_name, filename)
    telemetry['internal_location'] = 's3://{}/{}'.format(internal_landing_bucket_name, filename)
    telemetry['internal_received'] = internal_metadata.last_modified

    if error is None:
        telemetry['staging_location'] = 's3://{}/{}'.format(final_landing_bucket_name, filename)

        if consumption_final_extension is not None:
            stripped_filename = filename_investigator.determine_base_filename(filename)
            print(stripped_filename)
            consumption_filename = f'{stripped_filename}{consumption_final_extension}'
        else:
            consumption_filename = filename

        consumption_metadata = s3_service.metadata(consumption_bucket_name, consumption_filename)

        telemetry['consumption_location'] = 's3://{}/{}'.format(consumption_bucket_name, consumption_filename)
        telemetry['consumption_received'] = consumption_metadata.last_modified
        telemetry['consumption_filesize'] = consumption_metadata.content_length

        telemetry['successful_processed'] = 'true'
        telemetry['process_completion'] = consumption_metadata.last_modified
        telemetry['error_code'] = None
        telemetry['error_message'] = None
    else:
        telemetry['staging_location'] = None
        telemetry['consumption_location'] = None
        telemetry['consumption_received'] = None
        telemetry['consumption_filesize'] = None
        telemetry['successful_processed'] = 'false'
        telemetry['process_completion'] = None
        telemetry['error_code'] = error['code']
        telemetry['error_message'] = error['message']

    print(telemetry)

    json_mutator = JsonMutator(telemetry)
    csv = json_mutator.csv()

    telemetry_filename_response = __create_telemetry_filename(filename)
    telemetry_filename = telemetry_filename_response['filename']
    telemetry_filename = telemetry_filename.replace(".", "_")

    filename_to_write = telemetry_filename
    response = s3_service.write_file(telemetry_bucket_name, f'{filename_to_write}.csv', csv)

    return response


def __create_telemetry_filename(filename):
    return_filename = filename
    first_slash = filename.find('/')
    if first_slash > 0:
        table = filename[0:first_slash]
        last_slash = filename.rfind('/')
        base = filename[0:last_slash + 1]
        root_filename = filename[last_slash + 1:]
        return_filename = f'{base}{table}_{root_filename}'

        return {
            'filename': return_filename,
            'table': table
        }
    else:
        return {
            'filename': filename,
            'table': 'unknown'
        }
