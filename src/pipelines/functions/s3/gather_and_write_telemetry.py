from collections import OrderedDict
import sys
sys.path.append("src")
from pipelines.format.json.json_mutator import JsonMutator
from pipelines.integration.aws.s3 import S3Service
from pipelines.lang.filename_investigator import FilenameInvestigator

s3_service = S3Service()
filename_investigator = FilenameInvestigator()

# TODO - clean this up, make it more reusable or move into agrian-pipelines


def run(dataset, filename, external_landing_bucket_name, internal_landing_bucket_name, final_landing_bucket_name,
        consumption_bucket_name, consumption_bucket_folder, consumption_final_extension, telemetry_bucket_name, telemetry_bucket_folder, error=None):
    original_metadata = s3_service.metadata(external_landing_bucket_name, filename)

    telemetry = OrderedDict()
    try:
        telemetry['guid'] = filename_investigator.determine_simple_base_filename(filename)
    except Exception:
        telemetry['guid'] = filename
    telemetry['filetype'] = original_metadata.content_type
    telemetry['dataset'] = dataset
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

        print('the filename=' + f'{consumption_bucket_folder}{consumption_filename}')
        consumption_metadata = s3_service.metadata(consumption_bucket_name, f'{consumption_bucket_folder}{consumption_filename}')

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
    response = s3_service.write_file(telemetry_bucket_name, f'{telemetry_bucket_folder}{filename_to_write}.csv', csv)

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
