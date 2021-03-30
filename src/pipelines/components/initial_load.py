import logging
import sys
sys.path.append("src")
from pipelines.aws.s3 import S3Service
from pipelines.json.json_interrogator import JsonInterrogator
from pipelines.aws.step_functions import StepFunctionsService

s3_service = S3Service()
json_interrogator = JsonInterrogator()
step_functions_service = StepFunctionsService()


def process_next_page(state_machine_arn, execution_name, input):
    response = step_functions_service.start(
        arn=state_machine_arn,
        name=execution_name,
        input=input
    )

    logging.debug(f'The response initiating the state machine is {response}')

    return True


def process_page(options):
    page_size = options["page_size"]
    process_json = options.get('process_json', False)
    source_bucket_name = options['source_bucket_name']
    destination_bucket_name = options['destination_bucket_name']
    starting_token = options['starting_token']
    total_files = options['total_files']

    logging.debug(f'{source_bucket_name} to {destination_bucket_name} with page size {page_size} and starting token {starting_token}')

    page = s3_service.list_objects_page(source_bucket_name, page_size, starting_token)
    total_files_this_page = 0
    for metadata in page['Contents']:
        starting_token = metadata['Key']
        total_files_this_page = total_files_this_page + 1
        print(starting_token)
        if process_json is True:
            if ".json" in starting_token:
                s3_service.copy_file(source_bucket_name, starting_token, destination_bucket_name, starting_token)
                print(f'Processing json file {starting_token}')
            else:
                print(f'Skipping non json file {starting_token}')
        else:
            if ".json" in starting_token:
                print(f'Skipping json file {starting_token}')
            else:
                s3_service.copy_file(source_bucket_name, starting_token, destination_bucket_name, starting_token)
                print(f'Processing non json file {starting_token}')

    if page['IsTruncated'] is True:
        return {
            'another_page': 'true',
            'total_files': total_files + total_files_this_page,
            'starting_token': starting_token
        }
    else:
        return {
            'another_page': 'false',
            'total_files': total_files + total_files_this_page,
            'starting_token': ''
        }
