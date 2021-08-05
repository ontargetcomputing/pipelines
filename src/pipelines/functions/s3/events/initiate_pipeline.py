import logging
import sys
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.integration.aws.step_functions import StepFunctionsService

s3_service = S3Service()
step_functions_service = StepFunctionsService()


def run(s3_event, state_machine_options):
    filename = s3_event['Records'][0]['s3']['object']['key']
    bucket_name = s3_event['Records'][0]['s3']['bucket']['name']
    logging.debug(f'Processing {bucket_name}/{filename}')

    state_machine_input = {
        'location': {
            'bucket_name': bucket_name,
            'key': filename
        }
    }
    response = step_functions_service.start(
        arn=state_machine_options['state_machine_arn'],
        name=state_machine_options['execution_name'],
        input=state_machine_input
    )

    logging.debug(f'The response initiating the state machine is {response}')

    return response
