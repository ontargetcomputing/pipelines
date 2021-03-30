import boto3
import json


class StepFunctionsService:
    def __init__(self):
        self.step_functions_client = boto3.client('stepfunctions')

    def start(self, arn, name, input):
        return self.step_functions_client.start_execution(
            stateMachineArn=arn,
            name=name,
            input=json.dumps(input)
        )
