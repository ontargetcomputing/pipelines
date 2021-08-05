import boto3
import json


class StepFunctionsService:
    def __init__(self, region='us-east-1'):
        self.step_functions_client = boto3.client('stepfunctions', region_name=region)

    """
    Starts a step function with the given input
    """

    def start(self, arn, name, input):
        return self.step_functions_client.start_execution(
            stateMachineArn=arn,
            name=name,
            input=json.dumps(input)
        )
