import pytest
import sys
from moto import mock_stepfunctions
sys.path.append("src")
from pipelines.integration.aws.step_functions import StepFunctionsService


@mock_stepfunctions
@pytest.mark.aws
class TestStepFunctiwons:
    def test_start(self, sf_client, iam_admin_role_arn):
        # SETUP
        state_machine = sf_client.create_state_machine(
            name='TheStateMachine',
            definition='string',
            roleArn=iam_admin_role_arn,
            type='STANDARD'
        )

        # EXECUTE
        sf_service = StepFunctionsService()
        response = sf_service.start(state_machine['stateMachineArn'], 'name', {'foo': 'bar'})

        # ASSERT
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
