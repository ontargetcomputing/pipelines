import boto3


class DynamoDBService:
    def __init__(self, region='us-east-1'):
        self.client = boto3.client('dynamodb', region_name=region)

    def update_item(self, dynamo_table_name, dataset, table, last_updated_time):
        last_updated_time_s = last_updated_time.strftime("%m/%d/%Y %H:%M:%S")

        self.client.update_item(
            TableName=dynamo_table_name,
            Key={
                'dataset': {"S": dataset},
                'table': {"S": table}
            },
            UpdateExpression="SET last_updated_time=:u",
            ExpressionAttributeValues={
                ':u': {"S": last_updated_time_s}
            },
            ReturnValues="UPDATED_NEW"
        )
        return ""
