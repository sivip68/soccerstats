import boto3


class DynamoDB:
    def __init__(self, resource):
        self.resource = resource

    def create_table(
        self, 
        table_name, 
        first_key, 
        first_type, 
        second_key, 
        second_type
    ):
        params = {
            'TableName': table_name,
            'KeySchema': [
                {'AttributeName': first_key, 'KeyType': 'HASH'},
                {'AttributeName': second_key, 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': first_key, 'AttributeType': first_type},
                {'AttributeName': second_key, 'AttributeType': second_type}
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        }
        table = self.resource.create_table(**params)
        table.wait_until_exists()
        return table

    def persist(self, table_name, item):
        return self.resource.Table(table_name).put_item(Item=item)
    

def create_service(key, secret, region):
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )
    return DynamoDB(dynamodb)