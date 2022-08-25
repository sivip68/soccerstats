import os
from dotenv import load_dotenv
import boto3
from boto3.dynamodb.conditions import Attr

load_dotenv(".env.local")

dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.getenv("region_name"),
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)
tab_matchlinks = dynamodb.Table("dev_raw_matchlinks")

res = tab_matchlinks.scan(FilterExpression=Attr('_year_end').eq(2019) & Attr('_league').eq('Bundesliga'))
items = res['Items'][0]['links']
print(len(items))

