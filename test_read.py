import os
from dotenv import load_dotenv
import boto3

load_dotenv(".env.local")
print(os.getenv("region_name"))
dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.getenv("region_name"),
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)
tab_matchlinks = dynamodb.Table("dev_raw_matchlinks")

res = tab_matchlinks.get_item(Key={'_league':'Bundesliga', '_year_start': 2022})
print(res)
for item in res["Item"]['links']:
    print(item)
