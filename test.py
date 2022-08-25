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
table = dynamodb.Table("dev_seasons")
res = table.scan()
print(res)

for y in range(2016, 2023):
    item = {"id": (y - 2013), "year_start": y, "year_end": (y + 1)}
    print(item)
    table.put_item(Item=item)
