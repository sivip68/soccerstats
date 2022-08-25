import os
from dotenv import load_dotenv
import src.service.dynamodb as dynamodb
from config.basicdata import seasons


load_dotenv(".env.local")

db = dynamodb.create_service(
    os.getenv("aws_access_key_id"), 
    os.getenv("aws_secret_access_key"), 
    os.getenv("region_name")
)

tab = db.create_table("dev_season", "id", "N", "year_start", "N")
for item in seasons:
    db.persist("dev_season", item)
print("created dev_season")
