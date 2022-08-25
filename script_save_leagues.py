import os
from dotenv import load_dotenv
import src.service.dynamodb as dynamodb
from config.basicdata import leagues


load_dotenv(".env.local")

db = dynamodb.create_service(
    os.getenv("aws_access_key_id"), 
    os.getenv("aws_secret_access_key"), 
    os.getenv("region_name")
)

tab = db.create_table("dev_league", "id", "N", "name", "S")
for item in leagues:
    db.persist("dev_league", item)
print("created dev_league")
