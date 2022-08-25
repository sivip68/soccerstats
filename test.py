import os
from dotenv import load_dotenv
import boto3
import ScraperFC as sfc
import traceback


load_dotenv(".env.local")
print(os.getenv("region_name"))
dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.getenv("region_name"),
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)


tab_seasons = dynamodb.Table("dev_seasons")
seasondata = []
res = tab_seasons.scan()
for item in res["Items"]:
    seasondata.append(item)
print(seasondata)

tab_leagues = dynamodb.Table("dev_leagues")
leaguedata = []
res = tab_leagues.scan()
for item in res["Items"]:
    leaguedata.append(item)
print(leaguedata)

scraper = sfc.Understat()
tab_matchlinks = dynamodb.Table("dev_raw_matchlinks")
for season in seasondata:
    for league in leaguedata:
        print(f"fetch {season['year_start'], league['name']}")
