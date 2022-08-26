import os
from dotenv import load_dotenv
import ScraperFC as sfc
import traceback
from datetime import date
import src.service.dynamodb as dynamodb
import src.service.scraper as scraper
from config.basicdata import leagues
from config.basicdata import seasons


load_dotenv(".env.local")

db = dynamodb.create_service(
    os.getenv("aws_access_key_id"), 
    os.getenv("aws_secret_access_key"), 
    os.getenv("region_name")
)

tab_ids = db.create_table("dev_scraper_matchids", "league_id", "N", "season_id", "N")

o_scraper = sfc.Understat()
for season in seasons:
#season = seasons[5]
#league = leagues[4]
    for league in leagues:
        data = scraper.fetch_match_ids(o_scraper, season, league)        
        db.persist("dev_scraper_matchids", data)
        print(f"saved: league = {league['name']}, season = {season['year_start']}")
o_scraper.close()
print("created dev_raw_matchids")
