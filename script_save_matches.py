import os
from dotenv import load_dotenv
import ScraperFC as sfc
import traceback
import src.service.dynamodb as dynamodb
import src.service.scraper as scraper
from decimal import Decimal
import json
from config.basicdata import leagues
from config.basicdata import seasons


load_dotenv(".env.local")

db = dynamodb.create_service(
    os.getenv("aws_access_key_id"), 
    os.getenv("aws_secret_access_key"), 
    os.getenv("region_name")
)

#tab_match = db.create_table("dev_scraper_match", "match_id", "N", "date", "S")

tab_mids = db.get_resource().Table("dev_scraper_matchids")

oscraper = sfc.Understat()

for league in leagues:
    if league["id"] != 1:
        continue
    for season in seasons:
        if int(season["year_start"]) < 2017:
            continue
        res = tab_mids.get_item(Key={"season_id":season["id"], "league_id":league["id"]})
        match_ids = res["Item"]["match_ids"]
        cnt = 1
        for mid in match_ids:
            match = scraper.fetch_match(oscraper, mid)
            match = scraper.transform_type(match)
            match = json.loads(json.dumps(match), parse_float=Decimal)
            item = {"match_id": int(match["match id"]), "date": match["date"], "match": match}
            db.persist("dev_scraper_match", item)
            print(season["year_start"], cnt, len(match_ids))
            cnt = cnt + 1
oscraper.close()
