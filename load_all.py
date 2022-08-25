import os
from dotenv import load_dotenv
import ScraperFC as sfc
import traceback
import src.service.dynamodb as dynamodb
from config.basicdata import leagues
from config.basicdata import seasons


def fetch_match_ids(season, league):
    print(f"fetch {season['year_start'], league['name']}")
    out = scraper.get_match_links(int(season['year_end']), league['name'])
    link_list = list(out)
    mid_list = []
    for item in link_list:
        if item == None:
            continue
        l = item.split("/")
        mid_list.append(l[-1])
    data = {
        "league_id": league["id"],
        "season_id": season["id"],
        "match_ids": mid_list,
        "count": len(mid_list),
    }
    return data


load_dotenv(".env.local")

db = dynamodb.create_service(
    os.getenv("aws_access_key_id"), 
    os.getenv("aws_secret_access_key"), 
    os.getenv("region_name")
)

# repeat 4, 5
#tab = db.create_table("dev_raw_matchids", "league_id", "N", "season_id", "N")
scraper = sfc.Understat()
#for season in seasons:
season = seasons[5]
league = leagues[4]
#for league in leagues:
data = fetch_match_ids(season, league)        
db.persist("dev_raw_matchids", data)
scraper.close()
print("created dev_raw_matchids")
