import numpy
import datetime


def fetch_match_ids(scraper, season, league):
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
        "modified": datetime.date.today().strftime("%Y-%m-%d")
    }
    return data

def fetch_match(scraper, match_id):
    url = "https://understat.com/match/" + str(match_id)
    res = scraper.scrape_match(url)
    return res

def transform_type(obj):
    if (type(obj) is str) or (type(obj) is float) or (type(obj) is int):
        return obj
    if type(obj) is datetime.date:
        return obj.strftime("%Y-%m-%d")
    if type(obj) is numpy.float64:
        return float(obj)
    if type(obj) is numpy.int64:
        return int(obj)
    new_obj = dict(obj)
    for k in dict.keys(new_obj):
        new_obj[k] = transform_type(new_obj[k])
    return new_obj