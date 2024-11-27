import json
import requests

from datetime import datetime
from requests import Session
from time import localtime, mktime
from typing import Optional


def search(
    before: Optional[int] = None,
    format: Optional[str] = "gen9vgc2024regg",
    username: Optional[str] = None,
    session: Optional[Session] = None,
):
    session = session or requests
    params = {}
    if before is not None:
        params.update({"before": before})
    if format is not None:
        params.update({"format": format})
    if username is not None:
        params.update({"user": username})
    resp = session.get(
        "https://replay.pokemonshowdown.com/search.json",
        params=params,
        timeout=2,
    )
    return json.loads(resp.content)


def search_date_range(
    start: datetime = datetime.strptime("2024-11-01 10:00:00", "%Y-%m-%d %H:%M:%S"),
    end: datetime = datetime.strptime("2024-11-01 14:00:00", "%Y-%m-%d %H:%M:%S"),
    format: Optional[str] = "gen9vgc2024regg",
    session: Optional[Session] = None,
):
    session = session or requests
    results = []
    before = end
    while before >= start:
        search_results = search(before=before.timestamp(), format=format, session=session)
        next_before = int(search_results[-1]['uploadtime'])
        next_before = datetime.fromtimestamp(next_before)
        if next_before == before:
            break
        before = next_before
        results.extend(search_results)
    return results


def get_replay(replay_id: str, session: Optional[Session] = None):
    session = session or requests
    url = f"https://replay.pokemonshowdown.com/{replay_id}.json"
    resp = session.get(url, timeout=2)
    try:
        result = json.loads(resp.content)
    except json.decoder.JSONDecodeError as e:
        raise Exception(f"Error with {url}") from e
    return result