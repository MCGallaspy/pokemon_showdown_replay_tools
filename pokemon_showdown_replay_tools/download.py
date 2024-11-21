import json
import requests

from typing import Optional


def search(before: Optional[int] = None, format: Optional[str] = "gen9vgc2024regg"):
    params = {}
    if before is not None:
        params.update({"before": before})
    if format is not None:
        params.update({"format": format})
    resp = requests.get(
        "https://replay.pokemonshowdown.com/search.json",
        params=params,
    )
    return json.loads(resp.content)

def get_replay(replay_id: str):
    url = f"https://replay.pokemonshowdown.com/{replay_id}.json"
    resp = requests.get(url)
    return json.loads(resp.content)