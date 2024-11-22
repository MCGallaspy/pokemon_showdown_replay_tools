import argparse
import asyncio
import sqlite3

from datetime import datetime
from typing import Optional

from pokemon_showdown_replay_tools import download


parser = argparse.ArgumentParser(
    prog='populate',
    description='Download replays from Pokemon Showdown and populate a database with them',
)


parser.add_argument('-n', '--database', help="SQLite database name")
parser.add_argument('-s', '--start', help="timestamp in format %%Y-%%m-%%d_%%H:%%M:%%S", default="2024-11-01_10:00:00")
parser.add_argument('-e', '--end', help="timestamp in format %%Y-%%m-%%d_%%H:%%M:%%S", default="2024-11-01_14:00:00")
parser.add_argument('-f', '--format', help="meta format", default="gen9vgc2024regh")
parser.add_argument('-b', '--batch_size', default=51)


async def download_date_range(db_name: str, format: str, start: datetime, end: datetime, batch_size: int):
    create_replay_table(db_name)
    get_replay_tasks = []
    async for replay_ids in search_date_range(format, start, end):
        get_replay_tasks.extend([
            asyncio.create_task(get_replay(replay_id), name=replay_id)
            for replay_id in replay_ids
        ])
        if len(get_replay_tasks) >= batch_size:
            ready_replays = [
                future.result()
                async for future in asyncio.as_completed(get_replay_tasks[:batch_size])
            ]
            get_replay_tasks = get_replay_tasks[batch_size:]
            print(f"Persisting {len(ready_replays)} replays")
            persist_replays(db_name, ready_replays)
    
    if get_replay_tasks:
        ready_replays = [
            future.result()
            for future in asyncio.as_completed(get_replay_tasks)
        ]
        print(f"Persisting {len(ready_replays)} replays")
        persist_replays(db_name, ready_replays)


def create_replay_table(db_name: str, table_name: str = "replays"):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    try:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                        id TEXT PRIMARY KEY ON CONFLICT IGNORE,
                        format TEXT NOT NULL,
                        players TEXT NOT NULL,
                        log TEXT NOT NULL,
                        uploadtime INTEGER NOT NULL,
                        rating INTEGER)""")
    finally:
        con.commit()
        con.close()


def persist_replays(db_name: str, replay_data: list[dict], table_name: str = "replays"):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    try:
        for replay in replay_data:
            log = replay['log'].replace('"', '""')
            players = ",".join(replay['players'])
            rating = replay['rating'] or "null"
            cmd = f"""INSERT INTO {table_name}
            (id, format, players, log, uploadtime, rating)
            VALUES ("{replay['id']}", "{replay['formatid']}", "{players}",
                    "{log}", {replay['uploadtime']}, {rating})
            """            
            cur.execute(cmd)
    finally:
        con.commit()
        con.close()


async def search_date_range(format: str, start: datetime, end: datetime):
    remaining_searches: list[datetime] = [end]
    while remaining_searches:
        before = remaining_searches.pop(0)
        search_result = await search(before=before.timestamp(), format=format)
        
        try:
            next_search_before = int(search_result[-1]['uploadtime'])
            next_search_before = datetime.fromtimestamp(next_search_before)
            if start <= next_search_before and next_search_before <= end:
                remaining_searches.append(next_search_before)
        except (KeyError, IndexError):
            print("No more searches to perform")
        
        yield [s['id'] for s in search_result]


async def search(before: Optional[int] = None, format: Optional[str] = "gen9vgc2024regg"):
    return download.search(before, format)


async def get_replay(replay_id: str):
    return download.get_replay(replay_id)


async def main(db_name: str, format: str, start: str, end: str, batch_size: int):
    start = datetime.strptime(start, "%Y-%m-%d_%H:%M:%S")
    end = datetime.strptime(end, "%Y-%m-%d_%H:%M:%S")
    await download_date_range(db_name, format, start, end, batch_size)


if __name__ == "__main__":
    args = parser.parse_args()
    asyncio.run(main(args.database, args.format, args.start, args.end, int(args.batch_size)))
