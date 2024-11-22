import argparse
import asyncio
import concurrent.futures
import sqlite3
import time

from datetime import datetime
from requests import Session
from requests.adapters import HTTPAdapter
from typing import Optional
from urllib3.util import Retry

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
parser.add_argument('-p', '--pool_size', default=500)


async def download_date_range(db_name: str, format: str, start: datetime, end: datetime, batch_size: int, pool_size: int):
    create_replay_table(db_name)
    existing_replays = set(get_existing_replays(db_name))
    print(f"Found {len(existing_replays)} existing replays")
    
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=pool_size) as pool, Session() as session:
        retries = Retry(
            total=3,
            backoff_factor=0.1,
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))
        get_replay_tasks = []
        loop_start = last_print = time.time()
        print_delay = 10 # seconds
        num_replays = 0
        async for replay_ids in search_date_range(format, start, end, session):
            get_replay_tasks.extend([
                loop.run_in_executor(pool, download.get_replay, replay_id, session)
                for replay_id in replay_ids
                if replay_id not in existing_replays
            ])
            
            num_skipped = sum([int(replay_id in existing_replays) for replay_id in replay_ids])
            print(f"Skipping {num_skipped} downloaded replays")
            
            if len(get_replay_tasks) >= batch_size:
                print(f"Processing batch of {batch_size}")
                ready_replays = [
                    future.result()
                    async for future in asyncio.as_completed(get_replay_tasks[:batch_size])
                ]
                get_replay_tasks = get_replay_tasks[batch_size:]
                print(f"Persisting {len(ready_replays)} replays")
                num_replays += len(ready_replays)
                persist_replays(db_name, ready_replays)
            
            cur_time = time.time()
            if cur_time - last_print > print_delay:
                last_print = cur_time
                total_duration = cur_time - loop_start
                print(f"Processed {num_replays} replays in {total_duration:.2f}s")
                print(f"Estimated rate is {num_replays/total_duration:.2f} replays/second")
                print(f"Unprocessed replays: {len(get_replay_tasks)}")
        
        if get_replay_tasks:
            print(f"Processing final batch of {batch_size}")
            ready_replays = [
                future.result()
                async for future in asyncio.as_completed(get_replay_tasks)
            ]
            print(f"Persisting {len(ready_replays)} replays")
            num_replays += len(ready_replays)
            persist_replays(db_name, ready_replays)
        
        cur_time = time.time()
        total_duration = cur_time - loop_start
        print(f"Processed {num_replays} replays in {total_duration:.2f}s")
        print(f"Estimated rate is {num_replays/total_duration:.2f} replays/second")


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
        con.commit()
    finally:
        con.close()


def get_existing_replays(db_name: str, table_name: str = "replays"):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    try:
        ids = cur.execute(f"SELECT id FROM {table_name}")
        ids = cur.fetchall()
        ids = [x[0] for x in ids]  # Unpack the tuple
    finally:
        con.close()
    return ids


def persist_replays(db_name: str, replay_data: list[dict], table_name: str = "replays"):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    try:
        data = []
        for replay in replay_data:
            log = replay['log'].replace('"', '""')
            players = ",".join(replay['players'])
            rating = replay['rating'] or "null"
            data.append((
                replay['id'],
                replay['formatid'],
                players,
                log,
                replay['uploadtime'],
                rating,
            ))
        cmd = f"INSERT INTO {table_name} (id, format, players, log, uploadtime, rating) VALUES(?, ?, ?, ?, ?, ?)"
        cur.executemany(cmd, data)
        con.commit()
    finally:
        con.close()


async def search_date_range(format: str, start: datetime, end: datetime, session: Session):
    remaining_searches: list[datetime] = [end]
    replay_ids: list[str] = []
    yield_size = 51 * 10
    while remaining_searches:
        before = remaining_searches.pop(0)
        search_result = await search(before=before.timestamp(), format=format, session=session)
        replay_ids.extend([s['id'] for s in search_result])
        
        try:
            next_search_before = int(search_result[-1]['uploadtime'])
            next_search_before = datetime.fromtimestamp(next_search_before)
            if start <= next_search_before and next_search_before <= end:
                remaining_searches.append(next_search_before)
        except (KeyError, IndexError):
            print("No more searches to perform")
        
        if len(replay_ids) >= yield_size:
            yield replay_ids[:yield_size]
            replay_ids = replay_ids[yield_size:]
    
    if replay_ids:
        yield replay_ids


async def search(
    before: Optional[int] = None,
    format: Optional[str] = "gen9vgc2024regg",
    session: Optional[Session] = None,
):
    return download.search(before, format, session)


async def get_replay(replay_id: str):
    return download.get_replay(replay_id)


async def main(db_name: str, format: str, start: str, end: str, batch_size: int, pool_size: int):
    start = datetime.strptime(start, "%Y-%m-%d_%H:%M:%S")
    end = datetime.strptime(end, "%Y-%m-%d_%H:%M:%S")
    await download_date_range(db_name, format, start, end, batch_size, pool_size)


if __name__ == "__main__":
    args = parser.parse_args()
    print(f"run with args {args}")
    asyncio.run(
        main(
            args.database,
            args.format,
            args.start,
            args.end,
            int(args.batch_size),
            int(args.pool_size),
        )
    )
