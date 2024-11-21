import argparse
import pandas as pd
import sqlite3
import time

from concurrent.futures import Future
from datetime import datetime, timedelta
from prefect import task, flow
from prefect.cache_policies import INPUTS, TASK_SOURCE
from prefect.concurrency.sync import rate_limit
from prefect.futures import as_completed
from prefect.tasks import task_input_hash
from typing import Optional

from pokemon_showdown_replay_tools import download

search = task(download.search, cache_policy=INPUTS + TASK_SOURCE)
get_replay = task(download.get_replay, cache_policy=INPUTS + TASK_SOURCE)

parser = argparse.ArgumentParser(
    prog='populate',
    description='Download replays from Pokemon Showdown and populate a database with them',
)

parser.add_argument('-n', '--database', help="SQLite database name")
parser.add_argument('-s', '--start', help="timestamp in format %%Y-%%m-%%d_%%H:%%M:%%S", default="2024-11-01_10:00:00")
parser.add_argument('-e', '--end', help="timestamp in format %%Y-%%m-%%d_%%H:%%M:%%S", default="2024-11-01_14:00:00")
parser.add_argument('-f', '--format', help="meta format", default="gen9vgc2024regh")


@flow(retries=3, retry_delay_seconds=1, log_prints=True)
def download_date_range(db_name: str, format: str, start: datetime, end: datetime):
    create_replay_table(db_name)
    
    remaining_searches: list[datetime] = [end]
    search_results_futures: list[Future] = []
    replays_to_download: list[dict] = []
    WARMUP_ITERATIONS = 10
    current_iteration = 0
    REPLAY_BATCH_SIZE = 50
    while True:
        current_iteration += 1
        
        if len(remaining_searches) == 0 and \
                len(search_results_futures) == 0 and \
                len(replays_to_download) == 0 and \
                current_iteration >= WARMUP_ITERATIONS:
            print("Completed all pending work")
            break
        else:
            if current_iteration % 10 == 9:
                print(f"{len(remaining_searches)} remaining_searches")
                print(f"{len(search_results_futures)} search_results_futures")
                print(f"{len(replays_to_download)} replays_to_download")
    
        time.sleep(1)
    
        # Check if there are any searches to submit
        if len(remaining_searches) > 0:
            before = remaining_searches.pop(0)
            search_results_futures.append(search.submit(before=before.timestamp(), format=format))
        
        # Once a search is done, submit replay ids for download
        if len(search_results_futures) > 0:
            remove_idx = None
            for i, sr_fut in enumerate(search_results_futures):
                if sr_fut.state.is_completed():
                    remove_idx = i
                    if sr_fut.state.is_cancelled(): break
                    search_result = sr_fut.result()
                    replays_to_download.extend(search_result)
                    try:
                        next_search_before = int(search_result[-1]['uploadtime'])
                        next_search_before = datetime.fromtimestamp(next_search_before)
                        if start <= next_search_before and next_search_before <= end:
                            remaining_searches.append(next_search_before)
                    except (KeyError, IndexError):
                        print("No more searches to perform")
                    break
            if remove_idx is not None:
                search_results_futures.pop(remove_idx)
        
        # Submit replay downloads
        replay_futures: list[Future] = []
        while len(replays_to_download) > 0:
            replay_ids = [r['id'] for r in replays_to_download[:REPLAY_BATCH_SIZE]]
            replays_to_download = replays_to_download[REPLAY_BATCH_SIZE:]
            replay_future = concurrency_limited_get_replay.map(replay_ids)
            replay_futures.append(replay_future)
        
        for replay_future in replay_futures:
            ready_replays = [
                future.result()
                for future in as_completed(replay_future)
                if not future.state.is_cancelled()
            ]
            print(f"Persisting {len(ready_replays)} replays")
            persist_replays(db_name, ready_replays)


@task(retries=3, retry_delay_seconds=1, log_prints=True)
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


@task(retries=3, retry_delay_seconds=1, log_prints=True)
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


@task(retries=3, retry_delay_seconds=1, cache_policy=INPUTS + TASK_SOURCE)
def concurrency_limited_get_replay(replay_id: str):
    rate_limit("pokemon-showdown-rate-limit")
    return download.get_replay(replay_id)


def main(db_name: str, format: str, start: str, end: str):
    start = datetime.strptime(start, "%Y-%m-%d_%H:%M:%S")
    end = datetime.strptime(end, "%Y-%m-%d_%H:%M:%S")
    download_date_range(db_name, format, start, end)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args.database, args.format, args.start, args.end)

