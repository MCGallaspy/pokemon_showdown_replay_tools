"""
Functions to set up a sqlite database for replay analysis.
Assumes you have a table defined by the SQLite statement below.

This will be the case if you run populate_asyncio.py in this
repository's scripts directory.

The database may be non-destructively modified by these operations
by adding new tables.

The replays table definition:

    CREATE TABLE replays (
        id TEXT PRIMARY KEY,
        format TEXT NOT NULL,
        players TEXT NOT NULL,
        log TEXT NOT NULL,
        uploadtime INTEGER NOT NULL,
        rating INTEGER)

"""
import sqlite3
import pandas as pd

from typing import Optional

from pokemon_showdown_replay_tools.analysis import parse_replay


def create_appearances_table(
    database_con: sqlite3.Connection,
    appearances_table_name: str = "appearances",
    replay_table_name: str = "replays",
):
    """
    Creates a table corresponding to pokemon appearances in battles.
    Each row in the appearances table corresponds to a pokemon that
    appeared in a battle, per player. If a pokemon appeared on both
    teams in a battle (a mirror match) then it will correspond to two
    distinct rows. This only counts pokemon that were seen in battle,
    not pokemon that appeared in the team preview or only stayed in
    back. Thus, if a lead pair wins then the other two pokemon on that
    side will be unknown.
    
    The table is defined by the following SQLite statement:
    
        CREATE TABLE appearances (
        id TEXT NOT NULL,
        player TEXT NOT NULL,
        pokemon TEXT NOT NULL,
        won INTEGER NOT NULL,
        FOREIGN KEY(id) REFERENCES replays(id)
        CONSTRAINT one_poke_per_player_per_game UNIQUE(id, player, pokemon) ON CONFLICT IGNORE)
    
    """
    cur = database_con.cursor()
    cur.execute(f"""
        CREATE TABLE {appearances_table_name} (
        id TEXT NOT NULL,
        player TEXT NOT NULL,
        pokemon TEXT NOT NULL,
        won INTEGER NOT NULL,
        FOREIGN KEY(id) REFERENCES {replay_table_name}(id)
        CONSTRAINT one_poke_per_player_per_game UNIQUE(id, player, pokemon) ON CONFLICT IGNORE)
    """)
    batch_cur = database_con.cursor()
    batch_cur.execute("SELECT id, log FROM replays")
    BATCH_SIZE = 10_000
    batch = batch_cur.fetchmany(BATCH_SIZE)
    while batch:
        parsed_replays = [parse_replay(replay) for (replay_id, replay) in batch]
        ids = [replay_id for (replay_id, replay) in batch]
        data = []
        for replay_id, parsed_replay in zip(ids, parsed_replays):
            # parsed_replay is a nested dictionary of info about the replay
            # For details see parse_replay in analysis.py
            for pokemon_appearance in parsed_replay['pokemon']:
                data.append([
                    replay_id,
                    pokemon_appearance['player'],
                    pokemon_appearance['name'],
                    1 if pokemon_appearance['player'] == parsed_replay['winner'] else 0,
                ])
        if data:
            cur.executemany(f"INSERT INTO {appearances_table_name} VALUES(?, ?, ?, ?)", data)
        batch = batch_cur.fetchmany(BATCH_SIZE)
    database_con.commit()


def get_pair_marginal_win_rates(
    database_con: sqlite3.Connection,
    appearances_table_name: str = "appearances",
):
    """
    Computes the marginal probability of pairs of pokemon winning.
    A pair is considered to have won if it appeared at least once in a battle
    (although not necessarily with both pokemon on the board at the same time)
    and their team ended up winning.
    """
    cur = database_con.cursor()
    cur.execute("""
    WITH marginal AS (
        WITH pairs AS (
            WITH player_appearances AS (
                SELECT a1.pokemon as p1, a2.pokemon as p2, a1.player, a1.won as won
                FROM appearances as a1, appearances as a2
                WHERE a1.id = a2.id AND a1.player = a2.player AND p1 < p2
            ) SELECT p1, p2, player, COUNT(*) as appearances, SUM(won) as won
              FROM player_appearances
              GROUP BY p1, p2, player
        ) SELECT p1, p2, COUNT(*) as players, SUM(appearances) as appearances, SUM(won) as wins
          FROM pairs
          GROUP BY p1, p2
    ) SELECT *, 1.0 * wins / appearances FROM marginal
      ORDER BY appearances DESC
    """)
    return cur.fetchall()


def get_pair_marginal_win_rates_conditional(
    database_con: sqlite3.Connection,
    where: str = '',
    appearances_table_name: str = "appearances",
    replay_table_name: str = "replays",
    explain: bool = False,
):
    """
    Computes marginal win rate, but you can optionally you can specify
    a WHERE clause on the replays table to filter the replays considered.
    """
    cur = database_con.cursor()
    explain = "EXPLAIN QUERY PLAN" if explain else ""
    cur.execute(f"""
    {explain} WITH marginal AS (
        WITH pairs AS (
            WITH player_appearances AS (
                WITH filtered_appearances AS (
                    SELECT * FROM {appearances_table_name}
                    WHERE id IN (SELECT id FROM {replay_table_name} {where})
                )
                SELECT a1.pokemon as p1, a2.pokemon as p2, a1.player, a1.won as won
                FROM filtered_appearances as a1, filtered_appearances as a2
                WHERE a1.id = a2.id AND a1.player = a2.player AND p1 < p2
            ) SELECT p1, p2, player, COUNT(*) as appearances, SUM(won) as won
              FROM player_appearances
              GROUP BY p1, p2, player
        ) SELECT p1, p2, COUNT(*) as players, SUM(appearances) as appearances, SUM(won) as wins
          FROM pairs
          GROUP BY p1, p2
    ) SELECT *, 1.0 * wins / appearances FROM marginal
      ORDER BY appearances DESC
    """)
    return cur.fetchall()