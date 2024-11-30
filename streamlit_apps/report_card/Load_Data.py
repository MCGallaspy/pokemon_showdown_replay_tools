"""
Get your Pokemon Showdown public replay report card.
This downloads and analyzes your public replays based on your username.
"""
import numpy as np
import pandas as pd
import requests
import seaborn as sns
import streamlit as st
import time

from datetime import datetime
from matplotlib import pyplot as plt
from requests import Session
from requests.adapters import HTTPAdapter
from typing import Optional
from urllib3.util import Retry

from pokemon_showdown_replay_tools.analysis import parse_replay
from pokemon_showdown_replay_tools import download


st.header("Public Replay Report Card")

username = st.text_input("Username", value=st.session_state.get("report_username", "zqrubl"))

st.markdown("## Filters")
filters_columns = st.columns(3)

with filters_columns[0]:
    end = datetime.fromtimestamp(time.time())
    start = datetime.fromtimestamp(time.time() - 60 * 60 * 24)
    date_filters = st.date_input(
        "Upload date range",
        value=(start, end),
        min_value=start,
        max_value=end,
        format="YYYY/MM/DD"
    )
    if len(date_filters) == 0:
        filter_start, filter_end = start, end
    if len(date_filters) == 1:
        filter_start, filter_end = date_filters[0], end
    if len(date_filters) == 2:
        filter_start, filter_end = date_filters
    filter_start, filter_end = [
        datetime.combine(d, datetime.min.time())
        for d in (filter_start, filter_end)
    ]

with filters_columns[1]:
    rating_filter_start = st.number_input(
        "Rating min",
        1000,
    )
    rating_filter_end = st.number_input(
        "Rating max",
        9000,
    )

with filters_columns[2]:
    include_unrated = st.toggle("Include unrated games", value=False)


@st.cache_data(ttl="1s")
def cached_search(before, username):
    with Session() as session:
        retries = Retry(
            total=3,
            backoff_factor=0.1,
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return download.search(before=before, format=None, username=username, session=session)


def search_date_range(username: str, start: datetime, end: datetime):
    remaining_searches: list[datetime] = [end]
    search_results: list[dict] = []
    while remaining_searches:
        before = remaining_searches.pop(0)
        before=int(before.timestamp())
        search_result = cached_search(before=before, username=username)
        search_results.append(search_result)
        
        try:
            next_search_before = int(search_result[-1]['uploadtime'])
            next_search_before = datetime.fromtimestamp(next_search_before)
            if start <= next_search_before and next_search_before <= end:
                remaining_searches.append(next_search_before)
        except (KeyError, IndexError):
            print("No more searches to perform")
    
    search_results = [sr for sr_list in search_results for sr in sr_list]
    search_results = [
        s for s in search_results 
        if (include_unrated and (s['rating'] is None)) or
        (
            (s['rating'] is not None) and
            (rating_filter_start <= s['rating'] <= rating_filter_end)
        )
    ]
    
    return search_results


if st.button("Load data"):
    with st.spinner("Compulating..."):
        if not username:
            st.warning("Enter a username, dingus!")
            st.stop()

        search_results = search_date_range(username, filter_start, filter_end)
        if not search_results:
            st.warning("No data found")
            st.stop()
        search_df = pd.DataFrame(data=search_results)
        search_df['uploadtime'] = search_df['uploadtime'].apply(datetime.fromtimestamp)
        search_df = search_df.sort_values(by='uploadtime')
        del search_df['private']
        del search_df['password']
        search_df['replay_link'] = search_df.id.apply(lambda x: f"https://replay.pokemonshowdown.com/{x}")
        st.session_state['search_df'] = search_df
        st.session_state['report_username'] = username       
        if 'replays_df' in st.session_state:
            del st.session_state['replays_df']
        if 'appearances_df' in st.session_state:
            del st.session_state['appearances_df']
        st.rerun()


@st.cache_data
def get_replay_cached(replay_id: str):
    try:
        with Session() as session:
            retries = Retry(
                total=3,
                backoff_factor=0.1,
            )
            session.mount('https://', HTTPAdapter(max_retries=retries))
            return download.get_replay(replay_id, session)
    except:
        return {"id": replay_id, "log": "error"}


search_df = st.session_state.get('search_df', None)
if search_df is not None:

    if 'appearances_df' not in st.session_state or 'replays_df' not in st.session_state:
        pbar = st.progress(0, "Downloading replays")
        count = 0
        N = search_df.shape[0]
        def get_replay_with_progress(replay_id: str):
            result = get_replay_cached(replay_id)
            global count
            count += 1
            pbar.progress(count / N, "Downloading replays")
            return result

        replays_df = search_df.apply(
            lambda row: get_replay_with_progress(row.id),
            axis=1,
            result_type="expand",
        )
        
        pbar = st.progress(0 ,"Parsing replays")
        count = 0
        N = search_df.shape[0]
        def parse_replay_with_progress(replay_id: str):
            result = parse_replay(replay_id)
            global count
            count += 1
            pbar.progress(count / N, "Parsing replays")
            return result
        
        replays_df['parse_results'] = replays_df.log.apply(parse_replay_with_progress)
        replays_df['players'] = replays_df['players'].apply(','.join)
        del replays_df['log']
        st.session_state['replays_df'] = replays_df
        
        data = []
        report_username = st.session_state['report_username'].lower()
        pbar = st.progress(0, "Analyzing appearances")
        count = 0
        N = replays_df.shape[0]
        for row in replays_df.itertuples():
            parsed_replay = row.parse_results
            # parsed_replay is a nested dictionary of info about the replay
            # For details see parse_replay in analysis.py
            winner_name = parsed_replay['winner'].lower()
            for pokemon_appearance in parsed_replay['pokemon']:
                player_name = pokemon_appearance['player'].lower()
                if player_name == report_username:
                    data.append([
                        row.id,
                        pokemon_appearance['player'],
                        pokemon_appearance['name'],
                        1 if winner_name == player_name else 0,
                    ])
            count += 1
            pbar.progress(count / N, "Analyzing appearances")
        appearances_df = pd.DataFrame(data=data, columns=['id', 'player', 'pokemon', 'won'])
        appearances_df = appearances_df.drop_duplicates(keep='first')
        st.session_state['appearances_df'] = appearances_df
        
    
    st.header("Additional Filters")
    
    meta_formats = list(search_df.format.value_counts().index)
    meta_format_filter = st.multiselect(
        "Filter by meta format",
        meta_formats,
        meta_formats,
    )
    
    selection_mask = np.ones(search_df.shape[0], dtype='bool')
    
    if meta_format_filter:
        selection_mask &= search_df.format.isin(meta_format_filter)

    appearances_df = st.session_state['appearances_df']
    all_mons = sorted(appearances_df.pokemon.unique())
    seen_pokemon_picker = st.columns([0.8, 0.2])
    with seen_pokemon_picker[0]:
        seen_pokemon_filter = st.multiselect(
            "Filter by pokémon seen",
            all_mons,
        )
    with seen_pokemon_picker[1]:
        seen_pokemon_mode = st.radio(
            "Mode", ["any", "all"], index=1, key="seen_pokemon_mode")
    if seen_pokemon_filter:
        filtered_appearances_mask = appearances_df.pokemon.isin(seen_pokemon_filter)
        ids = []
        for replay_id, group_df in appearances_df[filtered_appearances_mask].groupby(by='id'):
            if seen_pokemon_mode == "all":
                game_pokemon = set(group_df.pokemon.unique())
                if all(p in game_pokemon for p in seen_pokemon_filter):
                    ids.append(replay_id)
            else:
                ids.append(replay_id)
        selection_mask &= search_df.id.isin(ids)

    won_pokemon_picker = st.columns([0.8, 0.2])
    with won_pokemon_picker[0]:
        won_pokemon_filter = st.multiselect(
            "Filter by pokémon that won",
            all_mons,
        )
    with won_pokemon_picker[1]:
        won_pokemon_mode = st.radio(
            "Mode", ["any", "all"], index=1, key="won_pokemon_mode")
    if won_pokemon_filter:
        filtered_appearances_mask = appearances_df.pokemon.isin(won_pokemon_filter)
        filtered_appearances_mask &= appearances_df.won.astype(bool)
        ids = []
        for replay_id, group_df in appearances_df[filtered_appearances_mask].groupby(by='id'):
            if won_pokemon_mode == "all":
                game_pokemon = set(group_df.pokemon.unique())
                if all(p in game_pokemon for p in won_pokemon_filter):
                    ids.append(replay_id)
            else:
                ids.append(replay_id)
        selection_mask &= search_df.id.isin(ids)

    lost_pokemon_picker = st.columns([0.8, 0.2])
    with lost_pokemon_picker[0]:
        lost_pokemon_filter = st.multiselect(
            "Filter by pokémon that lost",
            all_mons,
        )
    with lost_pokemon_picker[1]:
        lost_pokemon_mode = st.radio(
            "Mode", ["any", "all"], index=1, key="lost_pokemon_mode")
    if lost_pokemon_filter:
        filtered_appearances_mask = appearances_df.pokemon.isin(lost_pokemon_filter)
        filtered_appearances_mask &= ~appearances_df.won.astype(bool)
        ids = []
        for replay_id, group_df in appearances_df[filtered_appearances_mask].groupby(by='id'):
            if lost_pokemon_mode == "all":
                game_pokemon = set(group_df.pokemon.unique())
                if all(p in game_pokemon for p in lost_pokemon_filter):
                    ids.append(replay_id)
            else:
                ids.append(replay_id)
        selection_mask &= search_df.id.isin(ids)

    col_config = {
        "replay_link": st.column_config.LinkColumn(
            display_text="Go to replay"),
    }
    st.header("Loaded Data")
    st.dataframe(
        search_df[selection_mask],
        column_config=col_config,
    )
