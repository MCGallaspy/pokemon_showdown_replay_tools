"""
Get your Pokemon Showdown public replay report card.
This downloads and analyzes your public replays based on your username.
"""
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

from pokemon_showdown_replay_tools import download


st.header("Public Replay Report Card")

username = st.text_input("Username", value=st.session_state.get("report_username", "zqrubl"))

st.markdown("## Filters")
filters_columns = st.columns(3)

with filters_columns[0]:
    start, end = datetime.strptime("2024/11/01", "%Y/%m/%d"), datetime.fromtimestamp(time.time())
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
    rating_filter_start, rating_filter_end = st.slider(
        "Rating range",
        value=(1000, 2000),
        min_value=1000,
        max_value=2000,
    )

with filters_columns[2]:
    include_unrated = st.toggle("Include unrated games", value=False)


@st.cache_data(ttl="1h")
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
    replay_ids: list[str] = []
    search_results: list[dict] = []
    while remaining_searches:
        before = remaining_searches.pop(0)
        before=int(before.timestamp())
        search_result = cached_search(before=before, username=username)
        search_results.append(search_result)
        replay_ids.extend([s['id'] for s in search_result])
        
        try:
            next_search_before = int(search_result[-1]['uploadtime'])
            next_search_before = datetime.fromtimestamp(next_search_before)
            if start <= next_search_before and next_search_before <= end:
                remaining_searches.append(next_search_before)
        except (KeyError, IndexError):
            print("No more searches to perform")
    
    return [replay for replay_list in search_results for replay in replay_list]

if st.button("Load data"):
    with st.spinner("Compulating..."):
        if not username:
            st.warning("Enter a username, dingus!")
            st.stop()

        search_results = search_date_range(username, filter_start, filter_end)
        if not search_results:
            st.warning("No data found")
            st.stop()
        df = pd.DataFrame(data=search_results)
        df['uploadtime'] = df['uploadtime'].apply(datetime.fromtimestamp)
        df = df.sort_values(by='uploadtime')
        df['game_num'] = None
        for formatid, grouped in df.groupby(by='format'):
            N = grouped.shape[0]
            df.loc[grouped.index, "game_num"] = list(range(N))
        st.session_state['search_df'] = df
        st.session_state['report_username'] = username
        st.rerun()

search_df = st.session_state.get('search_df', None)
if search_df is not None:
    st.header("Loaded Data")
    st.dataframe(search_df)
