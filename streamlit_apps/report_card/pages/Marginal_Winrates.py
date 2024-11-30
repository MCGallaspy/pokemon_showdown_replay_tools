import pandas as pd
import seaborn as sns
import streamlit as st
import sqlite3

from datetime import datetime
from matplotlib import pyplot as plt
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from pokemon_showdown_replay_tools import download
from pokemon_showdown_replay_tools.analysis import parse_replay
from pokemon_showdown_replay_tools.sqlite import get_pair_marginal_win_rates_conditional


sns.set_style('darkgrid')
st.header("Marginal Win Rates")

search_df = st.session_state.get('search_df', None)
if search_df is None:
    st.warning("Load some data first")
    st.stop()

selection_mask = st.session_state['selection_mask']
search_df = search_df[selection_mask]

replays_df = st.session_state.get('replays_df', None)
replays_df = replays_df[replays_df.id.isin(search_df.id)]

with st.spinner("Compulating..."):
    appearances_df = st.session_state['appearances_df']
    try:
        con = sqlite3.connect(':memory:')
        with st.spinner("Creating replays table..."):
            replays_df.loc[:, ['format', 'players', 'uploadtime', 'rating']].to_sql('replays', con)
        with st.spinner("Creating appearances table..."):
            appearances_df.loc[:, ['id', 'player', 'pokemon', 'won']].to_sql('appearances', con)
        con.commit()
        cur = con.cursor()
        cur.execute("""
            CREATE INDEX IF NOT EXISTS marginal_idx ON appearances(id, player, pokemon)
        """)
        win_rates_data = get_pair_marginal_win_rates_conditional(con)
        
        win_rates_df = pd.DataFrame(data=win_rates_data, columns=[
            'pokemon1', 'pokemon2', 'players', 'appearances', 'win', 'Win %',
        ])
        del win_rates_df['players']
        win_rates_df['Win %'] *= 100
        st.session_state['win_rates_df'] = win_rates_df

        replays_df["ymd"] = replays_df.uploadtime.apply(datetime.fromtimestamp).dt.strftime("%Y/%m/%d")
        dfs = []
        for label, group_df in replays_df.groupby(by=["ymd", "format"]):
            format = label[1]
            day_start, day_end = group_df.uploadtime.min(), group_df.uploadtime.max()

            where = f"WHERE uploadtime > {day_start} AND uploadtime <= {day_end} "
            marginal_df = pd.DataFrame(
                data=get_pair_marginal_win_rates_conditional(con, where),
                columns=["p1", "p2", "players", "appearances", "wins", "Win %"],
            )
            marginal_df["Win %"] *= 100
            marginal_df['pair'] = marginal_df.p1 + ", " + marginal_df.p2
            marginal_df['day'] = datetime.fromtimestamp(day_start)
            marginal_df['format'] = format
            dfs.append(marginal_df)
        del replays_df["ymd"]
        daily_marginals_df = pd.concat(dfs, ignore_index=True)
    finally:
        con.close()

st.header("Detailed replay data")
st.write(win_rates_df)

unique_pairs = sorted(daily_marginals_df.pair.unique())
display_list = st.multiselect(
    "Plot daily win rates for these pairs",
    options=unique_pairs
)
disaggregate_formats = st.toggle("By format")
if display_list:
    mask = daily_marginals_df.pair.isin(display_list)
    if disaggregate_formats:
        facetgrid = sns.relplot(
            data=daily_marginals_df[mask],
            kind="line",
            hue='pair',
            style='pair',
            col="format",
            col_wrap=1,
            x='day',
            y='Win %',
            aspect=2,
            estimator='mean',
            errorbar=None,
            linewidth=3,
            palette='colorblind',
        )
        figure = facetgrid.figure
    else:
        figure = plt.figure(figsize=(12, 6))
        ax = plt.gca()
        sns.lineplot(
            data=daily_marginals_df[mask],
            hue='pair',
            style='pair',
            x='day',
            y='Win %',
            estimator='weighted_mean',
            ax=ax,
            linewidth=3,
            palette='colorblind',
        )
    st.write(figure)