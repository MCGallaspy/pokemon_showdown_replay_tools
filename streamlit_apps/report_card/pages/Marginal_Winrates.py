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


st.header("Marginal Win Rates")

search_df = st.session_state.get('search_df', None)
if search_df is None:
    st.warning("Load some data first")
    st.stop()

sns.set_style('darkgrid')



@st.cache_data
def get_replay(replay_id: str):
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

replays_df = st.session_state.get('replays_df', None)
daily_marginals_df = st.session_state.get('daily_marginals_df', None)

if replays_df is not None and daily_marginals_df is not None:
    st.header("Detailed replay data")
    st.write(replays_df)
    
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

if st.button("Download & analyze replay logs"):
    with st.spinner("Compulating..."):
        replays_df = search_df.apply(
            lambda row: get_replay(row.id),
            axis=1,
            result_type="expand",
        )
        replays_df['parse_results'] = replays_df.log.apply(parse_replay)
        replays_df['players'] = replays_df['players'].apply(','.join)
        
        data = []
        report_username = st.session_state['report_username'].lower()
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
        appearances_df = pd.DataFrame(data=data, columns=['id', 'player', 'pokemon', 'won'])
        appearances_df = appearances_df.drop_duplicates(keep='first')
        
        try:
            con = sqlite3.connect(':memory:')
            with st.spinner("Creating replays table..."):
            
                replays_df.loc[:, ['id', 'format', 'players', 'uploadtime', 'rating']].to_sql('replays', con)
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
            st.session_state['replays_df'] = win_rates_df

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
            st.session_state['daily_marginals_df'] = pd.concat(dfs, ignore_index=True)
        finally:
            con.close()

    st.rerun()