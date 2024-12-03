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
st.header("Moves Used")
st.markdown("Moves you have used, and how often.")

search_df = st.session_state.get('search_df', None)
if search_df is None:
    st.warning("Load some data first")
    st.stop()

selection_mask = st.session_state['selection_mask']
search_df = search_df[selection_mask]

moves_df = st.session_state.get('moves_df', None)
moves_df = moves_df[moves_df.id.isin(search_df.id)].copy()

st.markdown("## Grouped by pokemon")
groupbys = [
    'id',
]
move_counts = moves_df.groupby(by=['pokemon', 'move']).size()
move_counts.name = "times used"

games_used = moves_df.groupby(by=['pokemon', 'move', 'id']).size()
games_used = games_used.reset_index().groupby(['pokemon', 'move']).size()
games_used.name = "games used"
df = pd.concat([
    move_counts,
    games_used,
], axis=1)

df['percent used'] = None
for pokemon in df.reset_index().pokemon.unique():
    total = df.loc[(pokemon, slice(None)), 'times used'].sum()
    percent_used = df.loc[(pokemon, slice(None)), 'times used'] / total * 100
    df.loc[(pokemon, slice(None)), 'percent used'] = percent_used

df = df.reset_index().sort_values(by=['pokemon', 'percent used'], ascending=(True, False))
st.write(df)

st.markdown("## Grouped by move")
groupbys = [
    'pokemon',
    'id',
]
move_counts = moves_df.groupby(by='move').size()
move_counts.name = "times used"

pokemon_used = moves_df.groupby(by=['move', 'pokemon']).size()
pokemon_used = pokemon_used.reset_index().groupby('move').size()
pokemon_used.name = "pokemon used"

games_used = moves_df.groupby(by=['move', 'id']).size()
games_used = games_used.reset_index().groupby('move').size()
games_used.name = "games used"

df = pd.concat([
    move_counts,
    games_used,
    pokemon_used,
], axis=1)
df = df.sort_values(by='times used', ascending=False)
st.write(df)