import pandas as pd
import seaborn as sns
import streamlit as st

from matplotlib import pyplot as plt

st.header("Format Ratings")

search_df = st.session_state.get('search_df', None)
if search_df is None:
    st.warning("Load some data first")
    st.stop()

search_df = search_df.copy()

selection_mask = st.session_state['selection_mask']
search_df = search_df[selection_mask]

sns.set_style('darkgrid')
fig = plt.figure(figsize=(8, 4))
ax = plt.gca()

search_df['game_num'] = None
for label, group_df in search_df.groupby(by='format'):
    search_df.loc[group_df.index, 'game_num'] = list(range(group_df.shape[0]))

sns.lineplot(search_df, x='game_num', y='rating', hue='format', ax=ax)
sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
ax.set_title(f"Rating for {st.session_state['report_username']}")
st.write(fig)