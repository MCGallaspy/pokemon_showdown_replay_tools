import pandas as pd
import seaborn as sns
import streamlit as st

from matplotlib import pyplot as plt


df = st.session_state.get('search_df', None)
if df is None:
    st.warning("Load some data first")
    st.stop()

sns.set_style('darkgrid')
fig = plt.figure(figsize=(8, 4))
ax = plt.gca()

sns.lineplot(df, x='game_num', y='rating', hue='format', ax=ax)
sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
ax.set_title(f"Win rate for {st.session_state['report_username']}")
st.write(fig)