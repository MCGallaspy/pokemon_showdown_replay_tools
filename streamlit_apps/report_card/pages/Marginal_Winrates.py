import pandas as pd
import seaborn as sns
import streamlit as st

from matplotlib import pyplot as plt


search_df = st.session_state.get('search_df', None)
if search_df is None:
    st.warning("Load some data first")
    st.stop()

sns.set_style('darkgrid')
fig = plt.figure(figsize=(8, 4))
ax = plt.gca()

if st.button("Download & analyze replays"):
    pass