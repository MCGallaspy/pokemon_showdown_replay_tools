import pandas as pd
import seaborn as sns
import streamlit as st

from matplotlib import pyplot as plt

sns.set_style('darkgrid')

@st.cache_resource
def get_data():
    df = pd.read_parquet('streamlit_apps/win_rates.1732571443.parquet')
    df.win_rate = df.win_rate * 100
    return df

df = get_data()

display_list = df.pair.unique()[:3]
st.markdown(f"Displaying results for {display_list}")

@st.cache_data
def get_figure(df, display_list, ycol):
    fig = plt.figure(figsize=(8, 4))
    ax = plt.gca()
    mask = df.pair.isin(display_list)
    sns.lineplot(
        data=df[mask],
        x="week",
        y=ycol,
        style="pair",
        hue="pair",
    )
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    return fig, ax

fig, ax = get_figure(df, display_list, "win_rate")
ax.set_ylim((0, 100))
ax.set_yticks([i * 10 for i in range(11)])
ax.set_yticklabels([f"{i * 10}%" for i in range(11)])
ax.set_title("Win rates over weeks of Regulation H")
st.write(fig)

fig, ax = get_figure(df, display_list, "appearances")
ax.set_title("Appearances over weeks of Regulation H")
st.write(fig)

st.markdown("""**Note:** The data comes from Pokemon Showdown Reg H Bo1 and Bo3 replays with a rating >= 1300.
""")