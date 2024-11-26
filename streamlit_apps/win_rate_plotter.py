import pandas as pd
import seaborn as sns
import streamlit as st

from matplotlib import pyplot as plt

sns.set_style('darkgrid')

@st.cache_resource
def get_data():
    df = pd.read_parquet('streamlit_apps/win_rates.1732576965.parquet')
    df.win_rate = df.win_rate * 100
    return df

df = get_data()

all_pairs = list(df.pair.unique())
display_list = st.multiselect(
    "Select pokémon pairs to display results",
    all_pairs,
    default=all_pairs[:3],
)

if not display_list:
    st.warning("Select at least one pair of pokémon")
    st.stop()

@st.cache_data
def get_figure(df, display_list, ycol, plot_means=False):
    fig = plt.figure(figsize=(8, 4))
    ax = plt.gca()
    mask = df.pair.isin(display_list)
    
    if plot_means:
        average_win_rate = df[mask].win_rate.mean()
        ax.hlines(
            average_win_rate,
            xmin=df.week.min()-1, xmax=df.week.max()+1,
            colors="gray",
            label=f"Average\nwin rate\nfor set ({average_win_rate:.1f}%)",
            linestyles="dashed",
            zorder=-1,
            linewidths=2,
        )
    
    sns.lineplot(
        data=df[mask],
        x="week",
        y=ycol,
        style="pair",
        hue="pair",
        linewidth=3,
    )
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    
    buffer = 0.5
    ax.set_xticks(range(1, df.week.max()+1))
    ax.set_xlim((1-buffer, df.week.max()+buffer))
    
    return fig, ax

cols = st.columns(3)
with cols[0]:
    fixed = st.toggle("Fixed y-axis", value=False)
    st.markdown("Constrain the y-axis to these values. When disabled the limits are determined automatically.")
with cols[1]:
    ymin = st.number_input(
        "Y min",
        0,
        100,
        value=0,
        disabled=not fixed,
    )
with cols[2]:
    ymax = st.number_input(
        "Y max",
        0,
        100,
        value=100,
        disabled=not fixed,
    )

fig, ax = get_figure(df, display_list, "win_rate", plot_means=True)
if fixed:
    ax.set_ylim((ymin, ymax))
yticks = ax.get_yticks()
ax.set_yticklabels([f"{i}%" for i in yticks])
ax.set_title("Win rates over weeks of Regulation H")

st.write(fig)

fig, ax = get_figure(df, display_list, "appearances")
ax.set_title("Appearances over weeks of Regulation H")
st.write(fig)

st.markdown("**Note:** The data comes from Pokemon Showdown Reg H Bo1 and Bo3 replays with a rating >= 1300.")

if st.toggle("Show data table", value=False):
    if st.toggle("Average over all weeks", value=True):
        st.write(df.groupby(by="pair").agg({
            "win_rate": ["mean", "median", "std"],
            "players": "sum",
            "week": "count",
        }))
    else:
        st.write(df[df.pair.isin(display_list)])