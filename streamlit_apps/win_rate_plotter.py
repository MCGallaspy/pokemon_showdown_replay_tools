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


with st.sidebar:
    mean_win_rate = df.groupby(by=["pair"]).win_rate.mean()
    mse_df = df.set_index('pair')
    mse_df['mean_win_rate'] = mean_win_rate
    mse_df['sq_error'] = (mse_df.mean_win_rate - mse_df.win_rate)**2
    mse_df = mse_df.groupby(by='pair').agg({
        "sq_error": ["mean", "std", "size"],
        "appearances": "sum",
        "week": "count",
    })
    mse_df = mse_df[mse_df.loc[:, ('sq_error', 'size')] >= 2]
    st.markdown("Pick random pairs\nbased on certain criteria")
    num_pairs = st.number_input("Num pairs", 0, None, value=3)
    num_criteria = st.number_input("Num criteria", 0, 100, value=1)
    criteria = []
    operators = []
    low, mid = mse_df.loc[:, ('appearances', 'sum')].quantile([0.99, 0.999])
    mse_cutoff = 50
    for i in range(num_criteria):
        high_usage = f"High usage ({int(mid)}+)"
        med_usage = f"Medium usage ({int(low)} to {int(mid)})"
        low_usage = f"Low usage (below {int(low)})"
        high_consistency = f"High consistency"
        low_consistency = f"Low consistency"
        criterion = st.selectbox(f"Criterion {i+1}", options=[
            high_usage, med_usage, low_usage,
            high_consistency, low_consistency
        ])
        criteria.append(criterion)
        
        if i != num_criteria - 1:
            operator = st.radio(
                "",
                options=["and", "or"],
                index=0,
            )
            operators.append(operator)

    if st.button("Do it!"):
        overall_mask = None
        for criterion_num, criterion in enumerate(criteria):
            if criterion == high_usage:
                mask = mse_df.loc[:, ('appearances', 'sum')] >= mid
            elif criterion == med_usage:
                mask = mse_df.loc[:, ('appearances', 'sum')] < mid
                mask &= mse_df.loc[:, ('appearances', 'sum')] >= low
            elif criterion == low_usage:
                mask = mse_df.loc[:, ('appearances', 'sum')] < low
            elif criterion == high_consistency:
                mask = mse_df.loc[:, ('sq_error', 'mean')] < mse_cutoff
                mask &= mse_df.loc[:, ('week', 'count')] >= 16
            elif criterion == low_consistency:
                mask = mse_df.loc[:, ('sq_error', 'mean')] >= mse_cutoff
                #mask &= mse_df.loc[:, ('appearances', 'sum')] >= 16

            if overall_mask is None:
                overall_mask = mask
            elif criterion_num <= len(operators):
                n = criterion_num - 1
                if operators[n] == "and":
                    overall_mask &= mask
                elif operators[n] == "or":
                    overall_mask |= mask
        
        if overall_mask.sum() > 0:
            sample = mse_df[overall_mask].sample(n=num_pairs)
            st.write(f"Picking from {mse_df[overall_mask].shape[0]} pairs")
            st.session_state['random_selectors'] = list(sample.index)
        else:
            st.warning("No matches")

all_pairs = list(df.pair.unique())
display_list = st.multiselect(
    "Select pokémon pairs to display results",
    all_pairs,
    default=st.session_state.get('random_selectors', None) or "Archaludon, Pelipper",
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
    data_table = df[df.pair.isin(display_list)]
    if st.toggle("Average over all weeks", value=True):
        st.write(data_table.groupby(by="pair").agg({
            "win_rate": ["mean", "median", "std"],
            "players": "sum",
            "week": "count",
        }))
    else:
        st.write(data_table)