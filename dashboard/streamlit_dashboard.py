import boto3
import pandas as pd
import streamlit as st
import awswrangler as wr
import plotly.express as px

st.set_page_config(
    page_title="HIV TCE Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

        html, body, [class*="css"] {
            font-family: 'IBM Plex Sans', sans-serif;
            background-color: #0d0f14;
            color: #e2e8f0;
        }

        .stApp {
            background-color: #0d0f14;
        }

        .dashboard-header {
            padding: 2rem 0 1.5rem 0;
            border-bottom: 1px solid #2a2f3d;
            margin-bottom: 2rem;
        }

        .dashboard-title {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 1.1rem;
            font-weight: 600;
            letter-spacing: 0.15em;
            text-transform: uppercase;
            color: #94a3b8;
            margin: 0;
        }

        .dashboard-subtitle {
            font-size: 0.78rem;
            color: #475569;
            font-family: 'IBM Plex Mono', monospace;
            margin-top: 0.3rem;
            letter-spacing: 0.05em;
        }

        .chart-tile {
            background: #131720;
            border: 1px solid #1e2535;
            border-radius: 4px;
            padding: 1.5rem;
        }

        .chart-label {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.7rem;
            font-weight: 600;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #475569;
            margin-bottom: 1rem;
        }

        div[data-testid="stColumns"] {
            gap: 1.25rem;
        }

        div[data-testid="column"] > div {
            height: 100%;
        }

        .stPlotlyChart {
            border-radius: 2px;
        }
    </style>
""", unsafe_allow_html=True)

# ── Session / data ──────────────────────────────────────────────────────────

@st.cache_resource
def get_session():
    return boto3.Session(
    aws_access_key_id=st.secrets["hiv-project"]["aws_access_key_id"],
    aws_secret_access_key=st.secrets["hiv-project"]["aws_secret_access_key"]
    )

@st.cache_data
def load_regimen_data(_session):
    sql = """
        SELECT *
        FROM hivdb_tce.v_new_regimen_class_counts
        ORDER BY tce_count DESC;
    """
    return wr.athena.read_sql_query(
        sql=sql,
        database="hivdb_tce",
        boto3_session=_session,
        s3_output="s3://hiv-data-022784797781/athena-results/"
    )

@st.cache_data
def load_year_data(_session):
    sql = "SELECT * FROM hivdb_tce.tce_by_year ORDER BY baseline_year"
    return wr.athena.read_sql_query(
        sql=sql,
        database="hivdb_tce",
        boto3_session=_session,
        s3_output="s3://hiv-data-022784797781/athena-results/"
    )

# ── Build figures ────────────────────────────────────────────────────────────

def build_donut(df: pd.DataFrame):
    threshold = 7
    df_plot = df.copy()
    df_plot.loc[df_plot['tce_count'] < threshold, 'class_signature'] = 'Other'
    df_plot = df_plot.groupby('class_signature', as_index=False)['tce_count'].sum()
    df_plot = df_plot.sort_values('tce_count', ascending=False)
    other_row = df_plot[df_plot['class_signature'] == 'Other']
    df_plot = pd.concat([df_plot[df_plot['class_signature'] != 'Other'], other_row])

    colors = px.colors.qualitative.Prism
    color_map = {
        sig: colors[i % len(colors)]
        for i, sig in enumerate(df_plot[df_plot['class_signature'] != 'Other']['class_signature'])
    }
    color_map['Other'] = '#3a4155'

    legend_order = list(df_plot[df_plot['class_signature'] != 'Other']['class_signature']) + ['Other']

    fig = px.pie(
        df_plot,
        names="class_signature",
        values="tce_count",
        hole=0.45,
        color="class_signature",
        color_discrete_map=color_map,
        category_orders={"class_signature": legend_order}
    )
    fig.update_traces(
        textposition='inside',
        textinfo='percent',
        textfont=dict(family='IBM Plex Mono', size=11, color='#0d0f14'),
        marker=dict(line=dict(color='#0d0f14', width=2))
    )
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=10, b=10, l=10, r=10),
        legend=dict(
            font=dict(family='IBM Plex Mono', size=10, color='#94a3b8'),
            bgcolor='rgba(0,0,0,0)',
            bordercolor='rgba(0,0,0,0)',
        ),
        height=380
    )
    return fig


def build_line(df: pd.DataFrame):
    df = df.copy()
    df["baseline_year"] = df["baseline_year"].astype(int)
    df = df.sort_values("baseline_year")

    fig = px.line(
        df,
        x="baseline_year",
        y="tce_count",
        markers=True
    )
    fig.update_traces(
        line=dict(color='#60a5fa', width=2),
        marker=dict(color='#60a5fa', size=6, line=dict(color='#0d0f14', width=1.5))
    )
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            title="Baseline Year",
            title_font=dict(family='IBM Plex Mono', size=10, color='#475569'),
            tickfont=dict(family='IBM Plex Mono', size=10, color='#64748b'),
            gridcolor='#1e2535',
            linecolor='#1e2535',
            tickmode='linear',
            dtick=1
        ),
        yaxis=dict(
            title="Treatment Change Events",
            title_font=dict(family='IBM Plex Mono', size=10, color='#475569'),
            tickfont=dict(family='IBM Plex Mono', size=10, color='#64748b'),
            gridcolor='#1e2535',
            linecolor='#1e2535',
        ),
        margin=dict(t=10, b=40, l=10, r=10),
        height=380
    )
    return fig


# ── Layout ───────────────────────────────────────────────────────────────────

st.markdown("""
    <div class="dashboard-header">
        <p class="dashboard-title">HIV Treatment Change Events</p>
        <p class="dashboard-subtitle">Post-TCE drug class regimens &amp; frequency over time</p>
    </div>
""", unsafe_allow_html=True)

session = get_session()

col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="chart-tile"><p class="chart-label">New Regimen Class Combinations</p>', unsafe_allow_html=True)
    with st.spinner(""):
        df_regimen = load_regimen_data(session)
    st.plotly_chart(build_donut(df_regimen), use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="chart-tile"><p class="chart-label">TCE Frequency by Year</p>', unsafe_allow_html=True)
    with st.spinner(""):
        df_year = load_year_data(session)
    st.plotly_chart(build_line(df_year), use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)
