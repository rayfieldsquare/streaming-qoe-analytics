import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
from db_connector import fetch_data

# ========================================
# PAGE CONFIGURATION
# ========================================

st.set_page_config(
    page_title="Streaming Service QoE - Executive Dashboard",
    page_icon="📺",
    layout="wide",  # Use full screen width
    initial_sidebar_state="collapsed"
)

# ========================================
# STYLING
# ========================================

# Custom CSS to make it look professional
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
        color: #444;
    }
    .stMetric {
        background-color: #444;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 1px 1px 3px rgba(0,0,0,0.1);
    }
    h1 {
        color: #E50914;  /* Streaming Service red */
        padding-bottom: 10px;
        border-bottom: 3px solid #E50914;
    }
    div[data-testid="metric-container"] > label[data-testid="stMetricLabel"] + div {
        color: #444; /* Change the value color to red */
    }
    </style>
    """, unsafe_allow_html=True)

# ========================================
# HEADER
# ========================================

col1, col2 = st.columns([3, 1])

with col1:
    st.title("📺 Streaming Service QoE Executive Dashboard")
    st.markdown("**Real-time Quality of Experience Monitoring**")

with col2:
    st.metric(
        label="Last Updated",
        value=datetime.now().strftime("%H:%M:%S")
    )
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# ========================================
# KEY METRICS (Big Numbers at Top)
# ========================================

st.subheader("📊 Last 48 Hours Performance")

# Fetch overall health data
health_df = fetch_data('get_overall_health')

if not health_df.empty:
    health = health_df.iloc[0]  # Get first (and only) row

    # Create 4 columns for key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        # Quality Score with color coding
        qoe_score = health.get('avg_qoe_score',0)

        # Determine color based on score
        if qoe_score >= 80:
            delta_color = "normal"  # Green
            emoji = "🟢"
        elif qoe_score >= 60:
            delta_color = "off"  # Yellow
            emoji = "🟡"
        else:
            delta_color = "inverse"  # Red
            emoji = "🔴"

        st.metric(
            label=f"{emoji} Avg QoE Score",
            value=f"{qoe_score:.1f}",
            delta=f"Target: 85+",
            delta_color=delta_color
        )

    with col2:
        startup_ms = health.get('avg_startup_ms',0)

        # Good if under 2 seconds
        startup_good = startup_ms < 2000

        st.metric(
            label="⚡ Avg Startup Time",
            value=f"{startup_ms:.0f} ms",
            delta=f"{'✓ Good' if startup_good else '⚠ Slow'}",
            delta_color="normal" if startup_good else "inverse"
        )

    with col3:
        rebuffer_pct = health.get('avg_rebuffer_pct',0)

        # Good if under 2%
        rebuffer_good = rebuffer_pct < 2.0

        st.metric(
            label="⏸️ Avg Rebuffering",
            value=f"{rebuffer_pct:.2f}%",
            delta=f"{'✓ Low' if rebuffer_good else '⚠ High'}",
            delta_color="normal" if rebuffer_good else "inverse"
        )

    with col4:
        pct_poor = health['pct_poor_quality']

        # Good if under 5%
        poor_good = pct_poor < 5.0

        st.metric(
            label="⚠️ Poor Quality Sessions",
            value=f"{pct_poor:.1f}%",
            delta=f"of {health['total_sessions']:,.0f} sessions",
            delta_color="normal" if poor_good else "inverse"
        )

else:
    st.error("❌ Unable to fetch health metrics")

st.markdown("---")

# ========================================
# QUALITY TREND (30-Day Line Chart)
# ========================================

st.subheader("📈 30-Day Quality Trend")

trend_df = fetch_data('get_daily_trend', days=30)

if not trend_df.empty:
    # Create a dual-axis chart
    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]]
    )

    # Add QoE Score line
    fig.add_trace(
        go.Scatter(
            x=trend_df['date'],
            y=trend_df['avg_qoe_score'],
            name='QoE Score',
            line=dict(color='#E50914', width=3),
            mode='lines+markers'
        ),
        secondary_y=False
    )

    # Add session volume as area chart
    fig.add_trace(
        go.Scatter(
            x=trend_df['date'],
            y=trend_df['sessions'],
            name='Sessions',
            fill='tozeroy',
            line=dict(color='rgba(100, 100, 255, 0.3)', width=1),
            mode='lines'
        ),
        secondary_y=True
    )

    # Add target line
    fig.add_hline(
        y=85,
        line_dash="dash",
        line_color="green",
        annotation_text="Target",
        secondary_y=False
    )

    # Update layout
    fig.update_layout(
        height=400,
        hovermode='x unified',
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="QoE Score", secondary_y=False)
    fig.update_yaxes(title_text="Sessions", secondary_y=True)

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("⚠️ No trend data available")

st.markdown("---")

# ========================================
# QUALITY DISTRIBUTION (Pie Chart)
# ========================================

col1, col2 = st.columns(2)

with col1:
    st.subheader("🥧 Quality Distribution")

    quality_df = fetch_data('get_quality_distribution')

    if not quality_df.empty:
        # Create pie chart
        colors = {
            'excellent': '#2ecc71',  # Green
            'good': '#3498db',       # Blue
            'fair': '#f39c12',       # Orange
            'poor': '#e74c3c'        # Red
        }

        fig = px.pie(
            quality_df,
            values='session_count',
            names='session_quality',
            color='session_quality',
            color_discrete_map=colors,
            hole=0.4  # Donut chart
        )

        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=350, showlegend=True)

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ No quality distribution data")

# ========================================
# GEOGRAPHIC BREAKDOWN (Bar Chart)
# ========================================

with col2:
    st.subheader("🌍 Top Countries by Volume")

    geo_df = fetch_data('get_geographic_breakdown')

    if not geo_df.empty:
        # Get top 10 countries
        top_geo = geo_df.nlargest(10, 'sessions')

        # Create horizontal bar chart
        fig = px.bar(
            top_geo,
            y='country_code',
            x='sessions',
            orientation='h',
            color='avg_qoe_score',
            color_continuous_scale='RdYlGn',  # Red-Yellow-Green
            hover_data=['pct_poor']
        )

        fig.update_layout(
            height=350,
            yaxis_title="Country",
            xaxis_title="Sessions",
            coloraxis_colorbar_title="Avg QoE"
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ No geographic data")

st.markdown("---")

# ========================================
# TOP ISSUES (Alert Table)
# ========================================

st.subheader("⚠️ Top Quality Issues (Last 24 Hours)")

issues_df = fetch_data('get_top_issues', limit=5)

if not issues_df.empty:
    # Style the dataframe
    st.dataframe(
        issues_df,
        column_config={
            "issue_type": st.column_config.TextColumn("Issue Type", width="medium"),
            "device_type": st.column_config.TextColumn("Device", width="small"),
            "country_code": st.column_config.TextColumn("Country", width="small"),
            "affected_sessions": st.column_config.NumberColumn(
                "Affected Sessions",
                format="%d"
            ),
            "avg_metric": st.column_config.NumberColumn(
                "Avg Value",
                help="Startup time (ms) or Rebuffer count"
            )
        },
        hide_index=True,
        use_container_width=True
    )
else:
    st.success("✅ No critical issues detected!")

# ========================================
# FOOTER
# ========================================

st.markdown("---")
st.caption("🔄 Auto-refresh: Every 5 minutes | 📊 Data Source: PostgreSQL Data Warehouse | ⏰ Timestamp: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
