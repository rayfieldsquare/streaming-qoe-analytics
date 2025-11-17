import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime
from db_connector import fetch_data, get_db_connector

# ========================================
# PAGE CONFIGURATION
# ========================================

st.set_page_config(
    page_title="Streaming Service QoE - Product Dashboard",
    page_icon="🎯",
    layout="wide"
)

# ========================================
# HEADER
# ========================================

st.title("🎯 Streaming Service QoE Product Dashboard")
st.markdown("**User Experience & Engagement Insights**")

# ========================================
# TIME SELECTOR
# ========================================

col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    date_range = st.selectbox(
        "Time Period",
        ["Last 7 Days", "Last 30 Days", "Last 90 Days"],
        index=1
    )

days_map = {"Last 7 Days": 7, "Last 30 Days": 30, "Last 90 Days": 90}
days = days_map[date_range]

with col2:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# ========================================
# QUALITY VS ENGAGEMENT
# ========================================

st.subheader("📊 Quality Impact on User Experience")

# Query: correlation between quality and viewing duration
correlation_query = """
    SELECT
        CASE
            WHEN overall_qoe_score >= 80 THEN 'Excellent (80+)'
            WHEN overall_qoe_score >= 60 THEN 'Good (60-79)'
            WHEN overall_qoe_score >= 40 THEN 'Fair (40-59)'
            ELSE 'Poor (<40)'
        END as quality_tier,
        COUNT(*) as sessions,
        AVG(session_duration_sec / 60.0)::DECIMAL(10,2) as avg_watch_time_min,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY session_duration_sec / 60.0) as median_watch_time_min
    FROM fact_playback_sessions
    WHERE session_timestamp >= NOW() - INTERVAL '%s days'
    GROUP BY
        CASE
            WHEN overall_qoe_score >= 80 THEN 'Excellent (80+)'
            WHEN overall_qoe_score >= 60 THEN 'Good (60-79)'
            WHEN overall_qoe_score >= 40 THEN 'Fair (40-59)'
            ELSE 'Poor (<40)'
        END
    -- ORDER BY
    --     CASE
    --         WHEN quality_tier = 'Excellent (80+)' THEN 1
    --         WHEN quality_tier = 'Good (60-79)' THEN 2
    --         WHEN quality_tier = 'Fair (40-59)' THEN 3
    --         ELSE 4
    --     END
""" % days

try:
    db = get_db_connector()
    correlation_df = db.query(correlation_query)

    if not correlation_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Bar chart: sessions by quality
            fig = px.bar(
                correlation_df,
                x='quality_tier',
                y='sessions',
                color='quality_tier',
                color_discrete_map={
                    'Excellent (80+)': '#2ecc71',
                    'Good (60-79)': '#3498db',
                    'Fair (40-59)': '#f39c12',
                    'Poor (<40)': '#e74c3c'
                },
                title="Session Distribution by Quality"
            )
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Bar chart: watch time by quality
            fig = px.bar(
                correlation_df,
                x='quality_tier',
                y='avg_watch_time_min',
                color='quality_tier',
                color_discrete_map={
                    'Excellent (80+)': '#2ecc71',
                    'Good (60-79)': '#3498db',
                    'Fair (40-59)': '#f39c12',
                    'Poor (<40)': '#e74c3c'
                },
                title="Average Watch Time by Quality"
            )
            fig.update_layout(
                showlegend=False,
                height=350,
                yaxis_title="Minutes"
            )
            st.plotly_chart(fig, use_container_width=True)

        # Key insight
        excellent_data = correlation_df[correlation_df['quality_tier'] == 'Excellent (80+)']['avg_watch_time_min'].values
        poor_data = correlation_df[correlation_df['quality_tier'] == 'Poor (<40)']['avg_watch_time_min'].values
        
        excellent_watch = excellent_data[0] if len(excellent_data) > 0 else 0
        poor_watch = poor_data[0] if len(poor_data) > 0 else 0

        difference = excellent_watch - poor_watch

        st.info(f"💡 **Key Insight:** Users with excellent quality watch {difference:.1f} minutes longer on average than those with poor quality!")
        
        if len(excellent_data) > 0:
            st.info(f"💡 **Key Insight:** Excellent quality sessions detected with {excellent_data[0]:.1f} minutes average watch time.")
        else:
            st.info("💡 **Key Insight:** Insufficient data to compare quality tiers.")

except Exception as e:
    st.error(f"Error loading correlation data: {e}")

st.markdown("---")

# ========================================
# PEAK TIME USAGE PATTERNS
# ========================================

st.subheader("⏰ When Do Users Watch?")

peak_df = fetch_data('get_peak_time_analysis')

if not peak_df.empty:
    # Create combo chart: sessions + quality
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add session volume
    fig.add_trace(
        go.Bar(
            x=peak_df['hour'],
            y=peak_df['sessions'],
            name='Sessions',
            marker_color='lightblue',
            opacity=0.6
        ),
        secondary_y=False
    )

    # Add quality line
    fig.add_trace(
        go.Scatter(
            x=peak_df['hour'],
            y=peak_df['avg_qoe_score'],
            name='Avg QoE',
            line=dict(color='#E50914', width=3),
            mode='lines+markers'
        ),
        secondary_y=True
    )

    # Highlight peak hours (7pm-11pm)
    peak_hours = peak_df[peak_df['is_peak_time'] == True]
    fig.add_vrect(
        x0=19, x1=23,
        fillcolor="yellow", opacity=0.1,
        layer="below", line_width=0,
        annotation_text="Peak Hours",
        annotation_position="top left"
    )

    fig.update_layout(
        height=400,
        hovermode='x unified',
        xaxis_title="Hour of Day",
        legend=dict(x=0, y=1.1, orientation='h')
    )

    fig.update_yaxes(title_text="Sessions", secondary_y=False)
    fig.update_yaxes(title_text="QoE Score", secondary_y=True)

    st.plotly_chart(fig, use_container_width=True)

    # Stats
    col1, col2, col3 = st.columns(3)

    peak_sessions = peak_hours['sessions'].sum()
    total_sessions = peak_df['sessions'].sum()
    peak_pct = (peak_sessions / total_sessions * 100)

    with col1:
        st.metric("Peak Hour Sessions", f"{peak_pct:.1f}%",
                 delta=f"{peak_sessions:,.0f} of {total_sessions:,.0f}")

    with col2:
        peak_qoe = peak_hours['avg_qoe_score'].mean()
        non_peak_qoe = peak_df[peak_df['is_peak_time'] == False]['avg_qoe_score'].mean()
        st.metric("Peak Hour Quality", f"{peak_qoe:.1f}",
                 delta=f"{peak_qoe - non_peak_qoe:.1f} vs non-peak")

    with col3:
        busiest_hour = peak_df.loc[peak_df['sessions'].idxmax()]
        st.metric("Busiest Hour", f"{busiest_hour['hour']}:00",
                 delta=f"{busiest_hour['sessions']:,.0f} sessions")

st.markdown("---")

# ========================================
# DEVICE PREFERENCES
# ========================================

st.subheader("📱 Device Usage & Preferences")

device_df = fetch_data('get_device_breakdown')

if not device_df.empty:
    col1, col2 = st.columns(2)

    with col1:
        # Pie chart: device distribution
        fig = px.pie(
            device_df,
            values='sessions',
            names='device_type',
            title="Device Mix",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Scatter: sessions vs quality by device
        fig = px.scatter(
            device_df,
            x='sessions',
            y='avg_qoe_score',
            size='sessions',
            color='device_type',
            text='device_type',
            title="Device Volume vs Quality",
            labels={'sessions': 'Session Volume', 'avg_qoe_score': 'Quality Score'}
        )

        # Add target line
        fig.add_hline(y=85, line_dash="dash", line_color="green")

        fig.update_traces(textposition='top center')
        fig.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ========================================
# WEEKEND VS WEEKDAY BEHAVIOR
# ========================================

st.subheader("📅 Weekend vs Weekday Patterns")

weekday_query = """
    SELECT
        CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
        COUNT(*) as sessions,
        AVG(f.session_duration_sec / 60.0)::DECIMAL(10,2) as avg_watch_time_min,
        AVG(f.overall_qoe_score)::DECIMAL(5,2) as avg_qoe_score,
        SUM(f.session_duration_sec / 3600.0)::DECIMAL(12,2) as total_watch_hours
    FROM fact_playback_sessions f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '%s days'
    GROUP BY d.is_weekend
""" % days

try:
    db = get_db_connector()
    weekday_df = db.query(weekday_query)

    if not weekday_df.empty:
        col1, col2, col3, col4 = st.columns(4)

        # Calculate deltas
        weekend = weekday_df[weekday_df['day_type'] == 'Weekend'].iloc[0]
        weekday = weekday_df[weekday_df['day_type'] == 'Weekday'].iloc[0]

        with col1:
            delta_sessions = ((weekend['sessions'] - weekday['sessions']) / weekday['sessions'] * 100)
            st.metric(
                "Weekend Sessions",
                f"{weekend['sessions']:,.0f}",
                delta=f"{delta_sessions:+.1f}% vs weekday"
            )

        with col2:
            delta_watch = weekend['avg_watch_time_min'] - weekday['avg_watch_time_min']
            st.metric(
                "Avg Watch Time",
                f"{weekend['avg_watch_time_min']:.1f} min",
                delta=f"{delta_watch:+.1f} min"
            )

        with col3:
            delta_qoe = weekend['avg_qoe_score'] - weekday['avg_qoe_score']
            st.metric(
                "Quality Score",
                f"{weekend['avg_qoe_score']:.1f}",
                delta=f"{delta_qoe:+.1f}"
            )

        with col4:
            st.metric(
                "Total Watch Hours",
                f"{weekend['total_watch_hours']:,.0f}hrs",
                delta="Weekend"
            )

except Exception as e:
    st.error(f"Error: {e}")

st.markdown("---")

# ========================================
# CONTENT PERFORMANCE
# ========================================

st.subheader("🎬 Content Type Analysis")

content_query = """
    SELECT
        c.content_type,
        c.genre,
        COUNT(*) as sessions,
        AVG(f.overall_qoe_score)::DECIMAL(5,2) as avg_qoe_score,
        AVG(f.session_duration_sec / 60.0)::DECIMAL(10,2) as avg_watch_time_min
    FROM fact_playback_sessions f
    JOIN dim_content c ON f.content_key = c.content_key
    WHERE f.session_timestamp >= NOW() - INTERVAL '%s days'
    GROUP BY c.content_type, c.genre
    HAVING COUNT(*) >= 50
    ORDER BY sessions DESC
    LIMIT 15
""" % days

try:
    db = get_db_connector()
    content_df = db.query(content_query)

    if not content_df.empty:
        # Treemap: content by sessions
        fig = px.treemap(
            content_df,
            path=[px.Constant("All Content"), 'content_type', 'genre'],
            values='sessions',
            color='avg_qoe_score',
            color_continuous_scale='RdYlGn',
            hover_data=['avg_watch_time_min']
        )

        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

        st.caption("💡 **How to read:** Larger boxes = more sessions. Color shows quality (green=good, red=poor)")

except Exception as e:
    st.warning(f"Content analysis unavailable: {e}")

# ========================================
# FOOTER
# ========================================

st.markdown("---")
st.caption(f"🎯 Product Dashboard | Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data Period: {date_range}")
