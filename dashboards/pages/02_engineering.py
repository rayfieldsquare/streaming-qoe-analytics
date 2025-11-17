import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime
from db_connector import fetch_data

# ========================================
# PAGE CONFIGURATION
# ========================================

st.set_page_config(
    page_title="Streaming Service QoE - Engineering Dashboard",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========================================
# SIDEBAR FILTERS
# ========================================

st.sidebar.title("🔧 Engineering Dashboard")
st.sidebar.markdown("### Filters")

# Time range selector
time_range = st.sidebar.selectbox(
    "Time Range",
    ["Last 6 Hours", "Last 24 Hours", "Last 7 Days", "Last 30 Days"],
    index=1
)

# Convert to hours
time_range_map = {
    "Last 6 Hours": 6,
    "Last 24 Hours": 24,
    "Last 7 Days": 168,
    "Last 30 Days": 720
}
hours = time_range_map[time_range]

# Device filter
device_filter = st.sidebar.multiselect(
    "Device Types",
    ["smart_tv", "mobile", "web", "tablet"],
    default=["smart_tv", "mobile", "web", "tablet"]
)

# Metric selector
metric_focus = st.sidebar.radio(
    "Focus Metric",
    ["Startup Time", "Rebuffering", "Bitrate", "Overall QoE"]
)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.info("💡 **Tip:** Click on legend items to toggle visibility")

# ========================================
# HEADER
# ========================================

st.title("🔧 Netflix QoE Engineering Dashboard")
st.markdown(f"**Technical Diagnostics** | {time_range}")

# ========================================
# DEVICE PERFORMANCE TABLE
# ========================================

st.subheader("📱 Device Performance Breakdown")

device_df = fetch_data('get_device_breakdown')

if not device_df.empty:
    # Filter by selected devices
    device_df = device_df[device_df['device_type'].isin(device_filter)]

    # Add calculated columns
    device_df['p95_startup_sec'] = device_df['p95_startup_ms'] / 1000
    device_df['avg_startup_sec'] = device_df['avg_startup_ms'] / 1000

    # Style based on performance
    def color_qoe(val):
        """Color code QoE scores."""
        if val >= 80:
            color = 'background-color: #d4edda; color: #444'  # Green
        elif val >= 60:
            color = 'background-color: #fff3cd; color: #444'  # Yellow
        else:
            color = 'background-color: #f8d7da; color: #444'  # Red
        return color

    # Display styled dataframe
    styled_df = device_df[['device_type', 'sessions', 'avg_qoe_score',
                           'avg_startup_sec', 'p95_startup_sec', 'avg_rebuffer_count']].style\
        .format({
            'sessions': '{:,.0f}',
            'avg_qoe_score': '{:.1f}',
            'avg_startup_sec': '{:.2f}s',
            'p95_startup_sec': '{:.2f}s',
            'avg_rebuffer_count': '{:.2f}'
        })\
        .applymap(color_qoe, subset=['avg_qoe_score'])\
        .set_properties(**{'text-align': 'center'})

    st.dataframe(styled_df, use_container_width=True)

st.markdown("---")

# ========================================
# HOURLY TREND ANALYSIS
# ========================================

st.subheader(f"⏰ Hourly Trend - {metric_focus}")

hourly_df = fetch_data('get_hourly_trend', hours=min(hours, 48))  # Max 48 hours for readability

if not hourly_df.empty:
    # Determine which metric to plot
    metric_map = {
        "Startup Time": ('avg_startup_ms', 'Startup Time (ms)', 'YlOrRd'),
        "Rebuffering": ('avg_rebuffer_pct', 'Rebuffer %', 'Reds'),
        "Bitrate": ('avg_qoe_score', 'QoE Score', 'RdYlGn'),  # Using QoE as proxy
        "Overall QoE": ('avg_qoe_score', 'QoE Score', 'RdYlGn')
    }

    metric_col, metric_label, color_scale = metric_map[metric_focus]

    # Create line chart with area fill
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=hourly_df['hour'],
        y=hourly_df[metric_col],
        mode='lines+markers',
        name=metric_label,
        fill='tozeroy',
        line=dict(color='#E50914', width=2),
        marker=dict(size=6)
    ))

    # Add session volume as secondary axis
    fig.add_trace(go.Bar(
        x=hourly_df['hour'],
        y=hourly_df['sessions'],
        name='Sessions',
        yaxis='y2',
        opacity=0.3,
        marker_color='lightblue'
    ))

    fig.update_layout(
        height=400,
        hovermode='x unified',
        yaxis=dict(title=metric_label),
        yaxis2=dict(title='Sessions', overlaying='y', side='right'),
        legend=dict(x=0, y=1.1, orientation='h')
    )

    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")


# ========================================
# PERCENTILE DISTRIBUTION
# ========================================
st.subheader("📊 Startup Time Distribution by Device")

col1, col2 = st.columns(2)

with col1:
    # Box plot showing distribution
    if not device_df.empty:
        fig = go.Figure()
        
        for device in device_filter:
            device_data = device_df[device_df['device_type'] == device]
            
            if not device_data.empty:
                # Create synthetic distribution data for visualization
                # In production, you'd query actual percentile data
                fig.add_trace(go.Box(
                    y=[device_data['avg_startup_ms'].values[0]],
                    name=device,
                    boxmean='sd'
                ))

        fig.update_layout(
            height=350,
            yaxis_title="Startup Time (ms)",
            showlegend=True
        )

        st.plotly_chart(fig, use_container_width=True)
    
with col2:
    # Percentile comparison table
    st.markdown("Startup Time Percentiles")
    
    # Query for detailed percentiles
    percentile_query = """
        SELECT
            dev.device_type,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.startup_time_ms) as p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.startup_time_ms) as p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY f.startup_time_ms) as p90,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.startup_time_ms) as p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY f.startup_time_ms) as p99
        FROM fact_playback_sessions f
        JOIN dim_device dev ON f.device_key = dev.device_key
        WHERE f.session_timestamp >= NOW() - INTERVAL '%s hours'
        GROUP BY dev.device_type
    """ % hours
    
    percentile_df = fetch_data('query', sql=percentile_query) if hasattr(fetch_data, 'query') else pd.DataFrame()
    
    if not percentile_df.empty:
        st.dataframe(
            percentile_df.style.format({
                'p50': '{:.0f}ms',
                'p75': '{:.0f}ms',
                'p90': '{:.0f}ms',
                'p95': '{:.0f}ms',
                'p99': '{:.0f}ms'
            }),
            use_container_width=True,
            hide_index=True
        )
    else:
        # Fallback to device_df data
        st.dataframe(device_df[['device_type', 'avg_startup_ms', 'p95_startup_ms']])
	    
st.markdown("---")  

# ========================================
# HEATMAP: Time of Day vs Device
# ========================================

st.subheader("🔥 Quality Heatmap: Hour × Device")

# Query for heatmap data

heatmap_query = """
    SELECT
    t.hour,
    dev.device_type,
    AVG(f.overall_qoe_score) as avg_qoe
    FROM fact_playback_sessions f
    JOIN dim_time t ON f.time_key = t.time_key
    JOIN dim_device dev ON f.device_key = dev.device_key
    WHERE f.session_timestamp >= NOW() - INTERVAL '7 days'
    GROUP BY t.hour, dev.device_type
    ORDER BY t.hour, dev.device_type
"""

try:
    from db_connector import get_db_connector
    db = get_db_connector()
    heatmap_df = db.query(heatmap_query)
    
    if not heatmap_df.empty:
        # Pivot data for heatmap
        heatmap_pivot = heatmap_df.pivot(
            index='hour',
            columns='device_type',
            values='avg_qoe'
        )

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_pivot.values,
        x=heatmap_pivot.columns,
        y=heatmap_pivot.index,
        colorscale='RdYlGn',
        zmid=70,  # Middle value (yellow)
        zmin=50,
        zmax=90,
        colorbar=dict(title="QoE Score"),
        hoverongaps=False
    ))

    fig.update_layout(
        height=400,
        xaxis_title="Device Type",
        yaxis_title="Hour of Day",
        yaxis=dict(tickmode='linear')
    )

    st.plotly_chart(fig, use_container_width=True)

    st.caption("💡 **Interpretation:** Red = Poor Quality, Yellow = Fair, Green = Good")

except Exception as e:
    st.warning(f"⚠️ Could not generate heatmap: {e}")

st.markdown("---")

# ========================================
# GEOGRAPHIC PERFORMANCE
# ========================================

st.subheader("🌍 Geographic Performance Analysis")

geo_df = fetch_data('get_geographic_breakdown')

if not geo_df.empty:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Bar chart: Countries by QoE
        fig = px.bar(
            geo_df.nlargest(15, 'sessions'),
            x='country_code',
            y='avg_qoe_score',
            color='avg_qoe_score',
            color_continuous_scale='RdYlGn',
            hover_data=['sessions', 'avg_startup_ms', 'pct_poor'],
            labels={'avg_qoe_score': 'QoE Score', 'country_code': 'Country'}
        )

        # Add target line
        fig.add_hline(y=85, line_dash="dash", line_color="green",
                        annotation_text="Target")

        fig.update_layout(height=400, showlegend=False)

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Countries Below Target (QoE < 85)**")
    
        poor_countries = geo_df[geo_df['avg_qoe_score'] < 85].sort_values('avg_qoe_score')
    
        if not poor_countries.empty:
            st.dataframe(
                poor_countries[['country_code', 'avg_qoe_score', 'pct_poor']].style.format({
                    'avg_qoe_score': '{:.1f}',
                    'pct_poor': '{:.1f}%'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("✅ All countries above target!")
        
st.markdown("---")

# ========================================
# DETAILED ISSUE INVESTIGATION
# ========================================

st.subheader("🔍 Issue Deep Dive")

tab1, tab2, tab3 = st.tabs(["High Startup Time", "Excessive Buffering", "Low Bitrate"])

with tab1:
    st.markdown("**Sessions with Startup Time > 4 seconds**")
    startup_query = """
        SELECT
            dev.device_type,
            g.country_code,
            COUNT(*) as affected_sessions,
            AVG(f.startup_time_ms)::INTEGER as avg_startup_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.startup_time_ms)::INTEGER as p95_startup_ms
        FROM fact_playback_sessions f
        JOIN dim_device dev ON f.device_key = dev.device_key
        JOIN dim_geography g ON f.geo_key = g.geo_key
        WHERE f.startup_time_ms > 4000
        AND f.session_timestamp >= NOW() - INTERVAL '%s hours'
        GROUP BY dev.device_type, g.country_code
        HAVING COUNT(*) >= 5
        ORDER BY affected_sessions DESC
        LIMIT 20
    """ % hours
    
    try:
        db = get_db_connector()
        startup_issues_df = db.query(startup_query)
    
        if not startup_issues_df.empty:
            st.dataframe(
                startup_issues_df,
                column_config={
                    "device_type": "Device",
                    "country_code": "Country",
                    "affected_sessions": st.column_config.NumberColumn("Sessions", format="%d"),
                    "avg_startup_ms": st.column_config.NumberColumn("Avg (ms)", format="%d"),
                    "p95_startup_ms": st.column_config.NumberColumn("P95 (ms)", format="%d")
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.success("✅ No high startup time issues detected!")
    except Exception as e:
        st.error(f"Error: {e}")
	    
with tab2:
    st.markdown("Sessions with 3+ Rebuffering Events")
    
    rebuffer_query = """
        SELECT
            dev.device_type,
            n.network_type,
            COUNT(*) as affected_sessions,
            AVG(f.rebuffer_count)::DECIMAL(5,2) as avg_rebuffers,
            AVG(f.rebuffer_ratio)::DECIMAL(5,2) as avg_rebuffer_pct
        FROM fact_playback_sessions f
        JOIN dim_device dev ON f.device_key = dev.device_key
        JOIN dim_network n ON f.network_key = n.network_key
        WHERE f.rebuffer_count >= 3
        AND f.session_timestamp >= NOW() - INTERVAL '%s hours'
        GROUP BY dev.device_type, n.network_type
        HAVING COUNT(*) >= 5
        ORDER BY affected_sessions DESC
        LIMIT 20
    """ % hours
    
    try:
        db = get_db_connector()
        rebuffer_issues_df = db.query(rebuffer_query)
    
        if not rebuffer_issues_df.empty:
            st.dataframe(
                rebuffer_issues_df,
                column_config={
                    "device_type": "Device",
                    "network_type": "Network",
                    "affected_sessions": st.column_config.NumberColumn("Sessions", format="%d"),
                    "avg_rebuffers": st.column_config.NumberColumn("Avg Rebuffers", format="%.2f"),
                    "avg_rebuffer_pct": st.column_config.NumberColumn("Avg %", format="%.2f%%")
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.success("✅ No excessive buffering issues detected!")
    except Exception as e:
        st.error(f"Error: {e}")
	    
with tab3:
    st.markdown("Sessions with Bitrate < 2000 kbps")
    
    bitrate_query = """
        SELECT
            dev.device_type,
            n.network_type,
            COUNT(*) as affected_sessions,
            AVG(f.avg_bitrate_kbps)::INTEGER as avg_bitrate,
            MODE() WITHIN GROUP (ORDER BY f.resolution) as common_resolution
        FROM fact_playback_sessions f
        JOIN dim_device dev ON f.device_key = dev.device_key
        JOIN dim_network n ON f.network_key = n.network_key
        WHERE f.avg_bitrate_kbps < 2000
        AND f.session_timestamp >= NOW() - INTERVAL '%s hours'
        GROUP BY dev.device_type, n.network_type
        HAVING COUNT(*) >= 5
        ORDER BY affected_sessions DESC
        LIMIT 20
    """ % hours

    try:
        db = get_db_connector()
        bitrate_issues_df = db.query(bitrate_query)
    
        if not bitrate_issues_df.empty:
            st.dataframe(
                bitrate_issues_df,
                column_config={
                    "device_type": "Device",
                    "network_type": "Network",
                    "affected_sessions": st.column_config.NumberColumn("Sessions", format="%d"),
                    "avg_bitrate": st.column_config.NumberColumn("Avg Bitrate", format="%d kbps"),
                    "common_resolution": "Common Resolution"
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.success("✅ No low bitrate issues detected!")
    except Exception as e:
        st.error(f"Error: {e}")

# ========================================
# FOOTER
# ========================================

st.markdown("---")
st.caption(f"🔄 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 📊 Data Window: {time_range} | 🔧 Engineering Dashboard v1.0")