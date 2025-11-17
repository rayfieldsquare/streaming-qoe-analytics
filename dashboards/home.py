import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="Streaming Service QoE Dashboards",
    page_icon="📺",
    layout="wide"
)

# ========================================
# HEADER
# ========================================

col1, col2 = st.columns([3, 1])

with col1:
    st.title("📺 Streaming Service Quality of Experience")
    st.markdown("### Analytics Dashboard Suite")

with col2:
    st.image("https://upload.wikimedia.org/wikipedia/commons/0/08/Netflix_2015_logo.svg", width=150)

st.markdown("---")

# ========================================
# WELCOME MESSAGE
# ========================================

st.markdown("""
## Welcome to the Streaming Service QoE Analytics Platform

This dashboard suite provides comprehensive insights into streaming quality across
300+ million Streaming Service members worldwide.

### 📊 Available Dashboards

Select a dashboard from the sidebar to get started:
""")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    #### 📊 Executive Dashboard

    **For:** C-Suite, VPs, Directors

    **Focus:** High-level health metrics

    - Overall QoE score
    - Daily trends
    - Geographic overview
    - Top issues

    ➡️ **Use when:** You need a quick health check
    """)

with col2:
    st.markdown("""
    #### 🔧 Engineering Dashboard

    **For:** Engineers, DevOps, SREs

    **Focus:** Technical deep-dive

    - Device breakdowns
    - Percentile analysis
    - Hourly trends
    - Issue investigation

    ➡️ **Use when:** You're debugging problems
    """)

with col3:
    st.markdown("""
    #### 🎯 Product Dashboard

    **For:** Product Managers, UX Researchers

    **Focus:** User behavior & engagement

    - Quality vs engagement
    - Usage patterns
    - Device preferences
    - Content performance

    ➡️ **Use when:** You're analyzing user experience
    """)

st.markdown("---")

# ========================================
# QUICK STATS
# ========================================

st.subheader("📈 Platform Statistics")

from db_connector import fetch_data

try:
    health = fetch_data('get_overall_health').iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Sessions (24h)", f"{health['total_sessions']:,.0f}")

    with col2:
        st.metric("Avg QoE Score", f"{health['avg_qoe_score']:.1f}")

    with col3:
        st.metric("Avg Startup", f"{health['avg_startup_ms']:.0f}ms")

    with col4:
        st.metric("Poor Quality %", f"{health['pct_poor_quality']:.1f}%")

except Exception as e:
    st.warning("⚠️ Unable to load quick stats")

st.markdown("---")

# ========================================
# DOCUMENTATION
# ========================================

with st.expander("📚 Documentation & Help"):
    st.markdown("""
    ### How to Use These Dashboards

    **Navigation:**
    - Use the sidebar (click **>** if hidden) to switch between dashboards
    **Interacting with Charts:**
    - **Hover** over chart elements to see detailed information
    - **Click** legend items to show/hide data series
    - **Zoom** by clicking and dragging on charts
    - **Pan** by holding shift and dragging
    - **Reset** by double-clicking the chart

    **Filters:**
    - Engineering dashboard has time range and device filters in the sidebar
    - Product dashboard has time period selector at the top

    **Performance Tips:**
    - Data is cached for 5 minutes for faster loading
    - Use the 🔄 Refresh button to force update
    - Smaller time ranges load faster

    ### Data Sources

    - **Database:** PostgreSQL Data Warehouse
    - **Update Frequency:** Real-time (5-minute cache)
    - **Data Retention:** 90 days of detailed data, 2 years of aggregates

    ### Key Metrics Explained

    - **QoE Score:** Overall quality score (0-100) combining startup, buffering, and bitrate
    - **Startup Time:** Milliseconds from play button to first frame
    - **Rebuffer Ratio:** Percentage of session spent buffering
    - **P95:** 95th percentile - 95% of values are below this

    ### Support

    For questions or issues, contact:
    - Engineering: engineering@netflix.com
    - Data Team: data-insights@netflix.com
    """)

# ========================================
# FOOTER
# ========================================

st.markdown("---")

st.caption(f"""
    Streaming Service QoE Analytics Platform v1.0 |
    Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |
    Built with ❤️ using Streamlit & PostgreSQL
""")

