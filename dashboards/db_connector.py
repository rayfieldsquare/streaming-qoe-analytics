import psycopg2
import pandas as pd
from typing import Optional
import streamlit as st
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WarehouseDataConnector:
    """
    Connects to Netflix QoE data warehouse and fetches data.

    WHY A CLASS?
    - Reusable connection across all dashboards
    - Connection pooling (efficient)
    - Consistent error handling
    """

    def __init__(self, host="localhost", port=5433,
                 database="streaming_analytics", user="analytics_user",
                 password="analytics_password"):
        """Initialize database connection parameters."""
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.conn = None

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("✅ Connected to database")
            return self.conn
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("🔌 Database connection closed")

    def query(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.

        WHY DATAFRAME?
        - Easy to visualize with Plotly, Streamlit, etc.
        - Pandas has great data manipulation tools
        - Most dashboarding libraries expect DataFrames

        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries

        Returns:
            pandas DataFrame with query results
        """
        try:
            if not self.conn or self.conn.closed:
                self.connect()

            df = pd.read_sql_query(sql, self.conn, params=params)
            logger.info(f"✅ Query returned {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"❌ Query failed: {e}")
            logger.error(f"SQL: {sql}")
            raise

    # ========================================
    # PRE-BUILT QUERIES (Data Access Layer)
    # ========================================
    # These methods fetch specific data for dashboards
    # Think of them as "data endpoints"

    def get_overall_health(self) -> pd.DataFrame:
        """
        Get overall QoE health metrics.

        RETURNS:
        One row with key metrics:
        - total_sessions
        - avg_qoe_score
        - avg_startup_ms
        - pct_poor_quality
        """
        query = """
            SELECT
                COUNT(*) as total_sessions,
                AVG(overall_qoe_score) as avg_qoe_score,
                AVG(startup_time_ms) as avg_startup_ms,
                AVG(rebuffer_ratio) as avg_rebuffer_pct,
                SUM(CASE WHEN session_quality = 'poor' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as pct_poor_quality
            FROM fact_playback_sessions
            WHERE session_timestamp >= NOW() - INTERVAL '48 hours'
        """
        return self.query(query)

    def get_daily_trend(self, days: int = 30) -> pd.DataFrame:
        """
        Get daily QoE trend for last N days.

        RETURNS:
        One row per day with:
        - date
        - sessions
        - avg_qoe_score
        - avg_startup_ms
        """
        query = """
            SELECT
                d.full_date as date,
                d.day_name,
                COUNT(*) as sessions,
                AVG(f.overall_qoe_score) as avg_qoe_score,
                AVG(f.startup_time_ms) as avg_startup_ms,
                AVG(f.rebuffer_ratio) as avg_rebuffer_pct,
                SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as pct_poor
            FROM fact_playback_sessions f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE d.full_date >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY d.full_date, d.day_name
            ORDER BY d.full_date
        """
        return self.query(query, (days,))

    def get_device_breakdown(self) -> pd.DataFrame:
        """
        Get quality metrics by device type.
        """
        query = """
            SELECT
                dev.device_type,
                dev.device_family,
                COUNT(*) as sessions,
                AVG(f.overall_qoe_score) as avg_qoe_score,
                AVG(f.startup_time_ms) as avg_startup_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.startup_time_ms) as p95_startup_ms,
                AVG(f.rebuffer_count) as avg_rebuffer_count
            FROM fact_playback_sessions f
            JOIN dim_device dev ON f.device_key = dev.device_key
            WHERE f.session_timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY dev.device_type, dev.device_family
            ORDER BY sessions DESC
        """
        return self.query(query)

    def get_geographic_breakdown(self) -> pd.DataFrame:
        """
        Get quality metrics by country.
        """
        query = """
            SELECT
                g.country_code,
                g.region,
                COUNT(*) as sessions,
                AVG(f.overall_qoe_score) as avg_qoe_score,
                AVG(f.startup_time_ms) as avg_startup_ms,
                SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as pct_poor
            FROM fact_playback_sessions f
            JOIN dim_geography g ON f.geo_key = g.geo_key
            WHERE f.session_timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY g.country_code, g.region
            HAVING COUNT(*) >= 10
            ORDER BY sessions DESC
        """
        return self.query(query)

    def get_hourly_trend(self, hours: int = 24) -> pd.DataFrame:
        """
        Get hourly QoE trend for last N hours.
        """
        query = """
            SELECT
                DATE_TRUNC('hour', session_timestamp) as hour,
                COUNT(*) as sessions,
                AVG(overall_qoe_score) as avg_qoe_score,
                AVG(startup_time_ms) as avg_startup_ms,
                AVG(rebuffer_ratio) as avg_rebuffer_pct
            FROM fact_playback_sessions
            WHERE session_timestamp >= NOW() - INTERVAL '%s hours'
            GROUP BY DATE_TRUNC('hour', session_timestamp)
            ORDER BY hour
        """
        return self.query(query, (hours,))

    def get_quality_distribution(self) -> pd.DataFrame:
        """
        Get session count by quality category.
        """
        query = """
            SELECT
                session_quality,
                COUNT(*) as session_count
            FROM fact_playback_sessions
            WHERE session_timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY session_quality
            ORDER BY
                CASE session_quality
                    WHEN 'excellent' THEN 1
                    WHEN 'good' THEN 2
                    WHEN 'fair' THEN 3
                    WHEN 'poor' THEN 4
                END
        """
        return self.query(query)

    def get_top_issues(self, limit: int = 10) -> pd.DataFrame:
        """
        Get top quality issues in last 24 hours.
        """
        query = """
            WITH poor_sessions AS (
                SELECT
                    f.*,
                    dev.device_type,
                    g.country_code
                FROM fact_playback_sessions f
                JOIN dim_device dev ON f.device_key = dev.device_key
                JOIN dim_geography g ON f.geo_key = g.geo_key
                WHERE f.session_quality = 'poor'
                AND f.session_timestamp >= NOW() - INTERVAL '24 hours'
            )
            SELECT
                'High Startup Time' as issue_type,
                device_type,
                country_code,
                COUNT(*) as affected_sessions,
                AVG(startup_time_ms)::INTEGER as avg_metric
            FROM poor_sessions
            WHERE startup_time_ms > 4000
            GROUP BY device_type, country_code

            UNION ALL

            SELECT
                'Excessive Buffering',
                device_type,
                country_code,
                COUNT(*),
                AVG(rebuffer_count)::INTEGER
            FROM poor_sessions
            WHERE rebuffer_count >= 3
            GROUP BY device_type, country_code

            ORDER BY affected_sessions DESC
            LIMIT %s
        """
        return self.query(query, (limit,))

    def get_peak_time_analysis(self) -> pd.DataFrame:
        """
        Get quality by time of day.
        """
        query = """
            SELECT
                t.hour,
                t.time_of_day,
                t.is_peak_time,
                COUNT(*) as sessions,
                AVG(f.overall_qoe_score) as avg_qoe_score,
                AVG(f.startup_time_ms) as avg_startup_ms
            FROM fact_playback_sessions f
            JOIN dim_time t ON f.time_key = t.time_key
            WHERE f.session_timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY t.hour, t.time_of_day, t.is_peak_time
            ORDER BY t.hour
        """
        return self.query(query)

# ========================================
# STREAMLIT CACHING
# ========================================
# Cache database connections and queries for performance

@st.cache_resource
def get_db_connector():
    """
    Get database connector (cached for session).

    WHY CACHE?
    Streamlit reruns the entire script on every interaction.
    Caching prevents reconnecting to DB every time!
    """
    return WarehouseDataConnector()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_data(query_name: str, **kwargs):
    """
    Fetch data from database with caching.

    WHY TTL (Time To Live)?
    Data changes over time, so we refresh cache every 5 minutes.
    This balances freshness vs. performance.

    Args:
        query_name: Name of query method (e.g., 'get_overall_health')
        **kwargs: Arguments to pass to query method
    """
    db = get_db_connector()
    query_method = getattr(db, query_name)
    return query_method(**kwargs)