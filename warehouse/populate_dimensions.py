import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# DATABASE CONNECTION
# ============================================

def get_db_connection():
    """
    Connect to PostgreSQL database.

    WHY: We need to talk to the database to insert data.
    Think of this like opening the door to your library.
    """
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="streaming_analytics",
            user="analytics_user",
            password="analytics_password"  # Use your actual password!
        )
        logger.info("✅ Connected to database")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise


# ============================================
# POPULATE DIM_DATE
# ============================================

def populate_dim_date(conn, start_date, end_date):
    """
    Populate date dimension with all dates in range.

    WHY: We pre-calculate all date attributes so queries are fast.
    Instead of calculating "is this a weekend?" every time,
    we calculate it once and store it.

    EXAMPLE: For 2 years of data, this creates 730 rows.
    """
    logger.info(f"📅 Populating dim_date from {start_date} to {end_date}...")

    cursor = conn.cursor()

    # Generate all dates in range
    current_date = start_date
    date_rows = []

    # US Holidays (simple list - could expand this)
    holidays = {
        '2025-01-01',  # New Year's
        '2025-07-04',  # Independence Day
        '2025-12-25',  # Christmas
    }

    while current_date <= end_date:
        date_key = int(current_date.strftime('%Y%m%d'))  # e.g., 20251010

        date_row = (
            date_key,
            current_date,
            current_date.weekday(),  # 0=Monday, 6=Sunday
            current_date.strftime('%A'),  # 'Monday', 'Tuesday', etc.
            current_date.day,
            current_date.timetuple().tm_yday,  # Day of year (1-365)
            current_date.isocalendar()[1],  # Week of year
            current_date.month,
            current_date.strftime('%B'),  # 'January', 'February', etc.
            (current_date.month - 1) // 3 + 1,  # Quarter (1-4)
            current_date.year,
            current_date.weekday() >= 5,  # Is weekend?
            current_date.strftime('%Y-%m-%d') in holidays  # Is holiday?
        )

        date_rows.append(date_row)
        current_date += timedelta(days=1)

    # Insert all dates
    insert_query = """
        INSERT INTO dim_date (
            date_key, full_date, day_of_week, day_name, day_of_month,
            day_of_year, week_of_year, month, month_name, quarter,
            year, is_weekend, is_holiday
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (full_date) DO NOTHING
    """

    execute_batch(cursor, insert_query, date_rows, page_size=100)
    conn.commit()

    logger.info(f"✅ Inserted {len(date_rows)} dates into dim_date")


# ============================================
# POPULATE DIM_TIME
# ============================================

def populate_dim_time(conn):
    """
    Populate time dimension with all hours and minutes.

    WHY: Pre-calculate time-of-day categories.

    This creates 1,440 rows (24 hours × 60 minutes)
    But we'll simplify to just hours for this demo.
    """
    logger.info("🕐 Populating dim_time...")

    cursor = conn.cursor()
    time_rows = []

    # Generate all hours (0-23)
    for hour in range(24):
        time_key = hour * 100  # 0, 100, 200, ..., 2300

        # Categorize time of day
        if 6 <= hour < 12:
            time_of_day = 'morning'
        elif 12 <= hour < 17:
            time_of_day = 'afternoon'
        elif 17 <= hour < 22:
            time_of_day = 'evening'
        else:
            time_of_day = 'night'

        # Is peak time? (7pm-11pm)
        is_peak = 19 <= hour <= 23

        # Format hour label
        hour_12 = hour % 12
        if hour_12 == 0:
            hour_12 = 12
        am_pm = 'AM' if hour < 12 else 'PM'
        hour_label = f'{hour_12}:00 {am_pm}'

        time_row = (
            time_key,
            hour,
            0,  # minute (simplified to hour only)
            time_of_day,
            is_peak,
            hour_label
        )

        time_rows.append(time_row)

    insert_query = """
        INSERT INTO dim_time (
            time_key, hour, minute, time_of_day, is_peak_time, hour_label
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """

    execute_batch(cursor, insert_query, time_rows)
    conn.commit()

    logger.info(f"✅ Inserted {len(time_rows)} time periods into dim_time")


# ============================================
# POPULATE DIM_NETWORK
# ============================================

def populate_dim_network(conn):
    """
    Populate network dimension with common network types.
    """
    logger.info("📡 Populating dim_network...")

    cursor = conn.cursor()

    # Define network types and their characteristics
    networks = [
        ('ethernet', 'excellent', 100.0, False),
        ('ethernet', 'good', 50.0, False),
        ('wifi', 'excellent', 80.0, False),
        ('wifi', 'good', 40.0, False),
        ('wifi', 'fair', 15.0, False),
        ('wifi', 'poor', 5.0, False),
        ('cellular_5g', 'excellent', 60.0, True),
        ('cellular_5g', 'good', 30.0, True),
        ('cellular_5g', 'fair', 10.0, True),
        ('cellular_4g', 'good', 20.0, True),
        ('cellular_4g', 'fair', 8.0, True),
        ('cellular_4g', 'poor', 3.0, True),
    ]
    insert_query = """
        INSERT INTO dim_network (
            network_type, network_quality, estimated_bandwidth_mbps, is_metered
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (network_type, network_quality) DO NOTHING
    """

    execute_batch(cursor, insert_query, networks)
    conn.commit()

    logger.info(f"✅ Inserted {len(networks)} network configurations into dim_network")


# ============================================
# POPULATE DIM_USER_COHORT
# ============================================

def populate_dim_user_cohort(conn):
    """
    Populate user cohort dimension.

    NOTE: We don't store individual users (privacy!).
    Instead, we group users into cohorts with similar characteristics.
    """
    logger.info("👥 Populating dim_user_cohort...")

    cursor = conn.cursor()

    cohorts = []

    # Generate cohorts by signup month and behavior
    signup_months = ['2024-01', '2024-06', '2025-01', '2025-06']
    tiers = ['basic', 'standard', 'premium']
    cohort_types = [
        ('new_users', 30, 5.0),
        ('casual_viewers', 120, 10.0),
        ('regular_users', 180, 20.0),
        ('power_users', 365, 35.0)
    ]

    for signup_month in signup_months:
        for tier in tiers:
            for cohort_name, age_days, viewing_hours in cohort_types:
                cohort = (
                    f'{cohort_name}_{tier}',  # e.g., 'power_users_premium'
                    signup_month,
                    tier,
                    age_days,
                    viewing_hours
                )
                cohorts.append(cohort)

    insert_query = """
        INSERT INTO dim_user_cohort (
            cohort_name, signup_month, subscription_tier,
            account_age_days, avg_viewing_hours_per_week
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (cohort_name, signup_month) DO NOTHING
    """

    execute_batch(cursor, insert_query, cohorts)
    conn.commit()

    logger.info(f"✅ Inserted {len(cohorts)} cohorts into dim_user_cohort")


# ============================================
# POPULATE DIM_CONTENT
# ============================================

def populate_dim_content(conn):
    """
    Populate content dimension with sample content.

    In reality, this would come from Netflix's content catalog API.
    """
    logger.info("🎬 Populating dim_content...")

    cursor = conn.cursor()

    contents = []

    genres = ['Action', 'Comedy', 'Drama', 'Documentary', 'Thriller', 'Romance']
    content_types = ['movie', 'episode']
    codecs = ['H.264', 'H.265', 'AV1']

    # Generate 1000 sample content items
    for i in range(1, 1001):
        content = (
            f'content_{i}',
            content_types[i % 2],  # Alternate between movie and episode
            genres[i % len(genres)],
            45 if i % 2 == 1 else 120,  # Episodes 45min, Movies 120min
            2020 + (i % 5),  # Release years 2020-2024
            i % 3 == 0,  # Every 3rd is a Netflix Original
            codecs[i % len(codecs)],
            '4K' if i % 4 == 0 else '1080p'  # 25% are 4K
        )
        contents.append(content)

    insert_query = """
        INSERT INTO dim_content (
            content_id, content_type, genre, duration_minutes,
            release_year, is_original, video_codec, max_resolution
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (content_id) DO NOTHING
    """

    execute_batch(cursor, insert_query, contents, page_size=100)
    conn.commit()

    logger.info(f"✅ Inserted {len(contents)} content items into dim_content")


# ============================================
# MAIN EXECUTION
# ============================================

def populate_all_dimensions():
    """
    Populate all dimension tables.

    This is like stocking your library's reference section
    before you start adding the main books (fact data).
    """
    logger.info("🚀 Starting dimension population...")

    conn = get_db_connection()

    try:
        # Populate each dimension
        populate_dim_date(
            conn,
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2025, 12, 31)
        )

        populate_dim_time(conn)
        populate_dim_network(conn)
        populate_dim_user_cohort(conn)
        populate_dim_content(conn)

        logger.info("✅ All dimensions populated successfully!")

    except Exception as e:
        logger.error(f"❌ Error populating dimensions: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    populate_all_dimensions()
