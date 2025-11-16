import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from datetime import datetime
import logging
import numpy as np

from data_generation.generate_telemetry import BASE_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# DATABASE CONNECTION
# ============================================

def get_db_connection():
    """Connect to PostgreSQL database."""
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="streaming_analytics",
        user="analytics_user",
        password="analytics_password"
    )
    return conn


# ============================================
# DIMENSION KEY LOOKUP
# ============================================
# Before we can insert into the fact table, we need to find
# the corresponding keys in dimension tables.
# This is called "surrogate key lookup"

class DimensionKeyLookup:
    """
    Helper class to find dimension keys for fact records.

    WHY: Our fact table stores keys (numbers), not text.
    We need to convert "smart_tv" → device_key 123
    """

    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

        # Cache dimension data in memory for speed
        self._load_dimension_caches()

    def _load_dimension_caches(self):
        """
        Load all dimension tables into memory.

        WHY: Instead of querying the database for each fact row,
        we load dimensions once and look them up in memory (much faster!).
        """
        logger.info("📚 Loading dimension caches...")

        # Load dim_date
        self.cursor.execute("SELECT date_key, full_date FROM dim_date")
        self.date_cache = {
            str(row[1]): row[0] for row in self.cursor.fetchall()
        }

        # Load dim_time
        self.cursor.execute("SELECT time_key, hour FROM dim_time")
        self.time_cache = {row[1]: row[0] for row in self.cursor.fetchall()}

        # Load dim_device
        self.cursor.execute("""
            SELECT device_key, device_type, os_version, app_version
            FROM dim_device
        """)
        self.device_cache = {
            (row[1], row[2], row[3]): row[0]
            for row in self.cursor.fetchall()
        }

        # Load dim_geography
        self.cursor.execute("""
            SELECT geo_key, country_code, isp, cdn_pop
            FROM dim_geography
        """)
        self.geo_cache = {
            (row[1], row[2], row[3]): row[0]
            for row in self.cursor.fetchall()
        }

        # Load dim_content
        self.cursor.execute("SELECT content_key, content_id FROM dim_content")
        self.content_cache = {row[1]: row[0] for row in self.cursor.fetchall()}

        # Load dim_network
        self.cursor.execute("""
            SELECT network_key, network_type, network_quality
            FROM dim_network
        """)
        self.network_cache = {
            (row[1], row[2]): row[0]
            for row in self.cursor.fetchall()
        }

        logger.info("✅ Dimension caches loaded")

    def get_date_key(self, timestamp):
        """Convert timestamp to date_key."""
        date_str = str(timestamp.date())
        return self.date_cache.get(date_str)

    def get_time_key(self, timestamp):
        """Convert timestamp to time_key."""
        hour = timestamp.hour
        return self.time_cache.get(hour)

    def get_device_key(self, device_type, os_version, app_version):
        """
        Get device_key, creating new device if not exists.

        WHY "get or create": New devices appear all the time
        (new OS versions, app updates). We automatically add them.
        """
        key = (device_type, os_version, app_version)

        if key in self.device_cache:
            return self.device_cache[key]

        # Device doesn't exist - create it
        self.cursor.execute("""
            INSERT INTO dim_device (
                device_type, device_family, os_version, app_version,
                screen_size, supports_4k
            ) VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING device_key
        """, (
            device_type,
            self._get_device_family(device_type),
            os_version,
            app_version,
            self._get_screen_size(device_type),
            device_type == 'smart_tv'  # Only smart TVs support 4K (simplified)
        ))

        new_key = self.cursor.fetchone()[0]
        self.device_cache[key] = new_key
        self.conn.commit()

        return new_key

    def _get_device_family(self, device_type):
        """Map device type to family."""
        mapping = {
            'smart_tv': 'TV',
            'mobile': 'Mobile',
            'tablet': 'Mobile',
            'web': 'Desktop'
        }
        return mapping.get(device_type, 'Unknown')

    def _get_screen_size(self, device_type):
        """Map device type to screen size."""
        mapping = {
            'smart_tv': 'large',
            'web': 'medium',
            'tablet': 'medium',
            'mobile': 'small'
        }
        return mapping.get(device_type, 'medium')

    def get_geo_key(self, country_code, isp, cdn_pop):
        """Get geography key, creating if not exists."""
        key = (country_code, isp, cdn_pop)

        if key in self.geo_cache:
            return self.geo_cache[key]

        # Geography doesn't exist - create it
        region, timezone_group, market_maturity = self._get_geo_attributes(country_code)

        self.cursor.execute("""
            INSERT INTO dim_geography (
                country_code, region, timezone_group,
                market_maturity, isp, cdn_pop
            ) VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING geo_key
        """, (country_code, region, timezone_group, market_maturity, isp, cdn_pop))

        new_key = self.cursor.fetchone()[0]
        self.geo_cache[key] = new_key
        self.conn.commit()

        return new_key

    def _get_geo_attributes(self, country_code):
        """Get geographic attributes for a country."""
        # Simplified mapping
        geo_map = {
            'US': ('North America', 'Americas', 'mature'),
            'MX': ('North America', 'Americas', 'growing'),
            'BR': ('South America', 'Americas', 'growing'),
            'GB': ('Europe', 'EMEA', 'mature'),
            'FR': ('Europe', 'EMEA', 'mature'),
            'DE': ('Europe', 'EMEA', 'mature'),
            'IN': ('Asia', 'APAC', 'emerging'),
            'JP': ('Asia', 'APAC', 'mature'),
            'KR': ('Asia', 'APAC', 'mature'),
        }
        return geo_map.get(country_code, ('Unknown', 'Unknown', 'emerging'))

    def get_content_key(self, content_id):
        """Get content key."""
        return self.content_cache.get(content_id)

    def get_network_key(self, network_type, network_quality_inferred):
        """Get network key."""
        key = (network_type, network_quality_inferred)
        return self.network_cache.get(key)

    def get_cohort_key(self):
        """
        Get a user cohort key.

        In reality, we'd look up the actual user's cohort.
        For this demo, we'll randomly assign cohorts.
        """
        # Just return a random cohort for demo purposes
        # In production, this would be based on actual user data
        return np.random.randint(1, 48)


# ============================================
# FACT DATA LOADER
# ============================================

def load_fact_data(csv_file='netflix_telemetry_transformed.csv', batch_size=1000):
    """
    Load transformed telemetry data into fact table.

    PROCESS:
    1. Read CSV file
    2. For each row, look up dimension keys
    3. Insert into fact table in batches

    WHY BATCHES: Inserting 100K rows one-by-one is slow.
    Batching inserts 1000 at a time is much faster!
    """
    logger.info(f"📥 Loading fact data from {csv_file}...")

    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    # Initialize dimension lookup
    dim_lookup = DimensionKeyLookup(conn)

    # Read CSV
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)), '..')
    df = pd.read_csv(os.path.join(BASE_DIR, 'data', csv_file), parse_dates=['timestamp'])
    logger.info(f"📊 Loaded {len(df):,} rows from CSV")

    # Track progress
    total_rows = len(df)
    inserted_count = 0
    error_count = 0

    # Prepare batch
    fact_batch = []

    for idx, row in df.iterrows():
        try:
            # Convert timestamp to date and time keys
            timestamp = pd.to_datetime(row['timestamp'])
            date_key = dim_lookup.get_date_key(timestamp)
            time_key = dim_lookup.get_time_key(timestamp)

            if not date_key or not time_key:
                logger.warning(f"Missing date/time key for row {idx}")
                error_count += 1
                continue

            # Get dimension keys
            device_key = dim_lookup.get_device_key(
                row['device_type'],
                row['os_version'],
                row['app_version']
            )

            geo_key = dim_lookup.get_geo_key(
                row['country_code'],
                row['isp'],
                row['cdn_pop']
            )

            content_key = dim_lookup.get_content_key(row['content_id'])
            network_key = dim_lookup.get_network_key(
                row['network_type'],
                row.get('network_quality_inferred', 'good')
            )
            cohort_key = dim_lookup.get_cohort_key()

            # Build fact row
            fact_row = (
                row['session_id'],
                date_key,
                time_key,
                device_key,
                cohort_key,
                geo_key,
                content_key,
                network_key,
                timestamp,

                # Performance metrics
                int(row['startup_time_ms']),
                row.get('startup_category', 'good'),
                int(row['rebuffer_count']),
                int(row['rebuffer_duration_ms']),
                float(row.get('rebuffer_ratio', 0)),
                row.get('buffering_severity', 'none'),

                # Video quality
                int(row['bitrate_kbps']),
                int(row['bitrate_kbps']),  # min_bitrate (simplified)
                int(row['bitrate_kbps']),  # max_bitrate (simplified)
                row['resolution'],
                int(row.get('quality_score', 50)),

                # Playback metrics
                int(row.get('frames_dropped', 0)),
                int(row['session_duration_sec']),
                int(row['session_duration_sec']) - int(row['rebuffer_duration_ms'] / 1000),

                # Overall quality
                float(row.get('overall_qoe_score', 50)),
                row.get('session_quality', 'good'),

                # Flags
                False,  # video_start_failure
                False   # error_occurred
            )

            fact_batch.append(fact_row)

            # Insert batch when it reaches batch_size
            if len(fact_batch) >= batch_size:
                _insert_fact_batch(cursor, fact_batch)
                inserted_count += len(fact_batch)
                conn.commit()

                # Progress update
                progress = (inserted_count / total_rows) * 100
                logger.info(f"  Progress: {inserted_count:,}/{total_rows:,} ({progress:.1f}%)")

                fact_batch = []

        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            error_count += 1
            continue

    # Insert remaining batch
    if fact_batch:
        _insert_fact_batch(cursor, fact_batch)
        inserted_count += len(fact_batch)
        conn.commit()

    # Final stats
    logger.info(f"\n✅ Fact data load complete!")
    logger.info(f"  Total processed: {total_rows:,}")
    logger.info(f"  Successfully inserted: {inserted_count:,}")
    logger.info(f"  Errors: {error_count:,}")

    conn.close()


def _insert_fact_batch(cursor, batch):
    """Insert a batch of fact rows."""
    insert_query = """
        INSERT INTO fact_playback_sessions (
            session_id, date_key, time_key, device_key, cohort_key,
            geo_key, content_key, network_key, session_timestamp,
            startup_time_ms, startup_category, rebuffer_count,
            rebuffer_duration_ms, rebuffer_ratio, buffering_severity,
            avg_bitrate_kbps, min_bitrate_kbps, max_bitrate_kbps,
            resolution, quality_score, frames_dropped,
            session_duration_sec, playback_duration_sec,
            overall_qoe_score, session_quality,
            video_start_failure, error_occurred
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (session_id) DO NOTHING
    """

    execute_batch(cursor, insert_query, batch, page_size=1000)


# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    load_fact_data('netflix_telemetry_transformed.csv')
