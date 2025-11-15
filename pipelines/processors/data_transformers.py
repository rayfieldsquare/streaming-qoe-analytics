import pandas as pd
import numpy as np
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TelemetryTransformer:
    """
    Transforms raw telemetry into analytics-ready features.

    Think of this as a chef preparing ingredients:
    Raw data = whole vegetables
    Transformed data = chopped, seasoned, ready to cook
    """

    def transform_all(self, df):
        """
        Apply all transformations to the dataset.
        """
        logger.info(f"🔧 Starting transformation of {len(df):,} records...")

        df = self.add_time_features(df)
        df = self.calculate_quality_metrics(df)
        df = self.add_session_classifications(df)
        df = self.add_device_categories(df)
        df = self.add_geographic_features(df)

        logger.info("✅ Transformation complete!")
        return df

    def add_time_features(self, df):
        """
        Extract useful time-based features from timestamp.

        WHY THIS MATTERS:
        Netflix wants to know "when do quality issues happen?"
        - During peak hours? (more server load)
        - On weekends? (different content)
        - At night? (different network conditions)
        """
        logger.info("  Adding time features...")

        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Extract components
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['day_name'] = df['timestamp'].dt.day_name()

        # Create time of day categories
        def categorize_time_of_day(hour):
            if 6 <= hour < 12:
                return 'morning'
            elif 12 <= hour < 17:
                return 'afternoon'
            elif 17 <= hour < 22:
                return 'evening'
            else:
                return 'night'

        df['time_of_day'] = df['hour'].apply(categorize_time_of_day)

        # Is it a weekend?
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

        # Is it peak viewing time? (7pm - 11pm)
        df['is_peak_time'] = ((df['hour'] >= 19) & (df['hour'] <= 23)).astype(int)

        logger.info(f"    ✅ Added {7} time-based features")
        return df

    def calculate_quality_metrics(self, df):
        """
        Calculate derived QoE metrics.

        THESE ARE THE METRICS NETFLIX ACTUALLY USES!
        They tell us "how good was this viewing session?"
        """
        logger.info("  Calculating quality metrics...")

        # 1. Rebuffer Ratio
        # What percentage of the session was spent buffering?
        # Formula: (rebuffer_duration_ms / session_duration_sec * 1000) * 100
        df['rebuffer_ratio'] = (
            df['rebuffer_duration_ms'] / (df['session_duration_sec'] * 1000)
        ) * 100
        df['rebuffer_ratio'] = df['rebuffer_ratio'].fillna(0).clip(0, 100)

        # 2. Average Bitrate Quality Score
        # Convert bitrate to a 0-100 score
        # 480p (1000 kbps) = 25
        # 720p (3000 kbps) = 50
        # 1080p (6000 kbps) = 75
        # 4K (20000+ kbps) = 100
        def bitrate_to_score(bitrate):
            if bitrate >= 20000:
                return 100
            elif bitrate >= 6000:
                return 75
            elif bitrate >= 3000:
                return 50
            elif bitrate >= 1000:
                return 25
            else:
                return 10

        df['quality_score'] = df['bitrate_kbps'].apply(bitrate_to_score)

        # 3. Startup Performance Category
        # How fast did it start?
        def categorize_startup(startup_ms):
            if startup_ms < 1000:
                return 'excellent'  # Under 1 second
            elif startup_ms < 2000:
                return 'good'       # 1-2 seconds
            elif startup_ms < 4000:
                return 'fair'       # 2-4 seconds
            else:
                return 'poor'       # Over 4 seconds

        df['startup_category'] = df['startup_time_ms'].apply(categorize_startup)

        # 4. Overall QoE Score (0-100)
        # Weighted combination of factors
        startup_weight = 0.3
        rebuffer_weight = 0.4
        quality_weight = 0.3

        # Normalize startup time (lower is better, so invert)
        startup_normalized = 100 - (df['startup_time_ms'] / 300)  # 30s = 0 score
        startup_normalized = startup_normalized.clip(0, 100)

        # Normalize rebuffering (lower is better, so invert)
        rebuffer_normalized = 100 - df['rebuffer_ratio']

        # Quality score already 0-100
        quality_normalized = df['quality_score']

        df['overall_qoe_score'] = (
            startup_normalized * startup_weight +
            rebuffer_normalized * rebuffer_weight +
            quality_normalized * quality_weight
        )

        # 5. Video Start Failure Flag
        # Did the session fail to start?
        # (In our synthetic data, we don't have failures, but in real data we would)
        df['video_start_failure'] = 0  # Placeholder

        logger.info(f"    ✅ Added {6} quality metrics")
        return df

    def add_session_classifications(self, df):
        """
        Classify sessions into meaningful categories.

        WHY THIS HELPS:
        Instead of looking at individual numbers, we can ask:
        "What percentage of sessions were 'poor quality'?"
        "Which device type has the most 'excellent' sessions?"
        """
        logger.info("  Adding session classifications...")

        # 1. Overall Session Quality
        def classify_session_quality(score):
            if score >= 80:
                return 'excellent'
            elif score >= 60:
                return 'good'
            elif score >= 40:
                return 'fair'
            else:
                return 'poor'

        df['session_quality'] = df['overall_qoe_score'].apply(classify_session_quality)

        # 2. Viewing Duration Category
        # Short: < 10 minutes (just browsing)
        # Medium: 10-40 minutes (single episode)
        # Long: > 40 minutes (movie or binge-watching)
        def classify_duration(duration_sec):
            duration_min = duration_sec / 60
            if duration_min < 10:
                return 'short'
            elif duration_min < 40:
                return 'medium'
            else:
                return 'long'

        df['viewing_duration_category'] = df['session_duration_sec'].apply(classify_duration)

        # 3. Buffering Severity
        def classify_buffering(rebuffer_count):
            if rebuffer_count == 0:
                return 'none'
            elif rebuffer_count <= 2:
                return 'minor'
            elif rebuffer_count <= 5:
                return 'moderate'
            else:
                return 'severe'

        df['buffering_severity'] = df['rebuffer_count'].apply(classify_buffering)

        # 4. Network Quality Inference
        # Based on achieved bitrate, infer network quality
        def infer_network_quality(bitrate):
            if bitrate >= 10000:
                return 'excellent'
            elif bitrate >= 5000:
                return 'good'
            elif bitrate >= 2000:
                return 'fair'
            else:
                return 'poor'

        df['network_quality_inferred'] = df['bitrate_kbps'].apply(infer_network_quality)

        logger.info(f"    ✅ Added {4} classification features")
        return df

    def add_device_categories(self, df):
        """
        Group devices into broader categories for analysis.
        """
        logger.info("  Adding device categories...")

        # Device family grouping
        device_family_map = {
            'smart_tv': 'TV',
            'mobile': 'Mobile',
            'tablet': 'Mobile',  # Group tablets with mobile
            'web': 'Desktop'
        }

        df['device_family'] = df['device_type'].map(device_family_map)

        # Screen size category (inferred from device)
        screen_size_map = {
            'smart_tv': 'large',
            'web': 'medium',
            'tablet': 'medium',
            'mobile': 'small'
        }

        df['screen_size'] = df['device_type'].map(screen_size_map)

        logger.info(f"    ✅ Added {2} device features")
        return df

    def add_geographic_features(self, df):
        """
        Add geographic insights.
        """
        logger.info("  Adding geographic features...")

        # Region grouping
        region_map = {
            'US': 'North America',
            'MX': 'North America',
            'BR': 'South America',
            'GB': 'Europe',
            'FR': 'Europe',
            'DE': 'Europe',
            'IN': 'Asia',
            'JP': 'Asia',
            'KR': 'Asia'
        }

        df['region'] = df['country_code'].map(region_map)

        # Market maturity (how long Netflix has been there)
        # Mature markets tend to have better infrastructure
        market_maturity_map = {
            'US': 'mature',
            'GB': 'mature',
            'MX': 'growing',
            'BR': 'growing',
            'IN': 'emerging',
            'FR': 'mature',
            'DE': 'mature',
            'JP': 'mature',
            'KR': 'mature'
        }

        df['market_maturity'] = df['country_code'].map(market_maturity_map)

        # Timezone grouping (for global analysis)
        timezone_map = {
            'US': 'Americas',
            'MX': 'Americas',
            'BR': 'Americas',
            'GB': 'EMEA',  # Europe, Middle East, Africa
            'FR': 'EMEA',
            'DE': 'EMEA',
            'IN': 'APAC',  # Asia Pacific
            'JP': 'APAC',
            'KR': 'APAC'
        }

        df['timezone_group'] = df['country_code'].map(timezone_map)

        logger.info(f"    ✅ Added {3} geographic features")
        return df


# Example usage
if __name__ == "__main__":
    # Load cleaned data from validation step
    df = pd.read_csv('../../data/streaming_telemetry_clean.csv')

    # Transform it
    transformer = TelemetryTransformer()
    transformed_df = transformer.transform_all(df)

    # Show sample of new features
    print("\n Sample of transformed data:")
    new_columns = [
        'timestamp', 'device_type', 'time_of_day', 'is_peak_time',
        'rebuffer_ratio', 'overall_qoe_score', 'session_quality',
        'buffering_severity', 'region'
    ]
    print(transformed_df[new_columns].head(10))

    # Show distribution of session quality
    print("\n Session Quality Distribution:")
    print(transformed_df['session_quality'].value_counts(normalize=True).mul(100).round(1))

    # Save transformed data
    transformed_df.to_csv('../../data/streaming_telemetry_transformed.csv', index=False)
    print("\n Saved transformed data to 'streaming_telemetry_transformed.csv'")
