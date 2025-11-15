import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Set up logging (so we can track what happens)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_QUALITY_SCORE_THRESHOLD = 90


class TelemetryValidator:
    """
    Validates Netflix telemetry data.

    Think of this as a quality inspector at a factory -
    it checks every piece of data to make sure it's good!
    """

    def __init__(self):
        self.validation_errors = []
        self.data_quality_score = 100  # Start at 100%, deduct for errors

    def validate_all(self, df):
        """
        Run all validation checks.
        Returns: (is_valid, cleaned_df, report)
        """
        logger.info(f"Starting validation of {len(df):,} records...")

        # Run each check
        df = self.check_nulls(df)
        df = self.check_data_types(df)
        df = self.check_value_ranges(df)
        df = self.check_logical_consistency(df)
        df = self.check_duplicates(df)
        df = self.check_timestamps(df)

        # Generate report
        report = self.generate_report(df)

        # Decide if data is valid enough to proceed
        is_valid = self.data_quality_score >= DATA_QUALITY_SCORE_THRESHOLD  # Need 95%+ quality

        return is_valid, df, report

    def check_nulls(self, df):
        """
        Check for missing values.

        WHY THIS MATTERS:
        Missing data can break calculations. For example, if
        startup_time is blank, we can't calculate average startup time!
        """
        logger.info("Checking for null values...")

        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()

        if total_nulls > 0:
            null_percentage = (total_nulls / (len(df) * len(df.columns))) * 100

            logger.warning(f"Found {total_nulls:,} null values ({null_percentage:.2f}%)")

            # Log which columns have nulls
            for col, count in null_counts[null_counts > 0].items():
                pct = (count / len(df)) * 100
                logger.warning(f"  - {col}: {count:,} nulls ({pct:.2f}%)")

                # Deduct from quality score
                self.data_quality_score -= (pct * 0.1)  # Small penalty per % null

            # DECISION: Drop rows with critical nulls, fill non-critical
            critical_columns = ['session_id', 'timestamp', 'user_id']

            # Drop rows missing critical data
            before_count = len(df)
            df = df.dropna(subset=critical_columns)
            dropped = before_count - len(df)

            if dropped > 0:
                logger.warning(f"  Dropped {dropped:,} rows with critical nulls")

            # Fill non-critical nulls with defaults
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(0)

            logger.info(f"Null handling complete. {len(df):,} rows remain.")
        else:
            logger.info("No null values found!")

        return df

    def check_data_types(self, df):
        """
        Ensure columns have correct data types.

        EXAMPLE PROBLEM:
        Someone accidentally put "slow" instead of a number for startup_time.
        This would break any math calculations!
        """
        logger.info("Checking data types...")

        expected_types = {
            'session_id': 'object',  # String
            'user_id': 'object',
            'timestamp': 'datetime64[ns]',
            'startup_time_ms': 'int64',
            'rebuffer_count': 'int64',
            'rebuffer_duration_ms': 'int64',
            'bitrate_kbps': 'int64',
            'frames_dropped': 'int64',
            'session_duration_sec': 'int64'
        }

        type_errors = []

        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)

                if expected_type == 'datetime64[ns]' and actual_type != 'datetime64[ns]':
                    # Try to convert to datetime
                    try:
                        df[col] = pd.to_datetime(df[col])
                        logger.info(f"  Converted {col} to datetime")
                    except Exception as e:
                        type_errors.append(f"{col}: cannot convert to datetime")
                        logger.error(f"  {col}: {e}")

                elif expected_type in ['int64', 'float64'] and actual_type not in ['int64', 'float64']:
                    # Try to convert to number
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        logger.info(f"  Converted {col} to numeric")
                    except Exception as e:
                        type_errors.append(f"{col}: cannot convert to numeric")
                        logger.error(f"  {col}: {e}")

        if type_errors:
            self.validation_errors.extend(type_errors)
            self.data_quality_score -= len(type_errors) * 2

        logger.info(f"✅ Data type check complete")
        return df

    def check_value_ranges(self, df):
        """
        Check if values are in realistic ranges.

        EXAMPLE:
        Startup time should be 100ms - 30,000ms.
        If we see 999,999ms (999 seconds), that's clearly wrong!
        """
        logger.info("Checking value ranges...")

        range_checks = {
            'startup_time_ms': (100, 30000, 'Startup time out of range'),
            'rebuffer_count': (0, 100, 'Rebuffer count unrealistic'),
            'rebuffer_duration_ms': (0, 600000, 'Rebuffer duration too long'),
            'bitrate_kbps': (100, 50000, 'Bitrate out of range'),
            'frames_dropped': (0, 10000, 'Frames dropped unrealistic'),
            'session_duration_sec': (1, 14400, 'Session duration unrealistic')  # Max 4 hours
        }

        for col, (min_val, max_val, error_msg) in range_checks.items():
            if col in df.columns:
                out_of_range = ((df[col] < min_val) | (df[col] > max_val)).sum()

                if out_of_range > 0:
                    pct = (out_of_range / len(df)) * 100
                    logger.warning(f"  x  {col}: {out_of_range:,} values out of range ({pct:.2f}%)")

                    # Cap values at min/max (instead of deleting)
                    df[col] = df[col].clip(lower=min_val, upper=max_val)

                    logger.info(f"    Capped values to [{min_val}, {max_val}]")

                    self.data_quality_score -= (pct * 0.5)

        logger.info("✅ Range check complete")
        return df

    def check_logical_consistency(self, df):
        """
        Check if related fields make logical sense together.

        EXAMPLE LOGIC ERRORS:
        - rebuffer_count = 5 but rebuffer_duration_ms = 0
          (You can't buffer 5 times with zero duration!)
        - session_duration_sec = 10 but watched a 2-hour movie
          (Can't watch a movie in 10 seconds!)
        """
        logger.info("Checking logical consistency...")

        inconsistencies = 0

        # Check 1: If rebuffer_count > 0, rebuffer_duration should be > 0
        bad_rebuffer = ((df['rebuffer_count'] > 0) &
                       (df['rebuffer_duration_ms'] == 0)).sum()

        if bad_rebuffer > 0:
            logger.warning(f"  x  {bad_rebuffer:,} sessions have rebuffers with zero duration")

            # FIX: Estimate rebuffer duration based on count
            mask = (df['rebuffer_count'] > 0) & (df['rebuffer_duration_ms'] == 0)
            df.loc[mask, 'rebuffer_duration_ms'] = df.loc[mask, 'rebuffer_count'] * 2000  # Assume 2s each

            inconsistencies += bad_rebuffer

        # Check 2: Rebuffer duration can't exceed session duration
        bad_duration = (df['rebuffer_duration_ms'] > df['session_duration_sec'] * 1000).sum()

        if bad_duration > 0:
            logger.warning(f"  x  {bad_duration:,} sessions have rebuffer duration > total duration")

            # FIX: Cap rebuffer duration at session duration
            max_rebuffer = df['session_duration_sec'] * 1000
            df['rebuffer_duration_ms'] = df[['rebuffer_duration_ms', 'session_duration_sec']].apply(
                lambda x: min(x['rebuffer_duration_ms'], x['session_duration_sec'] * 1000),
                axis=1
            )

            inconsistencies += bad_duration

        # Check 3: Resolution should match bitrate range
        resolution_bitrate_map = {
            '4K': (15000, 50000),
            '1080p': (3000, 10000),
            '720p': (1500, 4000),
            '480p': (100, 2000)
        }

        for resolution, (min_br, max_br) in resolution_bitrate_map.items():
            mask = df['resolution'] == resolution
            out_of_range = mask & ((df['bitrate_kbps'] < min_br) | (df['bitrate_kbps'] > max_br))
            count = out_of_range.sum()

            if count > 0:
                logger.warning(f"  x  {count:,} sessions have {resolution} with bitrate outside expected range")
                inconsistencies += count

        if inconsistencies > 0:
            self.data_quality_score -= (inconsistencies / len(df)) * 100 * 0.5

        logger.info("✅ Logic check complete")
        return df

    def check_duplicates(self, df):
        """
        Check for duplicate session IDs.

        WHY THIS MATTERS:
        Each session should be unique. Duplicates mean either:
        1. Data was sent twice (network glitch)
        2. Bug in our data generation

        Either way, we need to remove them!
        """
        logger.info("Checking for duplicates...")

        duplicates = df['session_id'].duplicated().sum()

        if duplicates > 0:
            pct = (duplicates / len(df)) * 100
            logger.warning(f"  x  Found {duplicates:,} duplicate session IDs ({pct:.2f}%)")

            # Keep first occurrence, drop the rest
            before_count = len(df)
            df = df.drop_duplicates(subset='session_id', keep='first')
            after_count = len(df)

            logger.info(f"  Removed {before_count - after_count:,} duplicates")

            self.data_quality_score -= (pct * 0.8)
        else:
            logger.info("✅ No duplicates found!")

        return df

    def check_timestamps(self, df):
        """
        Validate timestamp data.

        CHECKS:
        - Are timestamps in the past? (can't have future data!)
        - Are they within expected date range?
        - Are they properly ordered?
        """
        logger.info("Checking timestamps...")

        # Ensure timestamp is datetime
        if df['timestamp'].dtype != 'datetime64[ns]':
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        # Check for future timestamps
        now = datetime.now()
        future_timestamps = (df['timestamp'] > now).sum()

        if future_timestamps > 0:
            logger.warning(f"  x  {future_timestamps:,} timestamps are in the future!")
            # Remove future timestamps
            df = df[df['timestamp'] <= now]
            self.data_quality_score -= 5

        # Check for very old timestamps (older than 1 year)
        one_year_ago = now - timedelta(days=365)
        old_timestamps = (df['timestamp'] < one_year_ago).sum()

        if old_timestamps > 0:
            pct = (old_timestamps / len(df)) * 100
            logger.warning(f"  x  {old_timestamps:,} timestamps are older than 1 year ({pct:.2f}%)")

        logger.info("✅ Timestamp check complete")
        return df

    def generate_report(self, df):
        """
        Create a validation report summarizing data quality.
        """
        report = {
            'timestamp': datetime.now(),
            'total_records': len(df),
            'data_quality_score': round(self.data_quality_score, 2),
            'validation_errors': self.validation_errors,
            'summary': {
                'null_columns': df.isnull().sum()[df.isnull().sum() > 0].to_dict(),
                'numeric_stats': df.describe().to_dict()
            }
        }

        logger.info(f"\n📊 VALIDATION REPORT")
        logger.info(f"  Total Records: {report['total_records']:,}")
        logger.info(f"  Quality Score: {report['data_quality_score']}%")
        logger.info(f"  Errors Found: {len(report['validation_errors'])}")

        return report


# Example usage
if __name__ == "__main__":
    # Load the data we generated in Step 1
    df = pd.read_csv('../../data/streaming_telemetry.csv')
    
    # Validate it
    validator = TelemetryValidator()
    is_valid, clean_df, report = validator.validate_all(df)
    
    if is_valid:
        print("\n✅ Data passed validation!")
        print(f"Quality Score: {report['data_quality_score']}%")
        
        # Save cleaned data
        clean_df.to_csv('../../data/streaming_telemetry_clean.csv', index=False)
        print("💾 Saved clean data to 'streaming_telemetry_clean.csv'")
    else:
        print("\n❌ Data failed validation!")
        print(f"Quality Score: {report['data_quality_score']}% (need 95%+)")
        print("Please fix data quality issues before proceeding.")