import pandas as pd
import numpy as np 
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid

# initialize faker
fake = Faker()

################################# CONFIGURATION SETTINGS #################################

# How many viewing sessions to generate
NUM_SESSIONS = 100000   # Number of sessions to generate

# What devices do people use
DEVICE_TYPES = ['smart_tv', 'mobile', 'web', 'tablet']

# Device probabilities (smart TVs are most popular for this streaming service)
DEVICE_WEIGHTS = [0.45, 0.30, 0.15, 0.10]  # weights must add up to 1.0

# Network Types
NETWORK_TYPES = ['wifi', 'cellular_5g', 'cellular_4g', 'ethernet']

# Countries (top markets for this streaming service)
COUNTRIES = ['US', 'BR', 'IN', 'GB', 'FR', 'DE', 'MX', 'JP', 'KR']

# Content IDs (show or movie IDs)
CONTENT_IDS = [f"content_{i}" for i in range(1, 1001)] # 1000 shows/movies

# ISPs (top ISPs for this streaming service)
ISPS = ['comcast', 'verizon', 'att', 'charter', 'cox', 'vodafone', 'bt', 'orange']



################################# HELPER FUNCTIONS #################################

def generate_session_id():
    """
    Create a unique ID for each viewing session.
    UUID = Universally Unique Identifier (basically impossible to duplicate)
    """
    return str(uuid.uuid4())

def generate_user_id():
    """
    Create a unique user ID.
    In reality, Netflix has 300M users, we'll simulate 1M
    """
    return f'user_{random.randint(1, 1000000)}'

def generate_timestamp(start_date, end_date):
    """
    Create a random timestamp between two dates.
    This makes our data look like it happened over time (not all at once)
    """
    time_between = end_date - start_date
    random_seconds = random.randint(0, int(time_between.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)

def generate_startup_time(device_type, network_type):
    """
    Generate realistic startup time in milliseconds.

    WHY DIFFERENT TIMES?
    - Smart TVs are slower (bigger, more processing)
    - 5G is faster than 4G
    - Good WiFi is fast, bad WiFi is slow

    We use normal distribution (bell curve) because most times are average,
    some are fast, some are slow - just like real life!
    """
    # Base startup times by device (in milliseconds)
    base_times = {
        'smart_tv': 2000,      # 2 seconds average
        'mobile': 1200,        # 1.2 seconds
        'web': 1500,          # 1.5 seconds
        'tablet': 1300        # 1.3 seconds
    }

    # Network impact (multiplier)
    network_multipliers = {
        'ethernet': 0.7,      # Wired is fastest
        'wifi': 1.0,          # WiFi is baseline
        'cellular_5g': 1.2,   # 5G is pretty good
        'cellular_4g': 1.8    # 4G is slower
    }

    base = base_times[device_type]
    multiplier = network_multipliers[network_type]

    # Add randomness using normal distribution
    # np.random.normal(mean, standard_deviation)
    # Standard deviation = how spread out the values are
    startup_time = np.random.normal(base * multiplier, base * 0.3)

    # Make sure it's not negative (impossible!) and at least 100ms
    return max(100, int(startup_time))

def generate_rebuffer_events(network_type, session_duration_sec):
    """
    Generate buffering events - those annoying pauses!

    WHY THIS MATTERS:
    - Bad networks = more buffering
    - Longer sessions = more chances to buffer
    """
    # Probability of buffering per minute of viewing
    rebuffer_rates = {
        'ethernet': 0.01,     # Almost never buffers
        'wifi': 0.05,         # Sometimes buffers
        'cellular_5g': 0.10,  # Buffers occasionally
        'cellular_4g': 0.25   # Buffers often (frustrating!)
    }

    rate = rebuffer_rates[network_type]
    viewing_minutes = session_duration_sec / 60

    # Poisson distribution - models random events over time
    # (like how many cars pass by in an hour)
    rebuffer_count = np.random.poisson(rate * viewing_minutes)

    # Each rebuffer event lasts 1-5 seconds
    if rebuffer_count > 0:
        rebuffer_duration = sum([random.randint(1000, 5000)
                                for _ in range(rebuffer_count)])
    else:
        rebuffer_duration = 0

    return rebuffer_count, rebuffer_duration

def generate_bitrate(network_type):
    """
    Generate video quality (bitrate) based on network speed.

    BITRATE EXPLAINED:
    Think of bitrate like water pressure in a hose:
    - High pressure (bitrate) = clear, beautiful picture
    - Low pressure (bitrate) = blurry, pixelated picture

    Netflix automatically adjusts quality based on your internet speed!
    """
    # Quality tiers with their bitrates (kbps)
    quality_tiers = {
        'ethernet': {
            '4K': (20000, 25000),       # Amazing quality
            '1080p': (5000, 8000),      # HD
            '720p': (2500, 4000),       # Good
            '480p': (1000, 2000)        # OK
        },
        'wifi': {
            '4K': (18000, 23000),
            '1080p': (4500, 7000),
            '720p': (2000, 3500),
            '480p': (800, 1500)
        },
        'cellular_5g': {
            '1080p': (4000, 6000),
            '720p': (2000, 3000),
            '480p': (800, 1200)
        },
        'cellular_4g': {
            '720p': (1500, 2500),
            '480p': (500, 1000)
        }
    }

    # Pick a quality tier for this network
    available_qualities = list(quality_tiers[network_type].keys())
    quality = random.choice(available_qualities)

    # Get bitrate range and pick random value in that range
    bitrate_range = quality_tiers[network_type][quality]
    bitrate = random.randint(bitrate_range[0], bitrate_range[1])

    return bitrate, quality

def inject_realistic_bugs(df):
    """
    Add realistic bugs that platform engineers would need to find!
    """
    print("\n🐛 Injecting realistic bugs...")

    # Bug 1: iOS 15.3 has 2x slower startup on iPhones
    ios_15_3_mask = df['os_version'].str.contains('iPhone', na=False) & \
                    (df['device_type'] == 'mobile')

    # 20% of mobile users have this iOS version
    ios_15_3_users = df[ios_15_3_mask].sample(frac=0.20)

    # Double their startup time
    df.loc[ios_15_3_users.index, 'startup_time_ms'] *= 2

    print(f"  - Added iOS 15.3 bug affecting {len(ios_15_3_users):,} sessions")

    # Bug 2: Certain content (content_42) has encoding issues on Smart TVs
    problem_content = (df['content_id'] == 'content_42') & \
                      (df['device_type'] == 'smart_tv')

    # These sessions buffer way more
    df.loc[problem_content, 'rebuffer_count'] *= 3
    df.loc[problem_content, 'rebuffer_duration_ms'] *= 3

    print(f"  - Added content encoding bug affecting {problem_content.sum():,} sessions")

    # Bug 3: Brazilian ISP "BrazilNet" has routing issues
    # (higher latency = slower startup)
    brazil_isp_issue = (df['country_code'] == 'BR') & \
                       (df['isp'] == 'Comcast')  # Pretend "Comcast" is "BrazilNet" in Brazil

    df.loc[brazil_isp_issue, 'startup_time_ms'] += 1500  # Add 1.5 second delay

    print(f"  - Added ISP routing bug affecting {brazil_isp_issue.sum():,} sessions")

    return df

def add_realistic_time_patterns(df):
    """
    Make viewing patterns match real user behavior.
    Peak hours: 7pm-11pm
    Peak days: Friday-Sunday
    """
    print("\n📅 Adding time-based viewing patterns...")

    # Add hour and day of week
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday

    # Weight sessions toward peak times
    # We'll do this by duplicating some sessions during peak hours
    peak_hours = (df['hour'] >= 19) & (df['hour'] <= 23)
    weekend = df['day_of_week'].isin([5, 6])  # Friday, Saturday

    # Sessions during peak times get duplicated (simulating more viewers)
    peak_sessions = df[peak_hours | weekend].sample(frac=0.3)

    print(f"  - Added {len(peak_sessions):,} additional peak-time sessions")

    # Combine original + peak duplicates
    df_enhanced = pd.concat([df, peak_sessions], ignore_index=True)
    df_enhanced = df_enhanced.sort_values('timestamp').reset_index(drop=True)

    return df_enhanced

def validate_telemetry_data(df):
    """
    Check if our generated data looks realistic.
    This is like quality control!
    """
    print("\n🔍 Validating generated data...\n")

    issues = []

    # Check 1: No missing values
    missing = df.isnull().sum()
    if missing.any():
        issues.append(f"❌ Found missing values:\n{missing[missing > 0]}")
    else:
        print("✅ No missing values")

    # Check 2: Reasonable startup times (should be 100ms to 30 seconds)
    bad_startup = ((df['startup_time_ms'] < 100) |
                   (df['startup_time_ms'] > 30000)).sum()
    if bad_startup > 0:
        issues.append(f"❌ {bad_startup} sessions have unrealistic startup times")
    else:
        print("✅ All startup times are realistic")

    # Check 3: Rebuffer duration makes sense
    # (can't rebuffer for 0ms if rebuffer_count > 0)
    bad_rebuffer = ((df['rebuffer_count'] > 0) &
                    (df['rebuffer_duration_ms'] == 0)).sum()
    if bad_rebuffer > 0:
        issues.append(f"❌ {bad_rebuffer} sessions have rebuffers but no duration")
    else:
        print("✅ Rebuffer logic is consistent")

    # Check 4: Timestamps are in order
    if not df['timestamp'].is_monotonic_increasing:
        issues.append("❌ Timestamps are not in chronological order")
    else:
        print("✅ Timestamps are properly ordered")

    # Check 5: Distribution looks realistic
    print("\n📊 Data Distribution:")
    print(f"  Device Types:\n{df['device_type'].value_counts(normalize=True).mul(100).round(1)}")
    print(f"\n  Network Types:\n{df['network_type'].value_counts(normalize=True).mul(100).round(1)}")
    print(f"\n  Average Metrics:")
    print(f"    Startup Time: {df['startup_time_ms'].mean():.0f}ms (median: {df['startup_time_ms'].median():.0f}ms)")
    print(f"    Rebuffer Rate: {(df['rebuffer_count'] > 0).mean() * 100:.1f}% of sessions had buffering")
    print(f"    Avg Session Duration: {df['session_duration_sec'].mean() / 60:.1f} minutes")

    if issues:
        print("\n⚠️  VALIDATION ISSUES FOUND:")
        for issue in issues:
            print(issue)
        return False
    else:
        print("\n✅ All validation checks passed!")
        return True

################################# MAIN GENERATION FUNCTION #################################

def generate_telemetry_data(num_sessions=NUM_SESSIONS):
    """
    This is the main function that creates all our fake streaming data!
    """
    print(f"🎬 Generating {num_sessions:,} stream viewing sessions...")
    print("This will take about 30 seconds...\n")

    # Create empty list to store all sessions
    sessions = []

    # Date range for our data (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    # Generate each session one by one
    for i in range(num_sessions):
        # Show progress every 10,000 sessions
        if i % 10000 == 0 and i > 0:
            print(f"  Generated {i:,} sessions...")

        # Pick random values for this session
        device_type = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0]
        network_type = random.choice(NETWORK_TYPES)
        country = random.choice(COUNTRIES)

        # Generate realistic session duration (people watch 5-120 minutes)
        session_duration_sec = random.randint(300, 7200)  # 5 min to 2 hours

        # Generate metrics based on conditions
        startup_time = generate_startup_time(device_type, network_type)
        rebuffer_count, rebuffer_duration = generate_rebuffer_events(
            network_type, session_duration_sec
        )
        bitrate, resolution = generate_bitrate(network_type)

        # Calculate frames dropped (better devices drop fewer frames)
        device_quality_factor = {
            'smart_tv': 1.5,
            'mobile': 1.0,
            'web': 1.2,
            'tablet': 1.0
        }
        frames_dropped = int(np.random.poisson(
            rebuffer_count * 10 * device_quality_factor[device_type]
        ))

        # Create the session record (like a row in Excel)
        session = {
            'session_id': generate_session_id(),
            'user_id': generate_user_id(),
            'timestamp': generate_timestamp(start_date, end_date),
            'device_type': device_type,
            'os_version': fake.user_agent(),  # Fake but realistic OS info
            'app_version': f'v{random.randint(15, 18)}.{random.randint(0, 9)}.{random.randint(0, 9)}',
            'content_id': random.choice(CONTENT_IDS),

            # Performance metrics
            'startup_time_ms': startup_time,
            'rebuffer_count': rebuffer_count,
            'rebuffer_duration_ms': rebuffer_duration,
            'bitrate_kbps': bitrate,
            'resolution': resolution,
            'frames_dropped': frames_dropped,
            'session_duration_sec': session_duration_sec,

            # Context
            'network_type': network_type,
            'country_code': country,
            'isp': random.choice(ISPS),
            'cdn_pop': f'{country}-{random.randint(1, 5)}'  # CDN location
        }

        sessions.append(session)

    # Convert list of dictionaries to pandas DataFrame (table)
    df = pd.DataFrame(sessions)

    # Sort by timestamp (chronological order)
    df = df.sort_values('timestamp').reset_index(drop=True)

    print(f"\n✅ Generated {len(df):,} sessions!")
    print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"📊 Data size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

    return df



###################################### RUN THE GENERATOR ######################################

if __name__ == "__main__":
    # Generate the data
    telemetry_df = generate_telemetry_data()

    # Save to CSV file
    output_file = './output/streaming_telemetry.csv'

    # ...but first, inject some realistic bugs and add realistic time patterns!
    telemetry_df = inject_realistic_bugs(telemetry_df)
    telemetry_df = add_realistic_time_patterns(telemetry_df)

    # After generating and enhancing data, validate it
    validate_telemetry_data(telemetry_df)

    telemetry_df.to_csv(output_file, index=False)
    print(f"\n💾 Saved to '{output_file}'")

    # Show some sample data
    print("\n📋 Sample of generated data (first 5 rows):")
    print(telemetry_df.head())

    # Show some statistics
    print("\n📈 Quick Statistics:")
    print(f"  Average startup time: {telemetry_df['startup_time_ms'].mean():.0f} ms")
    print(f"  Average rebuffer count: {telemetry_df['rebuffer_count'].mean():.2f}")
    print(f"  Most common device: {telemetry_df['device_type'].mode()[0]}")
    print(f"  Most common resolution: {telemetry_df['resolution'].mode()[0]}")

