-- ============================================
-- STREAMING SERVICE QOE DATA WAREHOUSE SCHEMA
-- ============================================
-- This creates our "organized library" structure
-- for analyzing the streaming quality of our service

-- Drop existing tables if they exist (for clean reruns)
DROP TABLE IF EXISTS fact_playback_sessions CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_device CASCADE;
DROP TABLE IF EXISTS dim_user_cohort CASCADE;
DROP TABLE IF EXISTS dim_geography CASCADE;
DROP TABLE IF EXISTS dim_content CASCADE;
DROP TABLE IF EXISTS dim_network CASCADE;

-- ============================================
-- DIMENSION TABLES (The "Who/What/When/Where")
-- ============================================

-- DIM_DATE: Calendar information
-- WHY: Lets us analyze by day, week, month, quarter, year
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,  -- Format: YYYYMMDD (e.g., 20251010)
    full_date DATE NOT NULL,
    day_of_week INTEGER,  -- 0=Sunday, 6=Saturday
    day_name VARCHAR(10),  -- 'Monday', 'Tuesday', etc.
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,  -- 1, 2, 3, or 4
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,  -- Could add holiday calendar
    UNIQUE(full_date)
);

-- Example row in dim_date:
-- date_key: 20251010
-- full_date: 2025-10-10
-- day_of_week: 5 (Friday)
-- day_name: 'Friday'
-- is_weekend: true


-- DIM_TIME: Time of day information
-- WHY: Analyze by hour, peak times, etc.
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,  -- Format: HHMM (e.g., 1430 for 2:30 PM)
    hour INTEGER,  -- 0-23
    minute INTEGER,  -- 0-59
    time_of_day VARCHAR(20),  -- 'morning', 'afternoon', 'evening', 'night'
    is_peak_time BOOLEAN,  -- True if 7pm-11pm
    hour_label VARCHAR(10)  -- '2:00 PM', '14:00', etc.
);

-- Example row in dim_time:
-- time_key: 1430
-- hour: 14
-- minute: 30
-- time_of_day: 'afternoon'
-- is_peak_time: false


-- DIM_DEVICE: Device information
-- WHY: Compare performance across device types, OS versions, app versions
CREATE TABLE dim_device (
    device_key SERIAL PRIMARY KEY,  -- Auto-incrementing ID
    device_type VARCHAR(50),  -- 'smart_tv', 'mobile', 'web', 'tablet'
    device_family VARCHAR(50),  -- 'TV', 'Mobile', 'Desktop'
    manufacturer VARCHAR(100),  -- 'Samsung', 'Apple', 'LG', etc.
    model VARCHAR(100),  -- 'Galaxy S23', 'iPhone 15', etc.
    os_name VARCHAR(50),  -- 'iOS', 'Android', 'webOS', 'Windows'
    os_version VARCHAR(50),  -- '16.3', '13.0', etc.
    app_version VARCHAR(50),  -- 'v17.2.1'
    screen_size VARCHAR(20),  -- 'small', 'medium', 'large'
    supports_4k BOOLEAN,  -- Can this device play 4K?

    -- Composite key for finding existing devices
    UNIQUE(device_type, os_version, app_version)
);

-- Example row in dim_device:
-- device_key: 123
-- device_type: 'mobile'
-- device_family: 'Mobile'
-- manufacturer: 'Apple'
-- model: 'iPhone 15'
-- os_name: 'iOS'
-- os_version: '17.1'
-- app_version: 'v17.2.1'


-- DIM_USER_COHORT: User segmentation
-- WHY: Analyze by user type without storing PII (Personal Identifiable Information)
-- NOTE: We DON'T store actual user IDs or names (privacy!)
CREATE TABLE dim_user_cohort (
    cohort_key SERIAL PRIMARY KEY,
    cohort_name VARCHAR(100),  -- 'new_users', 'power_users', 'casual_viewers'
    signup_month VARCHAR(7),  -- 'yyyy-mm' format (e.g., '2025-01')
    subscription_tier VARCHAR(50),  -- 'basic', 'standard', 'premium'
    account_age_days INTEGER,  -- How long they've been a member
    avg_viewing_hours_per_week DECIMAL(5,2),  -- Their typical usage

    UNIQUE(cohort_name, signup_month)
);

-- Example row in dim_user_cohort:
-- cohort_key: 5
-- cohort_name: 'power_users'
-- signup_month: '2024-06'
-- subscription_tier: 'premium'
-- account_age_days: 120
-- avg_viewing_hours_per_week: 25.5


-- DIM_GEOGRAPHY: Location information
-- WHY: Analyze by country, region, ISP, CDN
CREATE TABLE dim_geography (
    geo_key SERIAL PRIMARY KEY,
    country_code CHAR(2),  -- 'US', 'BR', etc.
    country_name VARCHAR(100),  -- 'United States', 'Brazil'
    region VARCHAR(100),  -- 'North America', 'South America', 'Europe'
    timezone_group VARCHAR(50),  -- 'Americas', 'EMEA', 'APAC'
    market_maturity VARCHAR(20),  -- 'mature', 'growing', 'emerging'
    isp VARCHAR(100),  -- Internet Service Provider
    cdn_pop VARCHAR(50),  -- CDN Point of Presence (e.g., 'US-East-1')

    UNIQUE(country_code, isp, cdn_pop)
);

-- Example row in dim_geography:
-- geo_key: 42
-- country_code: 'BR'
-- country_name: 'Brazil'
-- region: 'South America'
-- timezone_group: 'Americas'
-- market_maturity: 'growing'
-- isp: 'Comcast'
-- cdn_pop: 'BR-2'


-- DIM_CONTENT: Content metadata
-- WHY: Analyze quality by content type, genre, duration
CREATE TABLE dim_content (
    content_key SERIAL PRIMARY KEY,
    content_id VARCHAR(100),  -- Netflix's internal content ID
    content_type VARCHAR(50),  -- 'movie', 'episode', 'documentary'
    genre VARCHAR(100),  -- 'Action', 'Comedy', 'Drama', etc.
    duration_minutes INTEGER,  -- Length of content
    release_year INTEGER,
    is_original BOOLEAN,  -- Netflix Original content?
    video_codec VARCHAR(50),  -- 'H.264', 'H.265', 'AV1'
    max_resolution VARCHAR(10),  -- Highest quality available

    UNIQUE(content_id)
);

-- Example row in dim_content:
-- content_key: 789
-- content_id: 'content_42'
-- content_type: 'episode'
-- genre: 'Drama'
-- duration_minutes: 58
-- is_original: true
-- max_resolution: '4K'


-- DIM_NETWORK: Network conditions
-- WHY: Understand how network quality affects QoE
CREATE TABLE dim_network (
    network_key SERIAL PRIMARY KEY,
    network_type VARCHAR(50),  -- 'wifi', 'cellular_5g', 'cellular_4g', 'ethernet'
    network_quality VARCHAR(20),  -- 'excellent', 'good', 'fair', 'poor'
    estimated_bandwidth_mbps DECIMAL(8,2),  -- Estimated speed
    is_metered BOOLEAN,  -- Does user pay per GB?

    UNIQUE(network_type, network_quality)
);

-- Example row in dim_network:
-- network_key: 15
-- network_type: 'wifi'
-- network_quality: 'good'
-- estimated_bandwidth_mbps: 50.00
-- is_metered: false


-- ============================================
-- FACT TABLE (The Measurements/Metrics)
-- ============================================

CREATE TABLE fact_playback_sessions (
    -- Primary Key
    session_key BIGSERIAL PRIMARY KEY,  -- Unique ID for this fact row
    session_id VARCHAR(100) NOT NULL,  -- Original session ID from telemetry

    -- Foreign Keys (links to dimension tables)
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    time_key INTEGER NOT NULL REFERENCES dim_time(time_key),
    device_key INTEGER NOT NULL REFERENCES dim_device(device_key),
    cohort_key INTEGER REFERENCES dim_user_cohort(cohort_key),
    geo_key INTEGER NOT NULL REFERENCES dim_geography(geo_key),
    content_key INTEGER REFERENCES dim_content(content_key),
    network_key INTEGER REFERENCES dim_network(network_key),

    -- Timestamp (for exact queries)
    session_timestamp TIMESTAMP NOT NULL,

    -- PERFORMANCE METRICS (the numbers we want to analyze!)

    -- Startup Performance
    startup_time_ms INTEGER,  -- Milliseconds to first frame
    startup_category VARCHAR(20),  -- 'excellent', 'good', 'fair', 'poor'

    -- Buffering/Rebuffering
    rebuffer_count INTEGER,  -- Number of buffering events
    rebuffer_duration_ms INTEGER,  -- Total time spent buffering
    rebuffer_ratio DECIMAL(5,2),  -- Percentage of session spent buffering
    buffering_severity VARCHAR(20),  -- 'none', 'minor', 'moderate', 'severe'

    -- Video Quality
    avg_bitrate_kbps INTEGER,  -- Average bitrate delivered
    min_bitrate_kbps INTEGER,  -- Lowest bitrate during session
    max_bitrate_kbps INTEGER,  -- Highest bitrate during session
    resolution VARCHAR(10),  -- '4K', '1080p', '720p', '480p'
    quality_score INTEGER,  -- 0-100 score based on bitrate

    -- Playback Metrics
    frames_dropped INTEGER,  -- Rendering performance
    session_duration_sec INTEGER,  -- How long they watched
    playback_duration_sec INTEGER,  -- Actual playback time (minus buffering)

    -- Overall Quality
    overall_qoe_score DECIMAL(5,2),  -- 0-100 overall quality score
    session_quality VARCHAR(20),  -- 'excellent', 'good', 'fair', 'poor'

    -- Flags
    video_start_failure BOOLEAN DEFAULT false,  -- Did video fail to start?
    error_occurred BOOLEAN DEFAULT false,  -- Any errors during session?

    -- Constraints
    UNIQUE(session_id)  -- Each session appears only once
);

-- ============================================
-- INDEXES (Make Queries Fast!)
-- ============================================
-- Think of indexes like a book's index - they help you find things quickly

-- Why indexes matter:
-- Without index: Database scans ALL rows to find what you want (slow!)
-- With index: Database jumps directly to the right rows (fast!)

-- Date indexes (most queries filter by date)
CREATE INDEX idx_fact_date ON fact_playback_sessions(date_key);
CREATE INDEX idx_fact_timestamp ON fact_playback_sessions(session_timestamp);

-- Device indexes (analyze by device type)
CREATE INDEX idx_fact_device ON fact_playback_sessions(device_key);

-- Geography indexes (analyze by location)
CREATE INDEX idx_fact_geo ON fact_playback_sessions(geo_key);

-- Quality indexes (find poor quality sessions)
CREATE INDEX idx_fact_qoe_score ON fact_playback_sessions(overall_qoe_score);
CREATE INDEX idx_fact_session_quality ON fact_playback_sessions(session_quality);

-- Composite indexes (queries that filter on multiple columns)
CREATE INDEX idx_fact_date_device ON fact_playback_sessions(date_key, device_key);
CREATE INDEX idx_fact_date_geo ON fact_playback_sessions(date_key, geo_key);

-- ============================================
-- VIEWS (Pre-made queries for common analyses)
-- ============================================
-- Views are like saved queries - they make complex queries simple

-- View: Daily quality summary
CREATE VIEW vw_daily_quality_summary AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(*) as total_sessions,
    AVG(f.overall_qoe_score) as avg_qoe_score,
    AVG(f.startup_time_ms) as avg_startup_ms,
    AVG(f.rebuffer_ratio) as avg_rebuffer_ratio,
    SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END) as poor_quality_sessions,
    SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as poor_quality_pct
FROM fact_playback_sessions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date DESC;

-- View: Device performance comparison
CREATE VIEW vw_device_performance AS
SELECT
    dev.device_family,
    dev.device_type,
    dev.os_name,
    COUNT(*) as session_count,
    AVG(f.startup_time_ms) as avg_startup_ms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.startup_time_ms) as median_startup_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.startup_time_ms) as p95_startup_ms,
    AVG(f.overall_qoe_score) as avg_qoe_score,
    AVG(f.rebuffer_count) as avg_rebuffer_count
FROM fact_playback_sessions f
JOIN dim_device dev ON f.device_key = dev.device_key
GROUP BY dev.device_family, dev.device_type, dev.os_name
ORDER BY session_count DESC;

-- View: Geographic quality heatmap data
CREATE VIEW vw_geographic_quality AS
SELECT
    g.country_name,
    g.region,
    g.market_maturity,
    COUNT(*) as session_count,
    AVG(f.overall_qoe_score) as avg_qoe_score,
    AVG(f.startup_time_ms) as avg_startup_ms,
    AVG(f.avg_bitrate_kbps) as avg_bitrate_kbps
FROM fact_playback_sessions f
JOIN dim_geography g ON f.geo_key = g.geo_key
GROUP BY g.country_name, g.region, g.market_maturity
ORDER BY session_count DESC;

-- View: Peak time analysis
CREATE VIEW vw_peak_time_analysis AS
SELECT
    t.hour,
    t.time_of_day,
    t.is_peak_time,
    COUNT(*) as session_count,
    AVG(f.overall_qoe_score) as avg_qoe_score,
    AVG(f.startup_time_ms) as avg_startup_ms,
    AVG(f.rebuffer_ratio) as avg_rebuffer_ratio
FROM fact_playback_sessions f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.hour, t.time_of_day, t.is_peak_time
ORDER BY t.hour;

-- ============================================
-- COMMENTS (Documentation)
-- ============================================
-- Add descriptions to tables so others understand them

COMMENT ON TABLE fact_playback_sessions IS 'Core fact table containing streaming service playback session metrics';
COMMENT ON TABLE dim_date IS 'Date dimension for time-based analysis';
COMMENT ON TABLE dim_device IS 'Device dimension including hardware, OS, and app version';
COMMENT ON TABLE dim_geography IS 'Geographic dimension with country, region, ISP, and CDN data';

COMMENT ON COLUMN fact_playback_sessions.overall_qoe_score IS 'Composite quality score (0-100) based on startup, buffering, and bitrate';
COMMENT ON COLUMN fact_playback_sessions.rebuffer_ratio IS 'Percentage of session spent buffering (0-100)';
