-- ============================================
-- AGGREGATE TABLES FOR FAST DASHBOARDS
-- ============================================

-- Drop existing aggregates
DROP TABLE IF EXISTS agg_daily_quality CASCADE;
DROP TABLE IF EXISTS agg_daily_device_quality CASCADE;
DROP TABLE IF EXISTS agg_daily_geo_quality CASCADE;
DROP TABLE IF EXISTS agg_hourly_quality CASCADE;

-- ============================================
-- Daily Overall Quality Aggregate
-- ============================================

CREATE TABLE agg_daily_quality AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,

    -- Volume
    COUNT(*) as total_sessions,
    SUM(f.session_duration_sec) as total_watch_time_sec,

    -- Startup performance
    AVG(f.startup_time_ms) as avg_startup_ms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.startup_time_ms) as median_startup_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.startup_time_ms) as p95_startup_ms,

    -- Buffering
    AVG(f.rebuffer_count) as avg_rebuffer_count,
    AVG(f.rebuffer_ratio) as avg_rebuffer_ratio,
    SUM(CASE WHEN f.rebuffer_count > 0 THEN 1 ELSE 0 END) as sessions_with_rebuffering,

    -- Video quality
    AVG(f.avg_bitrate_kbps) as avg_bitrate_kbps,

    -- Overall QoE
    AVG(f.overall_qoe_score) as avg_qoe_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.overall_qoe_score) as median_qoe_score,

    -- Quality distribution
    SUM(CASE WHEN f.session_quality = 'excellent' THEN 1 ELSE 0 END) as excellent_sessions,
    SUM(CASE WHEN f.session_quality = 'good' THEN 1 ELSE 0 END) as good_sessions,
    SUM(CASE WHEN f.session_quality = 'fair' THEN 1 ELSE 0 END) as fair_sessions,
    SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END) as poor_sessions,

    -- Calculated metrics
    SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as poor_session_pct,

    -- Metadata
    NOW() as aggregated_at
FROM fact_playback_sessions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.is_weekend;

-- Add primary key and index
ALTER TABLE agg_daily_quality ADD PRIMARY KEY (full_date);
CREATE INDEX idx_agg_daily_date ON agg_daily_quality(full_date DESC);

-- ============================================
-- Daily Device Quality Aggregate
-- ============================================

CREATE TABLE agg_daily_device_quality AS
SELECT
    d.full_date,
    dev.device_type,
    dev.device_family,

    COUNT(*) as total_sessions,
    AVG(f.startup_time_ms) as avg_startup_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.startup_time_ms) as p95_startup_ms,
    AVG(f.rebuffer_count) as avg_rebuffer_count,
    AVG(f.overall_qoe_score) as avg_qoe_score,
    SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as poor_session_pct,

    NOW() as aggregated_at
FROM fact_playback_sessions f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_device dev ON f.device_key = dev.device_key
GROUP BY d.full_date, dev.device_type, dev.device_family;

ALTER TABLE agg_daily_device_quality
ADD PRIMARY KEY (full_date, device_type, device_family);

CREATE INDEX idx_agg_daily_device_date ON agg_daily_device_quality(full_date DESC);

-- ============================================
-- Daily Geographic Quality Aggregate
-- ============================================

CREATE TABLE agg_daily_geo_quality AS
SELECT
    d.full_date,
    g.country_code,
    g.region,

    COUNT(*) as total_sessions,
    AVG(f.startup_time_ms) as avg_startup_ms,
    AVG(f.rebuffer_count) as avg_rebuffer_count,
    AVG(f.avg_bitrate_kbps) as avg_bitrate_kbps,
    AVG(f.overall_qoe_score) as avg_qoe_score,
    SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as poor_session_pct,

    NOW() as aggregated_at
FROM fact_playback_sessions f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_geography g ON f.geo_key = g.geo_key
GROUP BY d.full_date, g.country_code, g.region;

ALTER TABLE agg_daily_geo_quality
ADD PRIMARY KEY (full_date, country_code);

CREATE INDEX idx_agg_daily_geo_date ON agg_daily_geo_quality(full_date DESC);

-- ============================================
-- Hourly Quality Aggregate (for real-time monitoring)
-- ============================================

CREATE TABLE agg_hourly_quality AS
SELECT
    DATE_TRUNC('hour', f.session_timestamp) as hour,

    COUNT(*) as total_sessions,
    AVG(f.startup_time_ms) as avg_startup_ms,
    AVG(f.rebuffer_ratio) as avg_rebuffer_ratio,
    AVG(f.overall_qoe_score) as avg_qoe_score,
    SUM(CASE WHEN f.session_quality = 'poor' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as poor_session_pct,

    NOW() as aggregated_at
FROM fact_playback_sessions f
GROUP BY DATE_TRUNC('hour', f.session_timestamp);

ALTER TABLE agg_hourly_quality ADD PRIMARY KEY (hour);
CREATE INDEX idx_agg_hourly_hour ON agg_hourly_quality(hour DESC);
