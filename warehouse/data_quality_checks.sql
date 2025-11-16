-- ============================================
-- DATA QUALITY CHECKS FOR DATA WAREHOUSE
-- ============================================

-- CHECK 1: Referential Integrity
-- Ensure all fact records have valid dimension keys
-- ============================================

SELECT 'Orphaned Device Keys' as check_name, COUNT(*) as issues
FROM fact_playback_sessions f
LEFT JOIN dim_device d ON f.device_key = d.device_key
WHERE d.device_key IS NULL

UNION ALL

SELECT 'Orphaned Geography Keys', COUNT(*)
FROM fact_playback_sessions f
LEFT JOIN dim_geography g ON f.geo_key = g.geo_key
WHERE g.geo_key IS NULL

UNION ALL

SELECT 'Orphaned Date Keys', COUNT(*)
FROM fact_playback_sessions f
LEFT JOIN dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;


-- CHECK 2: Data Completeness
-- Ensure critical fields are populated
-- ============================================

SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_playback_sessions

UNION ALL

SELECT
    'Records with NULL startup_time',
    COUNT(*)
FROM fact_playback_sessions
WHERE startup_time_ms IS NULL

UNION ALL

SELECT
    'Records with NULL QoE score',
    COUNT(*)
FROM fact_playback_sessions
WHERE overall_qoe_score IS NULL;


-- CHECK 3: Data Reasonableness
-- Check for impossible or suspicious values
-- ============================================

SELECT 'Negative Startup Times' as issue, COUNT(*) as count
FROM fact_playback_sessions
WHERE startup_time_ms < 0

UNION ALL

SELECT 'Startup Time > 1 minute', COUNT(*)
FROM fact_playback_sessions
WHERE startup_time_ms > 60000

UNION ALL

SELECT 'Rebuffer Count > 100', COUNT(*)
FROM fact_playback_sessions
WHERE rebuffer_count > 100

UNION ALL

SELECT 'QoE Score > 100', COUNT(*)
FROM fact_playback_sessions
WHERE overall_qoe_score > 100

UNION ALL

SELECT 'Session Duration > 24 hours', COUNT(*)
FROM fact_playback_sessions
WHERE session_duration_sec > 86400;


-- CHECK 4: Duplicate Detection
-- ============================================

SELECT
    'Duplicate Session IDs' as check_name,
    COUNT(*) - COUNT(DISTINCT session_id) as duplicates
FROM fact_playback_sessions;


-- CHECK 5: Date Continuity
-- Ensure we have data for every expected date
-- ============================================

WITH date_range AS (
    SELECT generate_series(
        (SELECT MIN(full_date) FROM dim_date WHERE full_date >= CURRENT_DATE - 30),
        CURRENT_DATE,
        '1 day'::interval
    )::date as expected_date
),
actual_dates AS (
    SELECT DISTINCT d.full_date
    FROM fact_playback_sessions f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - 30
)
SELECT
    'Missing Dates in Last 30 Days' as check_name,
    COUNT(*) as missing_days
FROM date_range dr
LEFT JOIN actual_dates ad ON dr.expected_date = ad.full_date
WHERE ad.full_date IS NULL;


-- CHECK 6: Dimension Cardinality
-- How many unique values in each dimension?
-- ============================================

SELECT 'dim_date' as dimension, COUNT(*) as row_count FROM dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL
SELECT 'dim_device', COUNT(*) FROM dim_device
UNION ALL
SELECT 'dim_geography', COUNT(*) FROM dim_geography
UNION ALL
SELECT 'dim_content', COUNT(*) FROM dim_content
UNION ALL
SELECT 'dim_network', COUNT(*) FROM dim_network
UNION ALL
SELECT 'dim_user_cohort', COUNT(*) FROM dim_user_cohort
UNION ALL
SELECT 'fact_playback_sessions', COUNT(*) FROM fact_playback_sessions;
