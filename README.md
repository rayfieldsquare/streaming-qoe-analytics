
# Streaming Service QoE Analytics
An example project on how to build a telemetry QoE analytics pipeline for a fictional streaming service using Python, Spark, Airflow and Postgres.

# Introduction & Goals
Imagine you're watching your favourite streaming service on your very expensive, super-smart, flat-screen TV. Sometimes it starts playing instantly, sometimes you see a loading spinner. Sometimes the video looks crystal clear, sometimes it's blurry. **This streaming service wants to know WHY these things happen.**

We're going to build a **mini-version of the streaming service's quality monitoring system** - the thing that helps them understand the following:

- How long does it take for shows to start playing?
- How often does the video pause to buffer (that annoying loading spinner)?
- Is the video quality good or bad?
- Which devices have problems?
- Which countries have slow connections?

Think of it like this: If the streaming service is a restaurant, we're building the "customer feedback system" - but instead of comment cards, we're collecting millions of automatic measurements every second.

# Contents

- [The Data Set](#the-data-set)
- [Used Tools](#used-tools)
  - [Connect](#connect)
  - [Buffer](#buffer)
  - [Processing](#processing)
  - [Storage](#storage)
  - [Visualization](#visualization)
- [Pipelines](#pipelines)
  - [Stream Processing](#stream-processing)
    - [Storing Data Stream](#storing-data-stream)
    - [Processing Data Stream](#processing-data-stream)
  - [Batch Processing](#batch-processing)
  - [Visualizations](#visualizations)
- [Demo](#demo)
- [Conclusion](#conclusion)
- [Follow Me On](#follow-me-on)
- [Appendix](#appendix)


# The Data Set

## The Streaming Service QoE Telemetry Generator

### What This Does

Generates realistic streaming telemetry data simulating a streaming service's Quality of Experience (QoE) monitoring system.

### Data Generation Steps

```bash
pip install pandas faker numpy
python generate_telemetry.py
```

### What Gets Generated

- **100,000 viewing sessions** across 30 days
- **Realistic device distribution** (45% Smart TV, 30% Mobile, etc.)
- **Performance metrics** (startup time, buffering, bitrate, resolution)
- **Injected bugs** for realistic troubleshooting scenarios

### Output

The generated data is saved to a CSV file named `streaming_telemetry.csv` in the `output` subdirectory.

#### Sample Output

```csv
📋 Sample of generated data (first 5 rows):
                             session_id      user_id                  timestamp device_type                                         os_version app_version  ... network_type  country_code       isp  cdn_pop  hour day_of_week
0  c69c98ae-efe9-4663-bf63-a59fa9e054eb  user_343299 2025-10-12 13:08:53.966590    smart_tv  Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/53...     v16.2.1  ...         wifi            KR    orange     KR-5    13           6
1  7537ddce-f09e-4684-a2f5-619d597574d1  user_519692 2025-10-12 13:10:11.966590    smart_tv  Mozilla/5.0 (compatible; MSIE 9.0; Windows NT ...     v16.3.9  ...         wifi            KR  vodafone     KR-2    13           6
2  30c1a990-33c6-4dfc-8569-9bbcee9711a5   user_98145 2025-10-12 13:10:27.966590      mobile  Mozilla/5.0 (X11; Linux x86_64; rv:1.9.5.20) G...     v15.6.5  ...         wifi            DE   comcast     DE-1    13           6
3  efba8776-c0b6-41e7-b8f0-2b3bf61fa81e  user_574772 2025-10-12 13:10:57.966590         web  Opera/8.38.(Windows NT 5.0; ber-MA) Presto/2.9...     v17.0.4  ...  cellular_4g            BR   verizon     BR-3    13           6
4  a8b5a150-cfc2-43cc-a339-3e7e2618ba18   user_46569 2025-10-12 13:11:01.966590      mobile  Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_7;...     v17.4.9  ...     ethernet            US       cox     US-2    13           6

```

### Metrics Explained

- **startup_time_ms**: Milliseconds from play button to first frame
- **rebuffer_count**: Number of buffering interruptions
- **bitrate_kbps**: Video quality in kilobits per second
- **resolution**: Final video resolution (4K, 1080p, 720p, 480p)


# Used Tools
- Explain which tools we use and why
- How do they work (don't go too deep into details, but add links)
- Why did we choose them
- How did we set them up


# Pipelines
**Pipeline** = A series of automated steps that data flows through, like water through pipes.

**Real-world example at our streaming service:**

```
User watches show → Data sent to servers → Data validated →
Data cleaned → Data organized → Data stored → Dashboards updated

```

All of this happens **automatically**, **every hour**, for **millions of users**.

We're building a mini-version that:

- Reads our fake data
- Validates it (checks for errors)
- Transforms it (makes it useful)
- Stores it (organizes it for analysis)
- Monitors it (alerts if something breaks)

## The Pipeline Architecture
```
┌─────────────────────────────────────────────────────────┐
│                     DATA PIPELINE                       │
└─────────────────────────────────────────────────────────┘

INPUT                                              OUTPUT
  │                                                  │
  ▼                                                  ▼
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Raw     │───▶│ Validate │───▶│Transform │───▶│  Clean   │
│  Data    │    │ & Clean  │    │ & Enrich │    │  Data    │
│  (CSV)   │    │          │    │          │    │(Database)│
└──────────┘    └──────────┘    └──────────┘    └──────────┘
                     │                │               │
                     ▼                ▼               ▼
                ┌─────────┐     ┌─────────┐     ┌─────────┐
                │ Quality │     │  Error  │     │ Success │
                │  Checks │     │   Logs  │     │ Metrics │
                └─────────┘     └─────────┘     └─────────┘
```

## Stream Processing
### Storing Data Stream
### Processing Data Stream
## Batch Processing
## Visualizations

## Data Flow (Session Journey)
```
1. GENERATION
   User device → Session telemetry → CSV file

2. INGESTION
   CSV → Pandas DataFrame → Validation

3. VALIDATION
   Quality checks → Pass/Fail → Cleaned DataFrame

4. TRANSFORMATION
   Raw metrics → Calculated features → Enriched DataFrame

5. DIMENSION LOOKUP
   Device "iPhone" → device_key 42
   Country "BR" → geo_key 15

6. FACT LOADING
   Enriched data + dimension keys → fact_playback_sessions table

7. AGGREGATION
   Fact table → GROUP BY date/device → agg_daily_device_quality

8. QUERY
   Dashboard request → SQL → Aggregate table → Results

9. VISUALIZATION
   Query results → Plotly chart → User's browser
```

# Data Warehouse

### Architecture

**Star Schema Design**
- 1 Fact Table: `fact_playback_sessions` (100K+ rows)
- 7 Dimension Tables: date, time, device, geography, content, network, user_cohort
- 4 Aggregate Tables: Daily and hourly pre-aggregated metrics
- 4 Views: Common analytical queries

### Database: PostgreSQL

**Why PostgreSQL?**
- Industry-standard OLAP database
- Excellent query optimizer
- Rich analytical functions (percentiles, window functions)
- Similar to production warehouses (Redshift, Snowflake)

### Key Features

**1. Dimensional Modeling**
- Star schema for fast analytical queries
- Surrogate keys for efficient joins
- Slowly changing dimensions (SCD Type 2 ready)

**2. Query Performance**
- Strategic indexing on fact table
- Pre-aggregated summary tables
- Materialized views for complex queries
- Partitioning-ready design

**3. Data Quality**
- Referential integrity constraints
- Automated quality checks
- Duplicate detection
- Range validation

### Sample Queries

**Overall Health:**
```sql
SELECT
    AVG(overall_qoe_score) as avg_qoe,
    AVG(startup_time_ms) as avg_startup
FROM fact_playback_sessions;
```
**Device Performance:**
```sql
SELECT
    device_type,
    AVG(overall_qoe_score) as avg_qoe
FROM fact_playback_sessions f
JOIN dim_device d ON f.device_key = d.device_key
GROUP BY device_type;
```

# Visualization

# Demo
- We could add a demo video here
- Or link to our presentation video of the project

# Conclusion
Write a comprehensive conclusion.
- How did this project turn out
- What major things have we learned
- What were the biggest challenges

# Follow Me On
[LinkedIn](https://www.linkedin.com/in/sada-garba/)

# Appendix

[Markdown Cheat Sheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)