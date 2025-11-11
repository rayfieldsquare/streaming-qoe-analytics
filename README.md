
# Streaming Service QoE Analytics
An example project on how to build a telemetry QoE analytics pipeline for a fictional streaming service using Python, Spark, Airflow and Postgres.

# Introduction & Goals
- Introduce your project to the reader
- Orient this section on the Table of contents
- Executive summary -- assuming ...
  - With what simulated data
  - What tools are you using
  - What are you doing with these tools
  - Once you are finished add the conclusion here as well

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


# Used Tools
- Explain which tools we use and why
- How do they work (don't go too deep into details, but add links)
- Why did we choose them
- How did we set them up

## Connect
## Buffer
## Processing
## Storage
## Visualization

# Pipelines
- Explain the pipelines for processing that we are building
- Go through our development and add our source code

## Stream Processing
### Storing Data Stream
### Processing Data Stream
## Batch Processing
## Visualizations

# Demo
- We could add a demo video here
- Or link to our presentation video of the project

# Conclusion
Write a comprehensive conclusion.
- How did this project turn out
- What major things have we learned
- What were the biggest challenges

# Follow Me On
Add the link to LinkedIn Profile

# Appendix

[Markdown Cheat Sheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)