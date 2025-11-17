# Quickstart Guide

This is a setup script meant to be invoked from the root directory of the project
after initial git clone before anything is executed.  You should run this file only once!

# 1.  Setup Airflow
```bash
docker compose up airflow-init
```

# 2.  Launch all other services including analytics warehouse database
```bash
docker-compose up -d  # Starts airflow and warehouse database (postgres)
```

# 3.  Pip install local dependencies
```bash
pip install -r requirements.txt
```

# 4.  Generate fake telemetry data
```bash
python data_generation/generate_telemetry.py
```
    
# 5. Login to Airflow
- visit http://localhost:8080  
- Username: airflow, Password: airflow
- Search for the _"streaming_qoe_pipeline"_ DAG and Trigger / Unpause it to see it run

# 6.  Populate Dimensions in warehouse database
```bash
python warehouse/populate_dimensions.py
```

# 7. Load fact data (generated from the airflow pipeline in step 5) into the warehouse database
```bash
python warehouse/load_fact_data.py
```

# 8. Create aggregates in warehouse database
```bash
docker exec -i streaming_analytics psql -U analytics_user -d streaming_analytics < warehouse/create_aggregates.sql
```

# 9.  Run the Dashboards
```bash
cd dashboards
streamlit run home.py
```


