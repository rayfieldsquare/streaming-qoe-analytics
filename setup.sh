# This is a setup script meant to be invoked from the root directory of the project
# after initial git clone before anything is executed.  You should run this file only once!

# 1.  Setup Airflow
docker compose up airflow-init

# 2.  Launch all other services including analytics warehouse database
docker-compose up -d  # Starts airflow and warehouse database (postgres)

# 3.  Pip install local dependencies
pip install -r requirements.txt

# 4.  Generate fake telemetry data
python data_generation/generate_telemetry.py
    
# 5. Login to Airflow
# visit http://localhost:8080  
# Username: airflow, Password: airflow
#. Search for the "streaming_qoe_pipeline" DAG and Trigger / Unpause it to see it run

# 6.  Populate Dimensions in warehouse database
python warehouse/populate_dimensions.py

# 7. Load fact data (generated from the airflow pipeline in step 5) into the warehouse database
python warehouse/load_fact_data.py

# 8. Create aggregates in warehouse database
docker exec -i streaming_analytics psql -U analytics_user -d streaming_analytics < warehouse/create_aggregates.sql

# 9.  Run the Dashboards
cd dashboards
streamlit run home.py

