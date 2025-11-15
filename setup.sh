# This is a setup script meant to be invoked from the root directory of the project
# after initial git clone before anything is executed.  You should run this file only once!

# 1.  Pip install dependencies
pip install -r requirements.txt

# 2.  Generate fake telemetry data
python data_generation/generate_telemetry.py

# 3.  Setup Postgres database
docker-compose up -d  # Starts PostgreSQL

# 4.  Setup Airflow
export AIRFLOW_HOME=$(pwd)/pipelines/airflow  # must be absolute path
export AIRFLOW__CORE__AUTH_MANAGER="airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"

airflow db migrate
airflow users create \
    --username airflow \
    --firstname Rayfeld \
    --lastname Square \
    --role Admin \
    --email inspector@rayfieldsquare.com \
    --password airflow

    # Start the web server like this ... (this gives you a visual interface)
    # airflow webserver --port 8080

    # Start the scheduler (in a second terminal) like this ... (this actually runs your pipelines and executes the workflows)
    # airflow scheduler

    # The aifflow dashboard will be visible here:
    # http://localhost:8080
    

# 5.  Setup Spark (optional)

# 6.  Setup Dashboards

