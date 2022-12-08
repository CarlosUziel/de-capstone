#!/bin/bash

# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
AIRFLOW_HOME=$(pwd)/_airflow
export AIRFLOW_HOME

AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/src/dags
export AIRFLOW__CORE__DAGS_FOLDER

AIRFLOW__CORE__PLUGINS_FOLDER=$(pwd)/src/plugins
export AIRFLOW__CORE__PLUGINS_FOLDER

AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__LOAD_EXAMPLES

AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS

# reset
killall airflow
rm -rf "$AIRFLOW_HOME"

# initialize the database
airflow db init

airflow users create \
--username admin \
--firstname Jon \
--lastname Snow \
--role Admin \
--email john@snow.org


# start the web server, default port is 8080
airflow webserver --daemon --port 8080

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
airflow scheduler --daemon

# visit localhost:8080 in the browser and use the admin account you just
# created to login.

# Wait till airflow web-server is ready
echo "Waiting for Airflow web server..."
while true; do
    _RUNNING=$(ps aux | grep airflow-webserver | grep ready | wc -l)
    if [ $_RUNNING -eq 0 ]; then
        sleep 1
    else
        echo "Airflow web server is ready"
        break;
    fi
done