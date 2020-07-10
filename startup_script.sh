#! /bin/bash
source update_env/bin/activate
python3 covid_contact_graphs/excecute_update.py >> excecution_logs/excecution.log
sleep 60s