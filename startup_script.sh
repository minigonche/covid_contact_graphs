#! /bin/bash
cd covid_contact_graphs/
echo "Excecuting Update"
python3 excecute_update.py > excecution.log
echo "Updating Git"
git add -A
git commit -a -m "updated_excecuted"
echo "Pushing"
git push