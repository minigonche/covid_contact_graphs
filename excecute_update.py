# Excecutes the update functions for transits and contacts
import pathlib
import os, sys
from datetime import datetime
from config import *
from googleapiclient import discovery

current_path = pathlib.Path(__file__).parent.absolute()

# Adds the location of the scripts
sys.path.append(os.path.join(current_path,'update_scripts/'))

# Imports the scripts
import update_transits
import update_contacts

print('Started Update Process')
print('----------------')
print()
print('Contacts')
print('')
#update_contacts.main()

print('----------')
print('')
print('Tranits')
print('')
#update_transits.main()

print('----------')
print('')
print('All Done')
print('')

with open('excecutions_timestamps.log','a') as f:
    f.write('Updated on: {}'.format(datetime.now()))

print('Shuting down ')
service = discovery.build('compute' ,'v1', cache_discovery=False)
request = service.instances().stop(project=project, zone=zone, instance=instance)
#response = request.execute()