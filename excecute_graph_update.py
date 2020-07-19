# Excecutes the update functions for transits and contacts
import pathlib
import os, sys
from datetime import datetime
from googleapiclient import discovery

current_path = pathlib.Path(__file__).parent.absolute()

# Adds the location of the scripts
sys.path.append(current_path)
sys.path.append(os.path.join(current_path,'update_graphs/'))
sys.path.append(os.path.join(current_path,'functions/'))

# Imports the scripts
from config import *
import update_transits
import update_contacts
import update_graphs
import update_depto_codes

print('')
print('')
print('')
print('----------------------------------------------------------------------------------------------------------------------------')
print('----------------------------------------------------------------------------------------------------------------------------')
print('----------------------------------------------------------------------------------------------------------------------------')
print('Time: {}'.format(datetime.now()))
print('Started Update Process')
print('--------------------------------------')
print()
print('Depto Codes')
print('')
update_depto_codes.main()

print('--------------------------------------')
print()
print('Contacts')
print('')
update_contacts.main()

print('--------------------------------------')
print('')
print('Transits')
print('')
update_transits.main()

print('--------------------------------------')
print('')
print('Edgelists')
print('')
update_graphs.main()

print('--------------------------------------')
print('')
print('All Done')
print('<-OK->')

with open(os.path.join(current_path, 'excecution_logs/excecutions_timestamps.log'),'a') as f:
    f.write('Updated on: {}\n'.format(datetime.now()))

