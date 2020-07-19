# Excecute Attribute update

import pathlib
import os, sys
from datetime import datetime


current_path = pathlib.Path(__file__).parent.absolute()

# Adds the location of the scripts
sys.path.append(current_path)
sys.path.append(os.path.join(current_path,'update_attributes/'))
sys.path.append(os.path.join(current_path,'functions/'))

# Imports the scripts
from config import *
import update_attributes
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
print('')
update_attributes.main()

print('--------------------------------------')
print('')
print('All Done')
print('<-OK->')

with open(os.path.join(current_path, 'excecution_logs/excecutions_attributes_timestamps.log'),'a') as f:
    f.write('Updated on: {}\n'.format(datetime.now()))
