# Excecutes the update functions for transits and contacts
import pathlib
import os, sys
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
update_contacts.main()

print('----------')
print('')
print('Tranits')
print('')
update_transits.main()

print('----------')
print('')
print('All Done')
