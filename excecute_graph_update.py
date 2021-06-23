# Excecutes the update functions for transits and contacts
import pathlib
import os, sys
from datetime import datetime


current_path = pathlib.Path(__file__).parent.absolute()

# Adds the location of the scripts
sys.path.append(current_path)
sys.path.append(os.path.join(current_path,'update_graphs/'))
sys.path.append(os.path.join(current_path,'functions/'))

# Imports the scripts
from config import *
import check_integrity
import update_transits
import update_contacts
import update_graphs
import update_depto_codes
import update_housing
import update_sizes
import update_paths
import update_movement
import update_seniority


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
print('Integrity')
print('')
check_integrity.main()

print('--------------------------------------')
print()
print('Depto Codes') # Code Level
print('')
update_depto_codes.main()


print('--------------------------------------')
print()
print('Seniority') # Code Level
print('')
update_seniority.main() 


print('--------------------------------------')
print()
print('Contacts') # Code Level
print('')
update_contacts.main()



print('--------------------------------------')
print('')
print('Transits') # location Level
print('')
update_transits.main() # CHECK STATIC TRANSITS



print('--------------------------------------')
print('')
print('Edgelists') # location Level
print('')
update_graphs.main() # CHECK STATIC LOCATIONS


print('--------------------------------------')
print()
print('Housing Locations') # Code Level
print('')
update_housing.main() 


print('--------------------------------------')
print()
print('Graph Sizes') # location Level
print('')
update_sizes.main()  # CHECK STATIC LOCATIONS



print('--------------------------------------')
print()
print('Paths') # Code Level
print('')
update_paths.main()


# Bogota tiene ciertos valores computados. Faltan las localidades
print()
print('Graph Movement') # location Level
print('')
update_movement.main() # CHECK STATIC LOCATIONS



print('--------------------------------------')
print('')
print('All Done')
print('<-OK->')
