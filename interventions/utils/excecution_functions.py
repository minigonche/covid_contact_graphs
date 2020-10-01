# Excecution functions
import os
import time
import numpy as np
import constants as con
from datetime import datetime

def excecute_script(script_location, name, code_type, parameters):
    '''
    Excecutes a certain type of script
    '''

    start = time.time()
    if code_type.upper() == 'PYTHON':

        #Python
        if not name.endswith('.py'):
            name = name + '.py'

        final_path = os.path.join(script_location, name)
        resp = os.system('{} {} {}'.format('python', final_path, parameters))

    elif code_type.upper() == 'R':
        #R
        if not name.endswith('.R'):
            name = name + '.R'

        final_path = os.path.join(script_location, name)
        resp = os.system('{} {} {}'.format('Rscript --vanilla', final_path, parameters))

    else:
        raise ValueError('No support for scripts in: {}'.format(code_type))


    elapsed_time = time.time() - start

    return(resp)
