script_folder_demostend  = '../../scripts/' # for demostend
script_folder_local  = 'scripts/' # for laptop
import sys
sys.path.append(script_folder_demostend)
sys.path.append(script_folder_local)
import crop
import numpy as np
import json
import signal_processing as sp
from hdf5handler_v2 import HDF5Handler
from classifier import classifier_loop

if __name__ == '__main__':
    
    if len(sys.argv) > 1:
        zone_num = sys.argv[1]  # 1-st argument from command line
    else:
        print("No zone.")
        
    
    hdf5_handler = HDF5Handler(
            object_name='../data/demostend_1',
            model_path = 'random_forest.pkl',
            zone = int(zone_num),
            detector_th = 150,
            mstd_th = 150,
            mstd_wdw = 32,
            N_wait_frames = 2,
            indent = 500,
            max_duration = 25000,
            max_files_count = 18,
            saving = True,
            verbose = True  
        )
    hdf5_handler.saver_on()
    
    
