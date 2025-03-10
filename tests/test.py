
script_folder_local  = 'scripts/' # for laptop
import sys
sys.path.append(script_folder_local)

from mainloop import Mainloop
import numpy as np
import os

mainloop = Mainloop(
    model_path = "/home/drozdovmk/Projects/ZB/zb-classification/RnD/classifier_v3_rf/pipeline2.pkl", 
    plotting = True,
    verbose=True,
    threshold = 2,
    indent_time=500,
    cooling_time=2000,
    max_time = 10000,
    saving=True,
    save_path = "tests/data/",
    zone_num=1,
    max_files_count=10)


mainloop.start_test(path_to_test_signal = "tests/test_data.npy")