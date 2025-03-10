import os
script_curdir = os.path.dirname(os.path.abspath(__file__)) # директория исполняемого скрипта
os.chdir(script_curdir)
from classifier import classifier_loop
if __name__ == '__main__':

    cl = classifier_loop(model_path = 'random_forest.pkl',
    			 plotting = True,
    			 detector_th = 150,
    			 mstd_th = 32,
    			 indent=500,
    			 N_wait_frames=2,
    			 max_duration = 25000)
    cl.classifier_on()
        
    


    
