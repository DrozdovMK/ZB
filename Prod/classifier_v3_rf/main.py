import os
import sys
scripts_folder = "scripts/"
sys.path.append(scripts_folder)
#script_curdir = os.path.dirname(os.path.abspath(__file__)) # директория исполняемого скрипта
#os.chdir(script_curdir)


from mainloop import Mainloop

if __name__ == '__main__':

    if len(sys.argv) > 1:
        zone_num = sys.argv[1]  # 1-st argument from command line
    else:
        print("no zone")
        zone_num=0
        
    mainloop = Mainloop(model_path="pipeline3.pkl",
                        indent_time=500,
                        cooling_time=1000,
                        max_time=10000,
                        threshold=2,
                        plotting=True,
                        verbose=True,
                        saving=False,
                        save_path="/home/demostend/drozdov/zb-classification/RnD/data/16_11_2024_data",
                        zone_num=zone_num,
                        max_files_count=250)
    mainloop.start()


    
