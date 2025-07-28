import os
import sys
import json
scripts_folder = "scripts/"
sys.path.append(scripts_folder) # add folder with scripts to PATH 
script_curdir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_curdir)


from mainloop import Mainloop

if __name__ == '__main__':

    if len(sys.argv) > 1:
        zone_num = sys.argv[1]  # 1-st argument from command line
    else:
        raise Exception("you must specify zone number as argument in command line")
        
    with open("classifier_config.json", "r", encoding="utf-8") as file:
        config = json.load(file)
        config["zone_num"] = zone_num
    
    mainloop = Mainloop(**config)
    mainloop.start()


    
