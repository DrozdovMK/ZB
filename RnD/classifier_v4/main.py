import os
import sys
import asyncio
import threading 
scripts_folder = "scripts/"
sys.path.append(scripts_folder)
#script_curdir = os.path.dirname(os.path.abspath(__file__)) # директория исполняемого скрипта
#os.chdir(script_curdir)

from mainloop_async import Mainloop

async def main(zone):
    mainloop = Mainloop(
        model_path="pipeline3.pkl",
        data_socket_path='/tmp/das_driver',
        indent_time=500,
        cooling_time=2000,
        max_time=10000,
        threshold=4,
        plotting=True,
        verbose=True,
        saving=False,
        save_path="/home/demostend/drozdov/zb-classification/RnD/data/16_11_2024_data",
        zone_num=zone,
        max_files_count=250
    )
    await mainloop.start()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("you must specify zone number")
    zone_num = sys.argv[1]
    asyncio.run(main(zone_num)) 