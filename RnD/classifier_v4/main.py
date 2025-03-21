import os
import sys
import asyncio

scripts_folder = "scripts/"
sys.path.append(scripts_folder)
#script_curdir = os.path.dirname(os.path.abspath(__file__)) # директория исполняемого скрипта
#os.chdir(script_curdir)

from mainloop_async import Mainloop

async def main():
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
        zone_num=455,
        max_files_count=250
    )
    await mainloop.start()

if __name__ == '__main__':
    asyncio.run(main())
