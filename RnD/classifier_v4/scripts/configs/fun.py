import json
import sys
import asyncio
async def dec():
    process = await asyncio.create_subprocess_exec("echo", "mainloop_async2.py","asdsa")

asyncio.run(dec())