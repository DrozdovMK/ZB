import json
import sys
import asyncio
import numpy as np

a = np.array([1,2,3])
a = a[:, None]
print(a.shape)