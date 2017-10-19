import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np
from colour import Color

colors = list(Color("red").range_to(Color("yellow"), 1500))
colors.extend(Color("yellow").range_to(Color("green"), 1501))

img = np.full((1746, 1286, 3), 1, dtype=np.float32)

arr = np.loadtxt("out/rapewinterrape_Yield_1996_1.asc", skiprows=6)

min_val = min(filter(lambda x: x != -9999, arr.flat))
max_val = max(filter(lambda x: x != -9999, arr.flat))

print min_val, max_val

for row in range(arr.shape[0]):
    for col in range(arr.shape[1]):
        val = arr[row, col]
        ival = int(val)
        if ival != -9999:
            img[row, col] = colors[ival].rgb #val / max_val


#imgplot = plt.imshow(img)
#plt.show()

mpimg.imsave("out/rapewinterrape_Yield_1996_1.png", img)




