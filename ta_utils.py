
import numpy as np

def mmi(data):
    """."""

    m = np.median(data)
    nh = 0
    nl = 0
    for i in range(1,len(data)):
        if (data[i] > m and data[i] > data[i - 1]):
            nl += 1
        elif (data[i] < m and data[i] < data[i - 1]):
            nh += 1
    return 100. * (nl + nh) / (len(data) - 1)


