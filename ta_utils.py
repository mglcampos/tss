
import numpy as np
from math import floor

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

def alma(data, period):
    """."""

    m = floor(0.85*(period-1))
    s = period/6.0
    alma = 0.
    wSum = 0.
    for i in range(0, period):
        w = np.exp(-(i-m)*(i-m)/(2*s*s))
        alma += data[period-1-i] * w
        wSum += w

    return alma / wSum

