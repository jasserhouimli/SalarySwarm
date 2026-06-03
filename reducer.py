#!/usr/bin/env python3
import sys

current_key = None
current_sum = 0

for line in sys.stdin:
    line = line.strip()
    key, value = line.split("\t")
    value = int(value)

    if current_key == key:
        current_sum += value
    else:
        if current_key:
            print("{}\t{}".format(current_key, current_sum))
        current_key = key
        current_sum = value

if current_key:
    print("{}\t{}".format(current_key, current_sum))