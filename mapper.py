#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split("\t")

    if len(parts) >= 3:
        city = parts[2].strip()
        print("{}\t1".format(city))