#!/usr/bin/python

import sys
import rrdtool
from lsftools.logfile import LogfileReader

if len(sys.argv) >= 2:
    input_file = open(sys.argv[1])
else:
    input_file = sys.stdin

reader = LogfileReader(input_file)

for row_num, record in enumerate(reader):
    print row_num, record['userName']
