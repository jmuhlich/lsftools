#!/usr/bin/python

import sys
from collections import defaultdict
from pylab import *
from matplotlib.dates import DateFormatter
from lsftools.logfile import LogfileReader

if len(sys.argv) >= 2:
    input_file = open(sys.argv[1])
else:
    input_file = sys.stdin

reader = LogfileReader(input_file)

time_resolution = 60  # in seconds
data = defaultdict(lambda: defaultdict(int))
for row_num, record in enumerate(reader):
    start_time = record['startTime'] / time_resolution
    end_time = record['eventTime'] / time_resolution
    if start_time == 0:
        continue
    for t in range(start_time, end_time + 1):
        data[record['userName']][t] += record['numExHosts']

for name in sorted(data.keys()):
    plot_date(array(data[name].keys()) * time_resolution, data[name].values(), label=name)
legend()
gca().set_yscale('log')
#gca().get_xaxis().set_major_formatter(DateFormatter('%b %d'))
show()
