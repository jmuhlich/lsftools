#!/usr/bin/python

import sys
import rrdtool
import csv
import re
import warnings
from collections import deque

class Field:
    string_format = None
    name = None
    format = None
    multi = False
    index = None

    def __init__(self, string_format, index):
        self.string_format = string_format
        self.index = index
        (self.name, self.format) = string_format.split('=')
        match = re.match(r'(.){#}$', self.format)
        if match:
            self.format = match.group(1)
            self.multi = True

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.string_format)

class Format:
    fields = []
    index = {}

    def __init__(self, fields):
        self.fields = fields
        self.index = dict((v, i) for i, v in enumerate([f.name for f in fields]))

class Record:
    format = None
    data = None

    def __init__(self, format):
        self.format = format
        self.data = [None] * len(format.fields)

    def __setitem__(self, i, value):
        assert(i in self.format.index)
        index = self.format.index[i]
        assert(index < len(self.data))
        self.data[index] = value

    def __getitem__(self, i):
        assert(i in self.format.index)
        index = self.format.index[i]
        assert(index < len(self.data))
        return self.data[index]

    def __str__(self):
        return str(self.data)

    def format_long(self):
        return '\n'.join('%-20s: %s' % (f.name, self[f.name]) for f in self.format.fields)
    
lsbacct_format = {
    'JOB_FINISH': [
        'eventType=s',
        'versionNumber=s',
        'eventTime=i',
        'jobId=i',
        'userId=i',
        'options=i',
        'numProcessors=i',
        'submitTime=i',
        'beginTime=i',
        'termTime=i',
        'startTime=i',
        'userName=s',
        'queue=s',
        'resReq=s',
        'dependCond=s',
        'preExecCmd=s',
        'fromHost=s',
        'cwd=s',
        'inFile=s',
        'outFile=s',
        'errFile=s',
        'jobFile=s',
        'numAskedHosts=i',
        'askedHosts=s{#}',
        'numExHosts=i',
        'execHosts=s{#}',
        'jStatus=i',
        'hostFactor=f',
        'jobName=s',
        'command=s',
        'ru_utime=f',
        'ru_stime=f',
        'ru_maxrss=f',
        'ru_ixrss=f',
        'ru_ismrss=f',
        'ru_idrss=f',
        'ru_isrss=f',
        'ru_minflt=f',
        'ru_majflt=f',
        'ru_nswap=f',
        'ru_inblock=f',
        'ru_oublock=f',
        'ru_ioch=f',
        'ru_msgsnd=f',
        'ru_msgrcv=f',
        'ru_nsignals=f',
        'ru_nvcsw=f',
        'ru_nivcsw=f',
        'ru_exutime=f',
        'mailUser=s',
        'projectName=s',
        'exitStatus=i',
        'maxNumProcessors=i',
        'loginShell=s',
        'timeEvent=s',
        'idx=i',
        'maxRMem=i',
        'maxRSwap=i',
        'inFileSpool=s',
        'commandSpool=s',
        'rsvId=s',
        'sla=s',
        'exceptMask=i',
        'additionalInfo=s',
        'exitInfo=i',
        'warningTimePeriod=i',
        'warningAction=s',
        'chargedSAAP=s',
        'licenseProject=s',
        'options3=i',
        'app=s',
        'postExecCmd=s',
        'runtimeEstimation=i',
        'jobGroupName=s',
        'resizeNotifyCmd=s',
        'lastResizeTime=i',
        'rsvId=s',
        'jobDescription=s',
        ]
    }

# prepare formats by turning them into Field objects
for name, fields in lsbacct_format.items():
    for j, field_string in enumerate(fields):
        fields[j] = Field(field_string, j)
    lsbacct_format[name] = Format(fields)

if len(sys.argv) >= 2:
    input_file = open(sys.argv[1])
else:
    input_file = sys.stdin
reader = csv.reader(input_file, delimiter=' ', doublequote=True)

for row_list in reader:
    row = deque(row_list)
    event_type = row_list[0]
    record = Record(lsbacct_format[event_type])
    for field in record.format.fields:
        if field.multi:
            record[field.name] = [row.popleft() for i in range(int(record[record.format.fields[field.index-1].name]))]
        else:
            record[field.name] = row.popleft()
    print record.format_long()
