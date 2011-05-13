import csv
import re
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
        return '%s(%s)' % (self.__class__.__name__, self)

    def __str__(self):
        return self.string_format

class FormatError(RuntimeError):
    msg = ''
    def __init__(self, msg):
        self.msg = msg
        RuntimeError.__init__(self, msg)

class FieldFormatError(FormatError):
    def __init__(self, field, value):
        message = "'%s' is not a valid value for field '%s' " % (value, field)
        FormatError.__init__(self, message)

class RecordFormatError(FormatError):
    def __init__(self, row_num, msg):
        # report 1-based row numbers
        message = "error at line %d: %s" % (row_num + 1, msg)
        FormatError.__init__(self, message)

class UnkownEventTypeError(FormatError):
    def __init__(self, event_type):
        message = "unknown event type '%s'" % event_type
        FormatError.__init__(self, message)

class NotEnoughValuesError(FormatError):
    def __init__(self, event_type, version):
        message = "not enough values for event '%s' version '%s'" % (event_type, version)
        FormatError.__init__(self, message)


class Format:
    fields = []
    index = {}

    def __init__(self, fields):
        self.fields = fields
        self.index = dict((v, i) for i, v in enumerate([f.name for f in fields]))

RE_INT = re.compile('-?[0-9]+')

class Record:
    format = None
    data = None

    def __init__(self, format):
        self.format = format
        self.data = [None] * len(format.fields)

    def _get_field_by_name(self, field_name):
        if field_name not in self.format.index:
            raise KeyError(field_name)
        field_index = self.format.index[field_name]
        if field_index >= len(self.data):
            raise IndexError('%s (%d)' % (field_name, field_index))
        return self.format.fields[field_index]

    def __setitem__(self, i, value):
        field = self._get_field_by_name(i)
        format = field.format
        try:
            if format == 'i':
                value = int(value)
            elif format == 'f':
                value = float(value)
            elif format == 's':
                pass
        except ValueError:
            raise FieldFormatError(field, value)
        self.data[field.index] = value

    def __getitem__(self, i):
        field = self._get_field_by_name(i)
        return self.data[field.index]

    def __str__(self):
        return str(self.data)

    def format_long(self):
        return '\n'.join('%s (%%%s) : %s' % (f.name, f.format, self[f.name]) for f in self.format.fields)
    
formats = {
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
        # warningTimePeriod and warningAction appear to be reversed in the lsb.acct docs (fixed here)
        ###'warningTimePeriod=i',
        ###'warningAction=s',
        'warningAction=s',
        'warningTimePeriod=i',
        'chargedSAAP=s',
        'licenseProject=s',
        # this column never appears in files i've observed
        ###'options3=i',
        'app=s',
        'postExecCmd=s',
        'runtimeEstimation=i',
        'jobGroupName=s',
        'autoRequeueCodes=s',
        'unknown_1=i',
        'resizeNotifyCmd=s',
        'lastResizeTime=i',
        'rsvId_alternate=s',
        'jobDescription=s',
        ]
    }

# last defined field name by LSF version for each format
format_last_field = {
    'JOB_FINISH': {
        '6.0' : 'chargedSAAP',
        '7.06': 'jobDescription',
        }
    }


# prepare formats by turning them into Field objects
for name, fields in formats.items():
    for j, field_string in enumerate(fields):
        fields[j] = Field(field_string, j)
    formats[name] = Format(fields)


class LogfileReader(object):
    csv_reader = None

    def __init__(self, input_file):
        self.csv_reader = csv.reader(input_file, delimiter=' ', doublequote=True, skipinitialspace=True)
        self.enum_reader = enumerate(self.csv_reader)

    def __iter__(self):
        return self

    def next(self):
        (row_num, row_list) = self.enum_reader.next()
        row = deque(row_list)
        event_type = row_list[0]
        try:
            if event_type not in formats:
                raise UnknownEventTypeError(event_type)
            record = Record(formats[event_type])
            for field in record.format.fields:
                if field.multi:
                    record[field.name] = [row.popleft() for i in range(int(record[record.format.fields[field.index-1].name]))]
                else:
                    record[field.name] = row.popleft()
                # seems there is always a dummy "0" as the last value, so len==1 means we're done
                if len(row) == 1:
                    version = record['versionNumber']
                    if format_last_field[event_type][version] == field.name:
                        return record
                    else:
                        raise NotEnoughValuesError(event_type, version)
        except FormatError as e:
            raise RecordFormatError(row_num, e.msg)
