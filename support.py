#from web3.modutils import Nodes
import os, gzip
import datetime, re, subprocess
from web3.modutils import TablePaginate, ModuleInfo

months = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 
          'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}

# defaultPageSize should match the value in the template for the lines per page <input> field
@TablePaginate('messages', defaultPageSize=50, sortParam="sort", pageParam="page", pageSizeParam="pageSize")
@ModuleInfo(services=[])
def read(_handle, startDate=None, logName='messages', prefix='', suffix='', filterStr=""):
    
    # Note: Filtering has to be done in the read() rather than in @TableFilter 
    # because of performance issues so we need to filter using grep.
    
    alphanumericRE = re.compile(r'\d+|\D+')
    msgFiles = [alphanumericRE.findall(f)
                for f in os.listdir('/var/log')
                if f.startswith(logName + '.')]
    
    msgFiles = [f for f in msgFiles if 1 < len(f)]
    msgFiles = [[not x.isdigit() and x or int(x) for x in f]
                for f in msgFiles if f[1].isdigit()]
    msgFiles.sort()
    logChoices = [{'value': '', 'text': 'Current Log'}] + \
                 [{'value': '.%s' % ''.join([str(x) for x in f][1:]), 'text': 'Archived Log # %d' % f[1]}
                  for f in msgFiles]
    
    # The log choice comes from the user so we can't trust it.  Strip
    # out the basename in case they entered a relative path to an
    # existing file, such as ../../etc/passwd .  (Wouldn't want to
    # filter and display that!)  We check below that such a file
    # actually exists in /var/log/ .
    fileBaseName = os.path.basename('%s%s%s' % (prefix, logName, suffix))
    # Construct log file name from base and number.
    # It looks like messages, user_messages, messages.1, user_messages.1, etc.
    fileName = '/var/log/%s' % fileBaseName

    # does the file exist
    os.stat(fileName)
    gzipped = fileName.endswith('.gz')

    # If no filter pattern, examine the file directly (plain or gzipped),
    # otherwise pipe it through grep or its gzip equivalent, zgrep.
    
    # filterStr is formatted as "msg:foobar" where msg is the column name and 
    # foobar is the filter string. If the format is not correct, ignore it
    filterSplit = filterStr.split(':')
    if not filterStr or len(filterSplit) == 1:
        logFile = gzipped and gzip.open(fileName, 'rb') or file(fileName, 'r')
    else:
        # remove the column name prefix from the filterStr param, we always filter
        # against the 'msg' column
        _filterStr = ':'.join(filterSplit[1:]) 
        grepPath = gzipped and '/usr/bin/zgrep' or '/bin/grep'
        cmd = [grepPath] + [_filterStr] + [fileName]
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                  stdout=subprocess.PIPE, close_fds=True)
        logFile = p.stdout
    
    
    # iterate over the logs try to separate out the time stamp from the log message
    # then filter against a start date if one is supplied
    if startDate:
        if len(startDate.split('/')) == 3:
            sd = datetime.date(*[int(x) for x in startDate.split('/')])
            st = datetime.time(0, 0, 0) # TODO: add a time widget
            sdt = datetime.datetime.combine(sd, st)
        else:
            startDate = None # zero this out, its malformed
    
    assumedYear = datetime.datetime.now().year
    rows = []
    rowId = 1
    for message in logFile:
        if not message.strip():
            # ignore blank lines
            continue
        # split and remove extra whitespace
        parts = message.split(' ')
        dateStr = ''
        if parts:
            if months.get(parts[0]):
                date = []
                index = 0
                i = 3 # time stamps consists of 3 parts, [month, day, time]
                for part in parts:
                    if not i:
                        # only collect the first 3 sections
                        break
                    index = index + 1
                    if not part:
                        # don't count extra white space, we might iterate 
                        # over more than 3 parts to get the time stamp
                        continue
                    date.append(part)
                    i = i - 1
                dateStr = ' '.join(date)
                
                d = datetime.date(assumedYear, months[date[0]], int(date[1]))
                t = datetime.time(*[int(x) for x in date[2].split(':')])
                dt = datetime.datetime.combine(d, t)
                # perform the time calculation, only accept times that are greater than
                # the state time
                if startDate and sdt > dt:
                    continue
                message = ' '.join(parts[index:])

        row = {
            'id': rowId,
            'date': dateStr,
            'msg': message
        }
        rowId = rowId + 1
        rows.append(row)

    return {
        'messages': rows,
        'logs': logChoices
    }
