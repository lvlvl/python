#!/usr/bin/python

import csv
import urllib
import urllib2
import json
import StringIO
from StringIO import StringIO

output_file = None
writer = None
url = 'http://invhdp01:8080/sax/v1/media/'

def getwriter(header):
    global output_file
    global writer
    if writer == None:
        output_file = open('/Users/Sergey/Work/output_sample.csv','w')
        writer = csv.DictWriter(output_file,fieldnames=header,delimiter='\t')
        writer.writeheader()
    return writer
# Choose method
def getmethod(line):
    if 'method' in line:
        return line['method']
    return 'all'

with open('/Users/Sergey/Work/sample_file_for_dict_processing_copy.txt','rb') as f:
    reader = csv.DictReader(f,delimiter='\t')
    for line in reader:
        request = url+getmethod(line)+'?'+urllib.urlencode(line)
#        print request
        response = urllib2.urlopen(request).read()
        io = StringIO(response)
        parsed = json.load(io)
        writer = getwriter(reader.fieldnames+sorted(parsed.keys()))
        writer.writerow(dict(line.items()+parsed.items()))

output_file.close()
