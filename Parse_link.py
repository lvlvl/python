#!/usr/bin/python

import csv
import urllib
import urllib2
import json
import StringIO
from StringIO import StringIO
import argparse

output_file = None
writer = None

def getwriter(header):
    global output_file
    global writer
    if writer == None:
        if not params.output_filename:
            fname_out = params.input_filename+'.out'
        else:
            fname_out = params.output_filename
        output_file = open(fname_out,'w+')
        writer = csv.DictWriter(output_file,fieldnames=header,delimiter=params.delimeter)
        writer.writeheader()
    return writer
# Choose method
def getmethod(line):
    if 'api_method' in line:
        return line['api_method']
    return 'all'

def argparser():
    parser = argparse.ArgumentParser(description='Specify input and output filenames')
    parser.add_argument('-i', '--input', dest='input_filename', default=None, required=True, help='Full path to input filename')
    parser.add_argument('-o', '--output', dest='output_filename', default=None, required=False, help='Full path to output filename')
    parser.add_argument('-u', '--url', dest='url', default='http://invhdp01:8080/sax/v1/media/', required=False, help='REST API')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true', required=False)
    parser.add_argument('-d', '--delimeter', dest='delimeter', default='\t', required=False)
    parser.add_argument('-b', '--bad', dest='bad', default=50, required=False, help='Number of bad rows allowed')
    args = parser.parse_args()
    return args

def main():
    fname = params.input_filename
    with open(fname,'rb') as f:
        reader = csv.DictReader(f,delimiter='\t')
        for line in reader:
            request = params.url+getmethod(line)+'?'+urllib.urlencode(line)
            response = urllib2.urlopen(request).read()
            if params.verbose:
                print(response)
            io = StringIO(response)
            parsed = json.load(io)
            writer = getwriter(reader.fieldnames+sorted(parsed.keys()))
            writer.writerow(dict(line.items()+parsed.items()))
    output_file.close()

params = argparser()

main()
