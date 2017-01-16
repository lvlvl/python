#!/usr/bin/python

import csv
import itertools
import argparse
from sets import Set
import pickle
import gzip
import tarfile
import os
from subprocess import check_call
from os import listdir
from os.path import isfile, join
from shutil import copyfile

def argparser():
    parser = argparse.ArgumentParser(description='Specify parameters')
    parser.add_argument('-i', '--input', dest='input_folder', default=None, required=True, help='Full path to input folder to read from')
    parser.add_argument('-o', '--output', dest='output_folder', default=None, required=False, help='Full path to output folder to write to')
    parser.add_argument('-t', '--temp', dest='temp_folder', default='/tmp/dcc', required=False, help='Full path to location for temporary file creation')
    parser.add_argument('-d', '--delimiter', dest='delimiter', default='\t', required=False, help='Delimiter')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true', required=False)
    args = parser.parse_args()
    return args
