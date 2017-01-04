#!/usr/bin/python

import tarfile

tar = tarfile.open('Path to the tarfile')
tar.extractall(path='Path to extracted directory')
tar.close()
