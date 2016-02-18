#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import re

def getSource(txt):
    sourcer = re.compile('(?:.*>)(.*)(?:<\/a>)')
    print sourcer.search(txt).groups()[0]


if __name__ == '__main__':
	getSource(sys.argv[1])