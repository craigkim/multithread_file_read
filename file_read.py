#!/usr/bin/env python
#coding=utf-8

import glob
import os

##########################################################################################
LOG_ROOT='/home/craig'
##########################################################################################
class FtDB(object):
    def __init__(self, schema_name):
        self.schema_name = schema_name

    # /home/craig/test1 과 같은 위치에 있는 파일을 읽어서 내용을 리턴한다.
    def appendLog(self):
        for fh in sorted(glob.glob('%s/%s/*' % (LOG_ROOT, self.schema_name))):
            bname = os.path.basename(fh)

            with open(bname, "r") as fp:
                line = fp.readline()

            yield line