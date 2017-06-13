# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import os
import glob

def mkdirp(directory):
  if not os.path.isdir(directory):
    os.makedirs(directory)

def rm_folder(path):
  files = glob.glob(path)
  for one in files:
    print "removing [%s]", one
    os.remove(one)
