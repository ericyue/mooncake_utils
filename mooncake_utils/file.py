# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import os
import glob
import shutil

def mkdirp(directory):
  if not os.path.isdir(directory):
    os.makedirs(directory)

def rm_folder(path, debug = False):
  files = glob.glob(path)
  for one in files:
    print "removing [%s]", one
    if not debug:
      os.remove(one)

