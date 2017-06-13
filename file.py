# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import os

def mkdirp(directory):
  if not os.path.isdir(directory):
    os.makedirs(directory)
