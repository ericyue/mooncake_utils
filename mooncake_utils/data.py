# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import os,sys
import mmap

def read_mmap(fn):
  """read_mmap

  :param fn:
  """
  with open(fn, "r+b") as f:
    map = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
    for line in iter(map.readline, ""):
      yield line.rstrip('\n')

def trunc(f, n = 4):
  """trunc

  :param f:
  :param n:
  """
  s = '{}'.format(f)
  if 'e' in s or 'E' in s:
      return float('{0:.{1}f}'.format(f, n))
  i, p, d = s.partition('.')
  ret = '.'.join([i, (d+'0'*n)[:n]])
  return float(ret)

def parse_int(m, default = 0):
  """parse_int

  :param m:
  :param default:
  """
  if type(m) == int:
    return m
  elif type(m) == str:
    try:
      return int(m)
    except:
      return default
  else:
    raise Exception('error input %s, cannot parse' % m)

def parse_float(m, default = 0.0):
  """parse_float

  :param m:
  :param default:
  """
  if type(m) == int:
    return m
  elif type(m) == str:
    try:
      return float(m)
    except:
      return default
  else:
    raise Exception('error input %s, cannot parse' % m)

if __name__ == "__main__":
    pass
