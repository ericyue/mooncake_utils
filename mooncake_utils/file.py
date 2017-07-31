# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import os
import glob
import shutil

def mkdirp(directory):
  """
    利用python库来做到shell中的 ``mkdir -p``

    好处是不用 ``os.system()``，避免了fork进程造成的资源浪费。

    :param directory: 路径
  """
  if not os.path.isdir(directory):
    os.makedirs(directory)

def rm_folder(path, debug = False):
  """
    清空文件夹
  
    :param debug: 若为True，则只打印日志，不执行删除操作。
  """
  if not path.endswith("/*"):
    path+="/*"
  files = glob.glob(path)
  for one in files:
    print("removing [%s]" % one)
    if not debug:
      os.remove(one)

def glob2list(fn):
  ret = []
  if not fn: return ret

  for path in fn:
    ret += glob.glob(path)

  return sorted(list(set(ret)))

def glob2list_by_date(fn, date_col = None):
  if not date_col:
    raise Exception("specify date_col")

  ret = []

  for path in fn:
    ret += glob.glob(path)

  split_ = {}
  for path in ret:
    date_ = path.split("/")[date_col]
    if date_ not in split_:
      split_[date_] = []
    split_[date_].append(path)

  return sorted(split_.items(), key = lambda x: x[0]),split_

def rglob(p):
  matches = []
  for root, dirnames, filenames in os.walk(p):
      for filename in filenames:
        path = os.path.join(root, filename)
        matches.append(path)
  
  return matches

def safewrite(filename, content):
  """Writes the content to a temp file and then moves the temp file to 
  given filename to avoid overwriting the existing file in case of errors.
  """
  f = file(filename + '.tmp', 'w')
  f.write(content)
  f.close()
  os.rename(f.name, filename)


if __name__ == "__main__":
  s= glob2list_by_date(["/data3/yuebin/dataset/v2/*/*/part-0000*"], date_col = 5)
  for i in s:
    print i[0],i[1]
