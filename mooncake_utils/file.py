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
  pass
