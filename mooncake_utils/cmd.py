# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import sys
from commands import *
from subprocess import Popen, PIPE
from termcolor import colored, cprint
from mooncake_utils.log import get_logger

logger = get_logger(
                debug = True,
                name = "mu.cmd",
                with_file = False) 

def run_cmd(cmd, debug = False):
  """
    运行一个shell命令，并且打印结果。
    注意，这里是阻塞运行。

    :param cmd: 需要执行的命令 如 ``ls -alh`` 
    :param debug: 如果设置True则不执行cmd，仅打印相关日志
  """
  logger.debug('--------- Running command ---------')
  logger.debug(' ==> Command [%s]' % cmd)
  if debug:
    return 0

  process = Popen(cmd,shell = True, stdout=PIPE, bufsize=1)
  with process.stdout:
      for line in iter(process.stdout.readline, b''): 
          print(line.strip())

  exit_code = process.wait() 
  core = bool(exit_code/ 256)
  signal_num = (exit_code << 8)  % 256

  if exit_code != 0:
    cprint( ' ==> Exit: %d, Signal: %d, Core: %s' % (exit_code, signal_num, bool(exit_code / 256)),
                "white", "on_red")
  else:
    logger.debug(' ==> Exit: %d, Signal: %d, Core: %s' % (exit_code, signal_num, bool(exit_code / 256)))
  logger.debug('--------- Command End ---------')

  return exit_code

def run_cmd_noblock(cmd, debug = False):
  """
    作用同 ``run_cmd``, 不过这里是非阻塞的。
    
    :param cmd: 需要执行的命令 如 ``ls -alh`` 
    :param debug: 如果设置True则不执行cmd，仅打印相关日志

  """
  logger.info(' ==> Command No-Block [%s]' % cmd)
  if debug:
    return 0
  status, text = getstatusoutput(cmd)
  exit_code = status >> 8
  signal_num = status % 256
  
  logger.info(' ==> Exit: %d, Signal: %d, Core: %s' % (exit_code, signal_num, bool(exit_code / 256)))
  logger.info(text) 
  return status, text

def md5_file(path):
  return md5(path)

def md5(path):
  """
    为文件生成相应的md5sum。
    
    :param path: 需要生成md5的路径，如 ``./output/final.dat``

    执行成功后产出 ``./output/final.dat.md5``

  """
  cmd = 'md5sum  {0} > {0}.md5'.format(path)
  run_cmd(cmd)

def gen_cmd(base, params, pretty = False):
  cmd = "%s " % base
  for p in params:
    if type(params[p]) == bool:
      if params[p]:
        cmd += "--%s " % (p)
      else:
        cmd += "--no%s " % (p)
    else:
      cmd += "--%s=%s " % (p, params[p])

    if pretty:
      logger.info(cmd.strip().split(' ')[-1])

  return cmd

class cmd_builder:
  pool = {}
  conf = None
  pretty = False
  bin_base = None

  def __init__(self, bin_base, conf, pretty = False):
    self.conf = conf
    self.bin_base = bin_base
    self.pretty = pretty
    logger.debug('init param_builder')

  def use_all_conf(self):
    self.pool = self.conf

  def put(self, p, value = None):
    if not value:
      self.pool[p] = self.conf[p]
    else:
      self.pool[p] = value

    logger.debug('put [%s:%s]' % (p, self.conf[p]))

  def get(self, p):
    return self.pool[p]

  def build(self):
    cmd = gen_cmd(self.bin_base, self.pool, self.pretty)
    logger.info('build result [%s]' % cmd)
    return cmd

def isint(x):
    try:
        x = int(x)
        return 1
    except:
        return 0

def isarg(pos):
    try:
        temp = sys.argv[pos]
        temp = 1
    except:
        temp = 0
    return temp

def setarg(pos, val):
    if isarg(pos):
        if isint(sys.argv[pos]):
            return int(sys.argv[pos])
        else:
            return sys.argv[pos]
    else:

        sys.argv.append(str(val)) # str(val) is used, because by default all arguments are strings  
        if isint(sys.argv[len(sys.argv)-1]):
            return int(sys.argv[len(sys.argv)-1])
        else:
            return sys.argv[len(sys.argv)-1]

def scp(src, host, target, port = 22):
  for host in host:
    run_cmd("scp -P %s %s %s:%s" % (port, src, host, target))
    run_cmd("scp -P %s %s.md5 %s:%s.md5" % (port, src, host, target))

if __name__ == "__main__":
  run_cmd('exit 123')
  run_cmd('exit 0')
  gen_cmd('python', {'a':1, 'b':'xxxxxx', 'c':True, 'd':False}, pretty= True)
  c = {'a':1, 'b':'xxxxxx', 'c':True, 'd':False}
  cb = cmd_builder('test.py', c)
  cb.put('a')
  cb.put('b')
  cb.put('c')
  cb.build()
