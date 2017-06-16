from commands import *
from subprocess import Popen, PIPE
from termcolor import colored, cprint
from mooncake_utils.log import get_logger

logger = get_logger(
                debug = True,
                name = "mu.cmd",
                with_file = False) 

def run_cmd(cmd, debug = True):
  if debug:
    logger.debug('')
    logger.debug('--------- Running command ---------')

  logger.debug(' ==> Command [%s]' % cmd)
  process = Popen(cmd,shell = True, stdout=PIPE, bufsize=1)
  with process.stdout:
      for line in iter(process.stdout.readline, b''): 
          print(line.strip())

  exit_code = process.wait() 
  core = bool(exit_code/ 256)
  signal_num = (exit_code << 8)  % 256

  if debug:
    if exit_code != 0:
      cprint( ' ==> Exit: %d, Signal: %d, Core: %s' % (exit_code, signal_num, bool(exit_code / 256)),
                  "white", "on_red")
    else:
      logger.debug(' ==> Exit: %d, Signal: %d, Core: %s' % (exit_code, signal_num, bool(exit_code / 256)))
    logger.debug('--------- Command End ---------')
    logger.debug('')

  return exit_code

def run_cmd_noblock(cmd, debug = True):
  if debug:
    print '--------- Running command ---------'
  print ' ==> Command [%s]' % cmd
  status, text = getstatusoutput(cmd)
  exit_code = status >> 8
  signal_num = status % 256
  if debug:
    print ' ==> Exit: %d, Signal: %d, Core: %s' % (exit_code, signal_num, bool(exit_code / 256))
    print ' ==> Output:\n%s' % text
    print '--------- Command End ---------'
    print ''
  return status, text

def gen_cmd(base, params):
  cmd = "%s " % base
  for p in params:
    cmd += "--%s=%s " % (p, params[p])

  logger.info(cmd)
  return cmd

if __name__ == "__main__":
  run_cmd('exit 123')
  run_cmd('exit 0')
  
  gen_cmd('python', {'a':1,'b':'xxxxxx'})

