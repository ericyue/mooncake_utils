from commands import *

def run_cmd(cmd):
  print '--------- Running command: %s ---------' % cmd
  status, text = getstatusoutput(cmd)
  exit_code = status >> 8
  signal_num = status % 256
  print ' ==> Exit: %d, Signal: %d, Core?: %s ' % (exit_code, signal_num, bool(exit_code / 256))
  print ' ==> Output:\n%s' % text
  print '--------- Command END ---------'
  print ''
  return status, text

if __name__ == "__main__":
  run_cmd('ls -alh')
