import os,sys
import mmap
import glob
from mooncake_utils.date import *

if __name__ == "__main__":
  input_fn = sys.argv[1]
  output_fn = "%s.fix" % input_fn
  print "input[%s] output[%s]" % (input_fn, output_fn)

  output = open(output_fn, "w")

  bad = 0
  with open(input_fn, "r+b") as f:
      try:
        map = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
      except Exception,w:
        print w
        exit()

      for line in iter(map.readline, ""):
        if line[-1] == "\n":
          key,value = line.strip().split('\x01')
          new_value = ""
          v_items = value.split(',')
          for v in v_items:
            try:
              int(v.split(':')[0])
              new_value += "%s," % v
            except Exception,w:
              print w,line
              continue

          output.write("%s\x01%s\n" % (key,new_value.rstrip(',')))
        else:
          bad +=1
          print "badline #%s" % bad
  
  print "total bad line:", bad
  output.close()
