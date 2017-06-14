import os,sys
import mmap
import glob
from mooncake_utils.date import *
if __name__ == "__main__":
  output_fn = "./%s.dat.fix" % gen_today(delta=0,with_time=True)
  w = open(output_fn, "w")
  input_fn = sys.argv[1]
  print "input[%s] output[%s]" % (input_fn, output_fn)

  files = glob.glob(input_fn)
  bad = 0
  for filename in files:
    print filename
    with open(filename, "r+b") as f:
        try:
          map = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        except Exception,w:
          print w
          continue

        for line in iter(map.readline, ""):
          if line[-1] == "\n":
            w.write(line)
          else:
            bad +=1
            print "badline #%s" % bad
  
  w.close()
