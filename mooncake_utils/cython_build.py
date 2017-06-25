from distutils.core import setup
from Cython.Build import cythonize
import glob
import os, sys
from mooncake_utils.cmd import *
setarg(1, 'build_ext')
setarg(2, '--inplace')

ABSPATH = os.path.dirname(os.path.abspath(sys.argv[0]))

def build():
  to_build = glob.glob('%s/lib/*.pyx' % ABSPATH)
  
  for one in to_build:
    setup(ext_modules = cythonize(one))
  
  run_cmd('rm -rf %s/build' % ABSPATH)
  run_cmd('rm -rf %s/lib/*.c' % ABSPATH)
