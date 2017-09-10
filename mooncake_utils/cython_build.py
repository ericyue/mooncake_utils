from distutils.core import setup
from Cython.Build import cythonize
import glob
import os, sys
from mooncake_utils.cmd import *


ABSPATH = os.path.dirname(os.path.abspath(sys.argv[0]))

def build(folder = 'lib'):
  setarg(1, 'build_ext')
  setarg(2, '--inplace')
  to_build = glob.glob('%s/%s/*.pyx' % (ABSPATH, folder))
  
  for one in to_build:
    setup(ext_modules = cythonize(one))
  
  if len(to_build) >0:
    run_cmd('rm -rf %s/build' % ABSPATH)
    run_cmd('rm -rf %s/lib/*.c' % ABSPATH)

def build_pyx(to_build = [], delete = False):
  setarg(1, 'build_ext')
  setarg(2, '--build-lib=./lib/')

  for one in to_build:
    print one
    setup(ext_modules = cythonize(one))
    if delete:
      run_cmd('rm -rf %s' % one.replace('pyx','c'))
 
  if len(to_build) >0 and delete:
    run_cmd('rm -rf %s/build' % ABSPATH)
