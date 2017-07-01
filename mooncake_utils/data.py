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


class Storage(dict):
  """
  类似dict, 但是可以通过.来直接访问： s.a instead of s['a'] 

    >>> o = storage(a=1)
    >>> o.a
    1
    >>> o['a']
    1
    >>> o.a = 2
    >>> o['a']
    2
    >>> del o.a
    >>> o.a
    Traceback (most recent call last):
      ...
    AttributeError: 'a'
  
  """
  def __getattr__(self, key): 
    try:
      return self[key]
    except KeyError as k:
      raise AttributeError(k)
  
  def __setattr__(self, key, value): 
    self[key] = value
  
  def __delattr__(self, key):
    try:
      del self[key]
    except KeyError as k:
      raise AttributeError(k)
  
  def __repr__(self):   
    return '<Storage ' + dict.__repr__(self) + '>'

storage = Storage

def storify(mapping, *requireds, **defaults):
  """
  Creates a `storage` object from dictionary `mapping`, raising `KeyError` if
  d doesn't have all of the keys in `requireds` and using the default 
  values for keys found in `defaults`.
  For example, `storify({'a':1, 'c':3}, b=2, c=0)` will return the equivalent of
  `storage({'a':1, 'b':2, 'c':3})`.
  
  If a `storify` value is a list (e.g. multiple values in a form submission), 
  `storify` returns the last element of the list, unless the key appears in 
  `defaults` as a list. Thus:
  
    >>> storify({'a':[1, 2]}).a
    2
    >>> storify({'a':[1, 2]}, a=[]).a
    [1, 2]
    >>> storify({'a':1}, a=[]).a
    [1]
    >>> storify({}, a=[]).a
    []
  
  Similarly, if the value has a `value` attribute, `storify will return _its_
  value, unless the key appears in `defaults` as a dictionary.
  
    >>> storify({'a':storage(value=1)}).a
    1
    >>> storify({'a':storage(value=1)}, a={}).a
    <Storage {'value': 1}>
    >>> storify({}, a={}).a
    {}
    
  """
  _unicode = defaults.pop('_unicode', False)

  to_unicode = safeunicode
  if _unicode is not False and hasattr(_unicode, "__call__"):
    to_unicode = _unicode
  
  def unicodify(s):
    if _unicode and isinstance(s, str): return to_unicode(s)
    else: return s
    
  def getvalue(x):
    if hasattr(x, 'file') and hasattr(x, 'value'):
      return x.value
    elif hasattr(x, 'value'):
      return unicodify(x.value)
    else:
      return unicodify(x)
  
  stor = Storage()
  for key in requireds + tuple(mapping.keys()):
    value = mapping[key]
    if isinstance(value, list):
      if isinstance(defaults.get(key), list):
        value = [getvalue(x) for x in value]
      else:
        value = value[-1]
    if not isinstance(defaults.get(key), dict):
      value = getvalue(value)
    if isinstance(defaults.get(key), list) and not isinstance(value, list):
      value = [value]

    
    setattr(stor, key, value)

  for (key, value) in iteritems(defaults):
    result = value
    if hasattr(stor, key): 
      result = stor[key]
    if value == () and not isinstance(result, tuple): 
      result = (result,)
    setattr(stor, key, result)
  
  return stor

if __name__ == "__main__":
    pass
