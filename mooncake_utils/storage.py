# -*- coding:utf-8 -*-
#
# Copyright (c) 2017 mooncake. All Rights Reserved
####
# @brief
# @author Eric Yue ( hi.moonlight@gmail.com )
# @version 0.0.1
import os, sys
import datetime, time, re, json

class Storage(dict):
  """
  A Storage object is like a dictionary except `obj.foo` can be used
  in addition to `obj['foo']`.
  
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

  # if _unicode is callable object, use it convert a string to unicode.
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
