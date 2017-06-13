from __future__ import print_function
import os,sys
from mmh3 import hash as mmh3_hash
from cityhash import CityHash32 as city_hash

class feature_hasher():
  def __init__(self, size=1000, hash_module="city", debug=False, print_collision=False, dense = False, use_col_index = False):
    self.size = size
    self.debug = debug
    self._collision = {}
    self.print_collision = print_collision
    self.dense = dense
    self.use_col_index = use_col_index

    if hash_module == "mmh3":
      self._hashlib = mmh3_hash
    elif hash_module == "city":
      self._hashlib = city_hash
    else:
      raise Exception("unknown hash function")

  def __hash__(self, obj):
    ret = abs(self._hashlib(obj)) % self.size
    if self.print_collision:
      if ret not in self._collision:
        self._collision[ret] = {}
      self._collision[ret][obj] = 1

    return ret

  def collision(self):
    if not self.print_collision:
      return
    cnt = 0.0
    for key in self._collision:
      if len(self._collision[key]) >1:
        cnt+=1

    print("collision[%s] total[%s] rate[%.4f%%]" % (cnt,len(self._collision), 100*cnt/len(self._collision)))

  def string_hash(self, key, value):
    h_key = key+value
    hash_value = self.__hash__(h_key)
    if self.debug:
      print("  string_hash   ->key[%s] value[%s] / h_key[%s]->[%s] h_value[%s]->[%s]" % (key, value,h_key,hash_value,value,1))

    if self.dense:
      return hash_value, hash_value
    else:
      return hash_value, 1

  def number_hash(self, key, value):
    if self.dense:
      hash_key = key
    else:

      hash_key = self.__hash__(str(key))
    if self.debug:
      print("  number_hash   ->key[%s] value[%s] / h_key[%s]->[%s] h_value[%s]->[%s]" % (key, value,key,hash_key,value,value))
    return hash_key, value

  def check_valid(self, obj):
    if obj.strip() in ["","-","0"]:
      return False
    else:
      return True

  def single_hash(self, key, value, ret, index):
    if type(value) == str:
      if not self.check_valid(value):
        if self.debug:
          print("invalid value[%s]" % value)
        return
      h_key,h_val = self.string_hash(key,value)
    elif type(value) in [int, float]:
      if self.use_col_index:
        h_key,h_val = index,value
      else:
        h_key,h_val = self.number_hash(key,value)
    else:
      raise Exception("unknown")

    ret[h_key] = h_val

  def list_hash(self, key, obj, ret):
    cnt = 0
    for item in obj:
      self.single_hash("%s_%s" % (cnt,key), item, ret, cnt)
      cnt+=1

  def hash(self, obj):
    if self.debug:
      print("input",obj)
    ret = {}
    label = None
    for u in obj:
      if not self.check_valid(u):
        continue

      if u == "__label__" :
        label = obj[u]
        continue
      if type(obj[u]) in [list,tuple]:
        self.list_hash(u, obj[u], ret)
      else:
        self.single_hash(u, obj[u], ret, 0)

    if self.dense:
      msg = self.dense_format(label, ret)
    else:
      msg = self.format(label, ret)
    if self.debug:
      print("output %s\n" % msg)
    return msg

  def dense_format(self, label, obj):
    msg = ""
    sort_obj = sorted(obj.items(), key = lambda x:x[0])

    for i in sort_obj:
      msg += "%s " % (i[1])

    if label != None:
      msg = "%s %s" % (label, msg.rstrip(" "))
    return msg

  def format(self, label, obj):
    msg = ""
    for i in obj:
      msg += "%s:%s " % (i,obj[i])

    if label != None:
      msg = "%s %s" % (label, msg.rstrip(" "))
    return msg

if __name__ == "__main__":
  a={"name":"mooncake","age":12,"float":3.333,"nickname":"mooncake","-":"","__label__":343}
  b={"name":"moake","age":12,"float":5.333,"nickname":"moake","ffff":"","asdf":"-","vec":["23"]}
  c={"name":"moake","age":32,"float":5.33,"vec":["23","moon","-"]}
  d={"name":"moake2","age":32,"float":5.33,"vec":["23","moon","-"]}
  e={"name":"moake2","age":32,"float":5.33,"vec":[1.2,2.3,444.4]}
  h={"name":"moake2","age":32,"float":5.33,"vec":[123.2,112.3,44.4]}

  h1={"__label__":1,"id":"394848222","vec":[123.2,112.3,44.4]}
  h2= {'w2v': [0.007911, -0.093373, -0.15307, -0.024283, -0.044193, 0.160349, -0.024016, 0.007423, 0.149864, 0.135744, 0.016073, 0.045109, -0.011489, -0.105786, 0.097938, -0.091035, 0.170713, 0.086309, -0.019482, -0.05405, -0.193355, -0.106077, -0.065943, 0.091179, 0.133637, -0.038045, 0.125531, 0.163907, -0.087991, 0.088282, 0.185405, -0.042518, -0.005262, 0.038919, 0.011682, 0.041738, -0.150831, 0.060612, 0.165593, -0.113252, 0.021496, -0.0505, 0.049408, -0.149098, -0.106122, 0.162164, 0.174148, 0.081231, -0.013936, -0.14077], 'uid': '66652331', 'zhuboid': '92677035', '__label__': 0}

  f = feature_hasher(size=100000000,hash_module="city",print_collision=True, debug=True, dense=False,use_col_index =True)
  f.hash(a)
  f.hash(b)
  f.hash(c)
  f.hash(d)
  f.hash(d)
  f.hash(h)
  f.hash(h1)
  f.hash(h2)
  f.collision()
