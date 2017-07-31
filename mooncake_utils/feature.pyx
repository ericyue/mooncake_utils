# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
from __future__ import print_function
import os,sys
import json
from mmh3 import hash as mmh3_hash
from mooncake_utils.log import *
from cityhash import CityHash32 as city_hash
import random

class FeatureHasher():
  """
    一个简易的特征抽取框架
      
    初始化特征类参数

    :param size: 特征总维度，也就是哈希桶的数目.
    :param hash_module: 采用的哈希库，可选 ``city``, ``mmh3``
    :param debug: 打印debug信息
    :param print_collision: 是否打印冲突率
    :param dense: 是否生成稠密结果
    :param use_col_index: 特征采用下标还是用哈希结果

  """
  def __init__(self,
          size = 1000, hash_module = "mmh3",
          debug = False, print_collision = False,
          dense = False, use_col_index = False,
          positive_random_drop = False, positive_random_drop_ratio = 0.0,
          negative_random_drop = False, negative_random_drop_ratio = 0.0,
          discretization = {}):

    self.discretization = discretization
    self.size = size
    self.debug = debug
    self.logger = get_logger(name="fea", debug=debug, wrapper=False)
    self.logger.info(json.dumps(self.discretization, indent=3))

    self._stat = {}
    self._collision = {}
    self.print_collision = print_collision
    self.dense = dense
    self.use_col_index = use_col_index
    
    self.positive_random_drop = positive_random_drop
    self.positive_random_drop_ratio = positive_random_drop_ratio

    self.negative_random_drop = negative_random_drop
    self.negative_random_drop_ratio = negative_random_drop_ratio
    self.label = None
    self.ins = {}
    self.error_value = {"","-","\\N"}

    if hash_module == "mmh3":
      self._hashlib = mmh3_hash
    elif hash_module == "city":
      self._hashlib = city_hash
    else:
      raise Exception("unknown hash function")

  def put(self, key, value):
    if self.check_valid(value):
      self.ins[key] = value
      #self.logger.log(1, "add key[%s] val[%s]" % (key, self.ins[key]))
      return True
    else:
      #self.logger.log(1, "not valid key[%s] val[%s]" % (key, value))
      return False

  def __hash(self, obj):
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

    self.logger.info("collision[%s] total[%s] rate[%.4f%%]" % (cnt,
                    len(self._collision), 100*cnt/len(self._collision)))

  def string_hash(self, key, value):
    h_key = "%s%s" % (key,value)
    hash_value = self.__hash(h_key)
    #self.logger.debug("->key[%s] value[%s] / h_key[%s]->[%s] h_value[%s]->[%s]" % (
    #                key, value,h_key,hash_value,value,1))

    if self.dense:
      return hash_value, hash_value
    else:
      return hash_value, 1
  
  def __discretization(self, key, value):
    flag = False
    if key not in self.discretization:
      return flag, key, value
    bins = self.discretization[key]
    ret = 0
    for b in bins:
      if value >= b:
        ret += 1 
    flag = True
    return flag, key, str(ret)
  
  def number_hash(self, key, value):
    if self.dense:
      hash_key = key
    else:
      flag, key, value = self.__discretization(key, value) 
      if flag:
        return self.string_hash(key, value)

      hash_key = self.__hash(str(key))

    #self.logger.debug("->key[%s] value[%s] / h_key[%s]->[%s] h_value[%s]->[%s]" % (
    #                key, value, key, hash_key, value, value))

    return hash_key, value

  def check_valid(self, obj):
    if isinstance(obj,list) and len(obj) > 0:
      return True
    if obj == None or obj in self.error_value:
      return False
    else:
      return True

  def single_hash(self, key, value, ret, index):
    if isinstance(value, str):
      h_key, h_val = self.string_hash(key, value)
    elif isinstance(value, (int, float)):
      if self.use_col_index:
        h_key,h_val = index, value
      else:
        h_key,h_val = self.number_hash(key, value)
    else:
      raise Exception("unknown")

    ret[h_key] = h_val

  def list_hash(self, key, obj, ret):
    for item in obj:
      self.single_hash("%s_%s" % (self.__counter, key), 
                          item, ret, self.__counter)
      self.__counter += 1

  def init(self):
    self.ins = {}
    self.label = None
    self.__counter = 0

  def set_label(self, value):
    self.label = value
    
    _g = random.uniform(0.0, 1.0)
     
    if self.negative_random_drop and \
      value <= 0 and \
      _g <= self.negative_random_drop_ratio:
        return False
    if self.positive_random_drop and \
      value > 0 and \
      _g <= self.positive_random_drop_ratio:
        return False

    return True

  def hash(self, obj = None):
    if obj:
      self.ins = obj

    ret = {}
    for u in self.ins:
      if isinstance(self.ins[u], (list, tuple)):
        self.list_hash(u, self.ins[u], ret)
      else:
        self.single_hash(u, self.ins[u], ret, 0)

    if self.dense:
      msg = self.dense_format(self.label, ret)
    else:
      msg = self.format(self.label, ret)

    
    if self.debug:
      #self.logger.debug("%s" % msg)
      if self.label not in self._stat:
        self._stat[self.label] = 0
      self._stat[self.label] += 1
    
    return msg

  def stat(self):
    self.logger.info(self._stat)
    total = float(sum(self._stat.values()))
    for label in self._stat:
      self.logger.info("label[%s] ratio[%.4f%%]" % (label, (100*self._stat[label]/total)))

  def dense_format(self, label, obj):
    msg = ""
    sort_obj = sorted(obj.items(), key = lambda x:x[0])

    for i in sort_obj:
      msg += "%s " % (i[1])

    if label != None:
      msg = "%s %s" % (label, msg.rstrip(" "))
    return msg

  def format(self, label, obj):
    #msg = ""
    #for i in obj:
    #  msg += "%s:%s " % (i,obj[i])
    msg = ' '.join(['{}:{}'.format(k,v) for k,v in obj.iteritems()])
    return "%s %s" % (label, msg)

if __name__ == "__main__":
  
  #a={"name":"mooncake","age":12,"float":3.333,"nickname":"mooncake","-":"","__label__":343}
  #b={"name":"moake","age":12,"float":5.333,"nickname":"moake","ffff":"","asdf":"-","vec":["23"]}
  #c={"name":"moake","age":32,"float":5.33,"vec":["23","moon","-"]}
  #d={"name":"moake2","age":32,"float":5.33,"vec":["23","moon","-"]}
  #h={"name":"moake2","age":32,"float":5.33,"vec":[123.2,112.3,44.4]}

  #h1={"__label__":1,"id":"394848222","vec":[123.2,112.3,44.4]}
  #h2= {'w2v': [0.007911, -0.093373, -0.15307, -0.024283, -0.044193, 0.160349, -0.024016, 0.007423, 0.149864, 0.135744, 0.016073, 0.045109, -0.011489, -0.105786, 0.097938, -0.091035, 0.170713, 0.086309, -0.019482, -0.05405, -0.193355, -0.106077, -0.065943, 0.091179, 0.133637, -0.038045, 0.125531, 0.163907, -0.087991, 0.088282, 0.185405, -0.042518, -0.005262, 0.038919, 0.011682, 0.041738, -0.150831, 0.060612, 0.165593, -0.113252, 0.021496, -0.0505, 0.049408, -0.149098, -0.106122, 0.162164, 0.174148, 0.081231, -0.013936, -0.14077], 'uid': '66652331', 'zhuboid': '92677035', '__label__': 0}

  #h4={'w2v': [-0.289897, -0.280452, -0.089623, 0.383446, -0.143555, -0.197646, -0.259489, -0.246846, -0.00203, 0.199725, 0.242156, -0.099511, 0.165036, 0.0781, 0.353059, 0.067087, -0.013154, -0.414995, -0.049902, -0.175679], 'user_w2v': [-0.006176, -0.016736, -0.001631, -0.144528, 0.137523, 0.022742, -0.105139, -0.088976, 0.030469, 0.197202, 0.306016, -0.102512, -0.009773, -0.03308, 0.079476, -0.195709, 0.021524, -0.177388, 0.052616, 0.14131], '__label__': 0}

  #f = FeatureHasher(size=100000000, hash_module="city", 
  #            print_collision=True, debug=True, dense=False,use_col_index =True)
  #f.hash(a)
  #f.hash(b)
  #f.hash(c)
  #f.hash(d)
  #f.hash(h)
  #f.hash(h1)
  #f.hash(h2)
  #f.collision()
  #f.hash(h4)
  
  f = FeatureHasher(size=100000000, hash_module="city", 
              print_collision=True, debug=True, dense=False,use_col_index =False,
          discretization = {
              "u_fav":[0,100,300,1000,5000,10000,100000],
              "u_reg":[0,30,60,120,180,365,720]
          })

  ins = {'uid': '999999617', 'u_mobile': 'vivo', 'u_fav': 114, 'u_prov': '\xe6\xb5\x99\xe6\xb1\x9f', 'zb_biz': 'sing', 'zb_fans': '1699849', 'zb_biz_r': '106', 'aid': '603909207', 'zb_prov': '\xe8\xbe\xbd\xe5\xae\x81'}
  f.set_label(1)
  f.hash(ins)
  
  ins = {'uid': '9999996134247', 'u_mobile': 'vivo', 'u_fav': 114, 'u_prov': '\xe6\xb5\x99\xe6\xb1\x9f', 'zb_biz': 'sing', 'zb_fans': '1699849', 'zb_biz_r': '106', 'aid': '603909207', 'zb_prov': '\xe8\xbe\xbd\xe5\xae\x81'}
  f.set_label(0)
  f.hash(ins)
  #f.collision()
