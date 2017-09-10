# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import os,sys
import logging
import json
from mmh3 import hash as mmh3_hash
from mooncake_utils.log import *
from cityhash import CityHash32 as city_hash
import random

from libc.stdlib cimport rand, RAND_MAX

cdef class FeatureHasher:
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
  cdef int size, label, debug, print_collision, dense, use_col_index, positive_random_drop, negative_random_drop, __counter
  cdef float positive_random_drop_ratio, negative_random_drop_ratio
  cdef str hash_module
  cdef dict ins,_stat,_collision,discretization
  cdef set error_value
  cdef object logger,_hashlib

  def __init__(self,
          size = 1000, hash_module = "mmh3",
          debug = 0, print_collision = 0,
          dense = 0, use_col_index = 0,
          positive_random_drop = 0, positive_random_drop_ratio = 0.0,
          negative_random_drop = 0, negative_random_drop_ratio = 0.0,
          discretization = {}):

    self.discretization = discretization
    self.size = size
    self.debug = debug
    self.logger = get_logger(name="fea", debug=(debug == 1), wrapper=False)
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

    self.label = 0
    self.ins = {}
    self.error_value = set(["","-","\\N"])

    if hash_module == "mmh3":
      self._hashlib = mmh3_hash
    elif hash_module == "city":
      self._hashlib = city_hash
    else:
      raise Exception("unknown hash function")

  def get_ins(self):
    return self.ins

  def put(self, str key, value):
    if self.check_valid(value):
      self.ins[key] = value
      return True
    else:
      return False

  cdef __hash(self, str obj):
    cdef int ret
    ret = abs(self._hashlib(obj)) % self.size
    if self.print_collision == 1:
      if ret not in self._collision:
        self._collision[ret] = {}
      self._collision[ret][obj] = 1

    return ret

  def collision(self):
    if not self.print_collision == 1:
      return
    cdef float cnt = 0.0
    for key in self._collision:
      if len(self._collision[key]) >1:
        cnt+=1

    self.logger.info("collision[%s] total[%s] rate[%.4f%%]" % (cnt,
                    len(self._collision), 100*cnt/len(self._collision)))

  cdef string_hash(self, str key, str value):
    cdef str h_key = "%s%s" % (key,value)
    cdef int hash_value = self.__hash(h_key)

    if self.dense == 1:
      return hash_value, hash_value
    else:
      return hash_value, 1
  
  cdef __discretization(self, str key, float value):
    cdef list bins = self.discretization[key]
    cdef int ret = 0
    for b in bins:
      if value >= b:
        ret += 1 
    return key, str(ret)
  
  cdef number_hash(self, str key, float value):
    if self.dense == 1:
      hash_key = key
    else:
      if key in self.discretization:
        key, _str_value = self.__discretization(key, value) 
        return self.string_hash(key, _str_value)
      else:
        raise Exception("2333 %s" % key)

    return hash_key, value

  cdef check_valid(self, obj):
    if isinstance(obj,list) and len(obj) > 0:
      return True
    if obj == None or obj in self.error_value:
      return False
    else:
      return True

  cdef single_hash(self, str key, value, dict ret, int index):
    if isinstance(value, str):
      h_key, h_val = self.string_hash(key, value)
      #print key, value,h_key, h_val
    elif isinstance(value, float):
      if self.use_col_index == 1:
        h_key,h_val = index, value
      else:
        h_key,h_val = self.number_hash(key, value)
    else:
      raise Exception("unknown %s type %s k:%s v:%s" % (value,type(value),key,value))

    ret[h_key] = h_val

  def list_hash(self, key, obj, ret):
    for item in obj:
      self.single_hash("%s_%s" % (self.__counter, key), 
                          item, ret, self.__counter)
      self.__counter += 1

  def init(self):
    self.ins = {}
    self.label = 0
    self.__counter = 0

  def set_label(self, int value, force = False):
    self.label = value
    if force:
      return True
    cdef float _g = rand() / (RAND_MAX + 1.0)
     
    if self.negative_random_drop == 1 and \
      value <= 0 and \
      _g <= self.negative_random_drop_ratio:
        return False
    if self.positive_random_drop == 1 and \
      value > 0 and \
      _g <= self.positive_random_drop_ratio:
        return False

    return True

  def hash(self, obj = None):
    if obj:
      self.ins = obj

    cdef dict ret = {}
    cdef str msg

    for k,v in self.ins.iteritems():
      if isinstance(v, list):
        self.list_hash(k, v, ret)
      else:
        self.single_hash(k, v, ret, 0)

    if self.dense == 1:
      msg = self.dense_format(ret)
    else:
      msg = self.format(ret)

    if self.debug == 1:
      if self.label not in self._stat:
        self._stat[self.label] = 0
      self._stat[self.label] += 1
    
    return msg

  def stat(self):
    self.logger.info(self._stat)
    total = float(sum(self._stat.values()))
    for label in self._stat:
      self.logger.info("label[%s] ratio[%.4f%%]" % (label, (100*self._stat[label]/total)))

  cdef dense_format(self,dict obj):
    cdef msg = ""
    cdef list sort_obj = sorted(obj.items(), key = lambda x:x[0])

    for i in sort_obj:
      msg += "%s " % (i[1])

    msg = "%s %s" % (self.label, msg.rstrip(" "))
    return msg

  cdef str format(self, dict obj):
    cdef str msg = ' '.join(['{}:{}'.format(k,v) for k,v in obj.iteritems()])
    return "%s %s" % (self.label, msg)

if __name__ == "__main__":
  
  f = FeatureHasher(size=100000000, hash_module="city", 
              print_collision=0, debug=1, dense=0,use_col_index = 0,
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
