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
  cdef int size, label, debug, print_collision, dense, use_col_index, positive_random_drop, negative_random_drop
  cdef float positive_random_drop_ratio, negative_random_drop_ratio
  cdef str hash_module
  cdef dict _stat,_collision,discretization
  cdef set error_value,final,ins
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
    self.ins = set()
    self.final = set()
    self.error_value = set(["","-","\\N"])

    if hash_module == "mmh3":
      self._hashlib = mmh3_hash
    elif hash_module == "city":
      self._hashlib = city_hash
    else:
      raise Exception("unknown hash function")

  def get_ins(self):
    return self.ins

  def put(self, str key, str value):
    if self.check_valid(value):
      self.ins.add("%s%s" % (key,value))
      return True
    else:
      return False
  
  cdef check_valid(self, obj):
    if isinstance(obj,list) and len(obj) > 0:
      return True
    if obj == None or obj in self.error_value:
      return False
    else:
      return True

  def init(self):
    self.ins.clear()
    self.final.clear()
    self.label = 0

  def set_label(self, int value, force = False):
    self.label = value
    if force:
      return True
    #cdef float _g = rand() / (RAND_MAX + 1.0)
    # 
    #if self.negative_random_drop == 1 and \
    #  value <= 0 and \
    #  _g <= self.negative_random_drop_ratio:
    #    return False
    #if self.positive_random_drop == 1 and \
    #  value > 0 and \
    #  _g <= self.positive_random_drop_ratio:
    #    return False

    return True
  cdef string_hash(self, str key):
    cdef int hash_value = abs(self._hashlib(key)) % self.size
    self.final.add(hash_value)

  def hash(self):
    for k in self.ins:
        self.string_hash(k)
    return self.format()

  cdef str format(self):
    cdef str msg = ' '.join(['{}:1'.format(k) for k in self.final])
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
