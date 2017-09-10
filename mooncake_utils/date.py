# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import time
import datetime
from dateutil.parser import parse

def datediff(dt, base = None, unit = 'day'):
  """
    用来计算分两个datetime的相差天数或者分钟数。

    如果不显示的指定base，则计算与当前时间的diff
    
    :param dt: input date ``必须是datetime``
    :param base: 基准日期
    :param unit: 计算粒度,可选的有 ``day``, ``hour`` , ``minute`` , ``second`` 
  """
  if not base:
    base = datetime.datetime.now()

  delta = base - dt
  if unit == "day":
    diff = delta.days
  elif unit == "hour":
    diff = delta.total_seconds()//3600
  elif unit == "minute":
    diff = (delta.total_seconds()//60)
  elif unit == "second":
    diff = delta.total_seconds()
  else:
    diff = None

  return diff

def gen_date_list_by_days(begin = None, days=7, join = False,  exclude = [], include_begin_day = False, format='%Y%m%d'):
  """
    生成指定的时间list

    :param begin: 起始日期，默认要求datetime类型，如果是str会自动尝试解析成datetime
    :param days: 生成距离begin的最近几天
    :param join: 是否将list按照,拼接。hadoop的input目录常用
    :param exclude: 去除日期。 统计数据时常用，统计最近7天，不过某几天数据损坏需要去掉某几天。
    :param include_begin_day: 是否包含begin这一天
    
    >>> gen_date_list_by_days(begin ='20170620', days=3) # 今天2017-6-24
    ['20170619', '20170618', '20170617']
    
    >>> gen_date_list_by_days(begin ='20170620', days=3, exclude=['20170618']) # 今天2017-6-24
    ['20170619', '20170617']

    >>> gen_date_list_by_days(days=3) # 今天2017-6-24
    ['20170623', '20170622', '20170621']

    >>> gen_date_list_by_days(days=3, join = True) # 今天2017-6-24
    '20170623,20170622,20170621'
  """
  if not begin:
    begin = gen_today(delta=0, raw = True)

  if type(begin) != datetime.datetime:
    begin = parse(begin)

  if include_begin_day:
    delta_inc = 0
  else:
    delta_inc = 1
  
  ret = []
  for i in range(days):
    dt = begin - datetime.timedelta(i + delta_inc)
    dt = dt.strftime(format)
    if dt in exclude:
      continue
    ret.append(dt) 

  if join:
    ret = ",".join(ret)

  return ret


def gen_date_list(begin, end, join = False, exclude = [], exclude_today = True):
  """
    生成指定起始到结束之内的日期列表，注意同gen_date_list_by_days对比。

  """
  if end == None:
    end = datetime.datetime.now()
  if type(begin) != datetime.datetime:
    begin = parse(begin)
  if type(end) != datetime.datetime:
    end = parse(end)
  if begin > end:
    return []

  dt_exclude = []
  if exclude_today:
    dt_exclude.append(parse(datetime.datetime.now().strftime('%Y%m%d')))
  for day in exclude:
    if type(day) != datetime.datetime:
      day = parse(day)

    dt_exclude.append(day)

  ret = []
  while begin <= end:
      if begin in dt_exclude:
        begin = begin + datetime.timedelta(days=(1))
        continue
      dt = begin.strftime('%Y%m%d')
      ret.append(dt)
      begin = begin + datetime.timedelta(days=(1))

  if join:
    return ",".join(ret)
  return ret

def gen_latest_date_list(begin, end, join = False, exclude = []):
  """
    已废弃，请用gen_date_list 
  """
  ret = []
  for i in range(begin+1, end):
      dt = datetime.datetime.now() - datetime.timedelta(days=(i))
      dt = dt.strftime('%Y%m%d')
      if dt in exclude:
        continue
      ret.append(dt)
  if join:
    return ",".join(ret)
  return ret

def datetime2timestamp(date):
  """
    datetime格式转时间戳(int)
  """
  return int(time.mktime(date.timetuple()))

def str2date(date):
  """
    str解析成datetime, 会尝试各种格式解析

    >>> str2date('20170101')
    <type 'datetime.datetime'>

    >>> str2date('2017-01-01')
    <type 'datetime.datetime'>

    >>> str2date('2017-01-01 11:11:22')
    <type 'datetime.datetime'>
  """
  return parse(date)

def str2datetime(date, date_format='%Y-%m-%d %H:%M:%S'):
  """
    固定格式的str转datetime
  """
  return datetime.datetime.strptime(date, date_format)

def timestamp2datetime(timestamp):
  """
    时间戳转datetime
    
    :param timestamp: str or int or float
    :returns: 返回datetime
  """
  if type(timestamp) == str:
    timestamp = int(float(timestamp))
  x = time.localtime(timestamp)
  return str2date(time.strftime('%Y-%m-%d %H:%M:%S',x))

def gen_today(delta = 1, raw = False, short = True, with_time = False, only_time = False):
  """
    生成当天的日期
    
    :param delta: 时间偏移量
    :param raw: 如果设置为``True``,返回``datetime``类型, 否则返回``str``类型
    :param short: 若为True, 会返回精简时间，如20170101，否则返回2017-01-01
    :param with_time: 是否添加时间，否则只返回日期
    :param only_time: 是否只返回时间，不加日期

    >>> gen_today(delta=0, with_time=True)
    20170624145222
    >>> gen_today(delta=0, with_time=True, short=False)
    2017-06-24 15:06:12
    >>> gen_today(delta=0, only_time=True, short=False)
    15:06:12
    >>> gen_today(delta=0, only_time=True)
    150612
    >>> gen_today(delta=0, with_time=False)
    20170624
    >>> gen_today(delta=1, with_time=False)
    20170623

  """

  dt = datetime.datetime.now()-datetime.timedelta(delta)
  if raw:
    return dt

  if short:
    if only_time:
      str_dt = dt.strftime('%H%M%S')
    else:
      if not with_time:
        str_dt = dt.strftime('%Y%m%d')
      else:
        str_dt = dt.strftime('%Y%m%d%H%M%S')

  else:
    if only_time:
      str_dt = dt.strftime('%H:%M:%S')
    else:
      if not with_time:
        str_dt = dt.strftime('%Y-%m-%d')
      else:
        str_dt = dt.strftime('%Y-%m-%d %H:%M:%S')

  return str_dt

if __name__ == "__main__":
  #print gen_date_list(0, 4)
  #print gen_date_list(0, 4, join=True)
  #print datetime2timestamp(datetime.datetime.now())
  #print str2datetime('2017-04-03 01:11:11')
  #print timestamp2datetime(1496820643)
  #print gen_date_list_by_days(begin ='20170620', days=3, exclude=['20170618'])
  #print type(str2date('20170101'))
  #print gen_today(delta=0,with_time=True,short=False)
  print gen_today(only_time=True)
  #print gen_today(delta=0, only_time=True, with_time=True)
  print gen_date_list(begin='20170301',end='20170303', exclude=['20170204'])
  print gen_date_list(begin='20170301',end=None, exclude=['20170204'],exclude_today=False)
