# -*- coding:utf-8 -*-

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import time
import datetime
from dateutil.parser import parse

def gen_date_list(begin, days=7):
  if type(begin) != datetime.datetime:
    begin = parse(begin)
 
  ret = []
  for i in range(days):
    dt = str(begin - datetime.timedelta(i+1)).split()[0].replace('-','')
    ret.append(dt) 

  return ret

def gen_latest_date_list(begin, end, join = False, exclude = []):
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


def gen_date_list(begin, end, join = False, exclude = []):
  if type(begin) != datetime.datetime:
    begin = parse(begin)
  if type(end) != datetime.datetime:
    end = parse(end)
  if begin > end:
    return []

  dt_exclude = []
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


def datetime2timestamp(date):
  return int(time.mktime(date.timetuple()))

def str2date(date):
  return parse(date)

def str2datetime(date, date_format='%Y-%m-%d %H:%M:%S'):
  return datetime.datetime.strptime(date, date_format)

def timestamp2datetime(timestamp):
  x = time.localtime(timestamp)
  return time.strftime('%Y-%m-%d %H:%M:%S',x)

def gen_today(delta = 1, raw = False, short = True, with_time = False, only_time = False):
  dt = datetime.datetime.now()-datetime.timedelta(delta)
  if raw:
    return dt

  if short:
    str_dt = dt.strftime('%Y%m%d%H%M%S')
  if only_time:
    str_dt = str_dt[8:]
    return str_dt

  if not with_time:
    str_dt = str_dt[:8]
  return str_dt

def int2datestr(date, diff):
  if date == None:
      sec = time.time()
  else:
      sd = time.strptime(date,'%Y%m%d')
      sec = time.mktime(sd)
  sec = sec - 86400 * diff  # 1 day is 86400 second
  ltime = time.localtime(sec)
  return time.strftime('%Y%m%d',ltime)

if __name__ == "__main__":
  print gen_date_list(0, 4)
  print gen_date_list(0, 4, join=True)
  print datetime2timestamp(datetime.datetime.now())
  print str2datetime('2017-04-03 01:11:11')
  print timestamp2datetime(1496820643)
  print get_today(with_time=False, delta=10)
  print gen_date_list(begin='20170201',end='20170303', exclude=['20170204'])
