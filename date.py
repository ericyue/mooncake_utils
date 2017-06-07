# -*- coding:utf-8 -*-
#

# @author Yue Bin ( hi.moonlight@gmail.com )
# @date 2017-06-07
import time
import datetime

def gen_date_list(begin, end, join = False, exclude = []):
  ret = []
  for i in range(begin+1, end):
      dt = str(datetime.datetime.now() - datetime.timedelta(days=(i))).split(" ")[0].replace("-","")
      if dt in exclude:
        continue
      ret.append(dt)
  if join:
    return ",".join(ret)
  return ret

def datetime2timestamp(date):
  return int(time.mktime(date.timetuple()))

def str2datetime(date, date_format='%Y-%m-%d %H:%M:%S'):
  return datetime.datetime.strptime(date, date_format)

def timestamp2datetime(timestamp):
  x = time.localtime(timestamp)
  return time.strftime('%Y-%m-%d %H:%M:%S',x)

def get_today(delta = 1, raw = False, short = True, with_time = False):
  dt = datetime.datetime.now()-datetime.timedelta(delta)
  if raw:
    return dt

  if short:
    str_dt = dt.strftime('%Y%m%d%H%M%S')
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
  print get_today(with_time=False,delta=10)
