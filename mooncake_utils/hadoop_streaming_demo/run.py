#-*- coding:utf-8 -*-
import os,sys
import multiprocessing
from mooncake_utils.cmd import run_cmd
from mooncake_utils.hadoop import Hadoop
from mooncake_utils.date import *
import gflags
import yaml

conf = yaml.load(open('./conf/job.yaml','r'))

gflags.DEFINE_string("date", gen_today(delta = 1), "job date to run")
gflags.DEFINE_bool("debug", True, "debug or not")
gflags.DEFINE_bool("with_mu", True, "with mooncake_utils or not")
gflags.DEFINE_integer("days", 1, "num of jobs")
gflags.DEFINE_integer("per_job_days", 1, "days in one job")

FLAGS = gflags.FLAGS

def available_task(delta_date):
  '''
  启动任务process

  :param delta_date: 任务启动时间

  '''
  hadoop = Hadoop(run_date=delta_date)
  input_dt = gen_date_list_by_days(str2date(delta_date),
                                  days = FLAGS.per_job_days,
                                  join=True,
                                  include_begin_day=True)


  #TODO  change xxxxxxx
  input_path = "/xxxxxxxx/{%s}/ "  % input_dt
  output_path = hadoop.local_output

  if hadoop.run(input_path) != 0:
    print "error ..."
  else:
    post()

def post():
  '''
    streaming任务完成后做的一些后续操作
  '''
  pass

def multi_run():
    pool = multiprocessing.Pool(processes = min(1, FLAGS.days))
    diff_day = datediff(str2date(FLAGS.date))
    for i in range(FLAGS.days) :
      dt =  gen_today(delta=(i+diff_day), raw=False)
      p = pool.apply_async(available_task, (dt, ))
    pool.close()
    pool.join()

if __name__ == "__main__":
    FLAGS(sys.argv)
    if FLAGS.with_mu:
      cmd = "rm -rf ./bin/mooncake_utils && cp -r %s ./bin" % conf['job']['mooncake_utils_path']
      run_cmd(cmd)

    multi_run()
