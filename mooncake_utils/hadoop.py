# -*- coding:utf-8 -*-
# @author Yue Bin 
import os,sys
import ConfigParser
base = os.path.dirname(os.path.abspath(__file__))

from termcolor import colored, cprint
import time
from mooncake_utils.date import *
from mooncake_utils.cmd import *
from mooncake_utils.alert import *

ABSPATH = os.path.dirname(os.path.abspath(sys.argv[0]))

def gen_input_date(per_day, days=10):
  ret = []
  for idx in range(days):
    ret.append(gen_date_list_by_days(gen_today(idx), per_day))

  return ret

class Hadoop:
  """Hadoop-Streaming"""

  conf = ConfigParser.ConfigParser({})
  run_date = None

  def __init__(self,run_date, conf_path = "./conf/hadoop.conf", job_suffix=""):
    cprint("begin init Hadoop using [%s]" % conf_path)
    self.conf.read(conf_path)
    self.run_date = run_date
    self.load_conf(job_suffix)
    self.alert = Alert()
    print "jobname_base [%s]" % self.job_name 

  def filter_input(self, days, input_base):
    ret = []
    for day in days:
      if not self.has_hadoop_dir(input_base % day):
        cprint("ignore not-exist input path [%s]" % (input_base % day), 'red', 'on_yellow')
        continue
      ret.append(day)

    return input_base % (','.join(ret))


  def load_conf(self, job_suffix = ""):
    self.job_name_base =  ABSPATH.split("/")[-1].strip()+job_suffix

    self.mapper_file_name = "map.py"
    self.reducer_file_name = "reduce.py"

    self.output_base = self.conf.get("hadoop","output_base")
    job_time = gen_today(only_time=True)
    self.output_path = "%s/%s/%s/%s" % (self.output_base, self.job_name_base, self.run_date, job_time)
    self.job_name = "%s_%s_%s" % (self.job_name_base, self.run_date, job_time)

    self.bin_dir = self.output_base+"tar_bin/"

    self.streaming_jar = self.conf.get("hadoop","streaming_jar","streaming")
    self.job_priority = self.conf.get("hadoop","job_priority","VERY_HIGH")
    self.map_memory_limit = self.conf.get("hadoop","map_memory_limit",2000)
    self.reduce_memory_limit = self.conf.get("hadoop","reduce_memory_limit",2000)
    self.reduce_capacity = self.conf.get("hadoop","reduce_capacity",1000)
    self.reduce_num = self.conf.get("hadoop","reduce_num",50)
    self.map_num = self.conf.get("hadoop","map_num",10000)
    self.map_capacity = self.conf.get("hadoop","map_capacity",10000)
    self.hadoop_retry_times = self.conf.getint("hadoop","hadoop_retry_times")
    self.hadoop_retry_interval = self.conf.getint("hadoop","hadoop_retry_interval")

    self.python_archive = self.conf.get("hadoop","python_archive")
    self.python_bin_path = self.conf.get("hadoop","python_bin_path")
    
    self.dfs_mapred_tar = self.bin_dir+self.job_name+".tar.gz"
    self.tar_alias_name = os.popen("basename "+self.dfs_mapred_tar+"|sed s/\.tar\.gz//").read().strip()

    self.hadoop_bin_path = self.conf.get("hadoop", "hadoop_bin_path")

    self.homepath = os.path.abspath("./")

    self.binpath = self.homepath + "/bin/"
    self.confpath = self.homepath + "/conf/"
    self.datapath = self.homepath + "/data/"
    self.logpath = self.homepath + "/log/"
    self.temppath = self.homepath + "/temp"
    
    self.local_output = "./output/%s/%s" % (self.run_date, self.output_path.split("/")[-1])

  def run(self, input_path, get_result_to_local=True, need_alert=True,
            getmerge=False, extra_cmd = "", map_param = "", reduce_param = ""):

      cprint('\n[hadoop job is preparing ...]', 'white', 'on_magenta')
      self.prepare_local_dirs()
      self.pack_upload()
      cprint("[input hdfs path]")
      print input_path
      cprint("[output hdfs path]")
      print self.output_path
      cprint("[current job name]")
      print self.job_name

      
      command = self.hadoop_bin_path+" " + self.streaming_jar 
      if extra_cmd != "":
        command += "  %s  " % extra_cmd
      command += "  -D mapred.job.map.capacity=100" +\
              "  -D mapred.job.reduce.capacity=100"+\
              "  -D mapreduce.job.name=mooncake_"+self.job_name + \
              "  -D mapreduce.job.reduces="+self.reduce_num + \
              "  -D mapreduce.job.priority="+ self.job_priority + \
              "  -D mapreduce.reduce.memory.mb="+self.reduce_memory_limit +\
              "  -D mapreduce.map.memory.mb="+self.map_memory_limit +\
              "  -D mapreduce.output.fileoutputformat.compress=false" +\
              "  -D mapred.combine.input.format.local.only=false"+\
              "  -input " + input_path.strip() +\
              "  -output "+ self.output_path.strip() +\
              "  -cacheArchive "+self.dfs_mapred_tar+"#"+self.tar_alias_name
      

      command += "  -cacheArchive "+ self.python_archive +\
              "  -mapper \" "+ self.python_bin_path + " " + self.tar_alias_name+"/bin/"+ self.mapper_file_name +" "+ map_param +"\""+\
              "  -reducer \" "+ self.python_bin_path +" " + self.tar_alias_name+"/bin/"+ self.reducer_file_name +" "+ reduce_param +"\""
  
      pretty_cmd = command.replace("  ", "\n\t")
  
      cprint("[hadoop run command]\n%s\n%s\n%s" % ("=="*50, pretty_cmd, "=="*50),"yellow","on_blue")
      
      ret = 255
      try:
          ret = self.run_hadoop_retry(command, self.output_path)
      except KeyboardInterrupt:
          cprint("KeyboardInterrupt","red","on_blue")
          ret = 256

      if ret == 0:
          cprint( "[hadoop job : %s done]" % self.job_name,"white","on_green")
  
          if get_result_to_local:
              cprint("begin hdfs result to local", "white", "on_green")
              run_cmd("mkdir -p ./output/%s" % self.run_date)
              if getmerge:
                run_cmd("%s fs -getmerge  %s ./output/%s/%s" % (self.hadoop_bin_path,
                                  self.output_path,
                                  self.run_date,
                                  self.output_path.split("/")[-1]))
              else:
                run_cmd("%s fs -get  %s ./output/%s/%s" % (self.hadoop_bin_path,
                                  self.output_path,
                                  self.run_date,
                                  self.output_path.split("/")[-1]))
              local_path = "./output/%s/%s" % (self.run_date,self.output_path.split("/")[-1])
              cprint("get hdfs result to local[%s]" % local_path, "white", "on_green")
          else:
              cprint("disable hdfs result to local", "white", "on_green")

      else:
        if need_alert:
          self.alert.send("hadoop job faild [%s]" % self.job_name,channel="#zuiyou_recsys")
        cprint("hadoop job faild",'red', attrs=['bold'], file=sys.stderr)
  
      return ret
  
  def run_hadoop_retry(self, command, opath = ''):
      cprint( "[begin hadoop job]")
      for i in range(self.hadoop_retry_times) :
          if opath != '':
              ret = run_cmd( self.hadoop_bin_path + " fs -ls " + opath )
              if ret == 0:
                  run_cmd( self.hadoop_bin_path + ' fs -rmr ' + opath)
          ret = run_cmd(command)
          if ret != 0 :
              time.sleep(self.hadoop_retry_interval)
          else:
              return 0
      return 255
  
  def copy_to_hadoop_retry(self,command, opath = ''):
      for i in range(self.hadoop_retry_times) :
          if opath != '':
              ret = run_cmd( self.hadoop_bin_path + " fs -ls " + opath )
              if ret == 0:
                  run_cmd( self.hadoop_bin_path + ' fs -rm ' + opath + '/*')
              else:
                  run_cmd( self.hadoop_bin_path + ' fs -mkdir ' + opath )
          ret = run_cmd(command)
          if ret != 0 :
              orgtime.sleep(self.hadoop_retry_interval)
          else:
              return 0
      return 255
  
  def has_hadoop_dir(self,opath):
      if opath != '':
          ret = run_cmd( self.hadoop_bin_path + " fs -ls " + opath )
          if ret == 0:
              return True
          else:
              return False
      return False
  
  def has_hadoop_file(self,path,filename):
      ret = run_cmd( self.conf.hadoop_bin_path + " fs -ls " + path + filename )
      if ret == 0:
          return True
      return False
  
  def prepare_local_dirs(self):
      cprint( "[preparing local dirs]")
      cmd = "mkdir -p ./data ./log ./temp ./conf"
      run_cmd(cmd)
  
  def pack_upload(self):
      ret = 0
      cprint( "[pack upload and put to hadoop]")
      cmd = "tar czf ./temp/"+self.tar_alias_name+" conf bin data *.py "
      ret += run_cmd(cmd)
      cmd = "%s fs -rm %s" % (self.hadoop_bin_path,self.dfs_mapred_tar)
      ret += run_cmd(cmd)
      cmd = "%s fs -put ./temp/%s %s " %(self.hadoop_bin_path,self.tar_alias_name, self.dfs_mapred_tar)
      ret += run_cmd(cmd)

  def latest_paths(self, path = "/tmp/warehouse/consume/", limit = 3):
    status, text= run_cmd_noblock("%s fs -ls %s" % (self.hadoop_bin_path,path))
    lines = text.split("\n")
    ret = []
   
    for line in reversed(lines):
      if limit <=0:
        break
      if line.find("done")!=-1:
        continue
      items = line.split(" ")
      if len(items) <4:
        continue
      ret.append(items[-1])
      limit = limit - 1
  
    return ret
  
  def get_to_local(self, src, dest):
      cmd = "%s fs -get %s %s " %(self.hadoop_bin_path, src, dest)
      return run_cmd(cmd)

  def getmerge_to_local(self, src, dest):
      cmd = "%s fs -getmerge %s %s " %(self.hadoop_bin_path, src, dest)
      return run_cmd(cmd)

def hdfs_latest_path(path = "/tmp/warehouse/consume/", limit = 3, cmd_base = "/usr/local/livers/hadoop/bin/hadoop"):
  status, text= run_cmd_noblock("%s fs -ls %s" % (cmd_base,path))
  lines = text.split("\n")
  ret = []

  for line in reversed(lines):
    if limit <=0:
      break
    if line.find("done")!=-1 or line.find("_temporary")!=-1 or line.find("_COPYING_")!=-1:
      continue
    print "========",line
    items = line.split(" ")
    if len(items) <4:
      continue
    ret.append(items[-1])
    limit = limit - 1

  return ret



if __name__ == "__main__":
  #s.run()
  pass
  #print latest_paths()
