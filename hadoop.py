# -*- coding:utf-8 -*-
# @author Yue Bin 
import os,sys
import ConfigParser
base = os.path.dirname(os.path.abspath(__file__))

from termcolor import colored, cprint
from alert import *
from mooncake_utils.date import *

ABSPATH = os.path.dirname(os.path.abspath(sys.argv[0]))

def gen_input_date(per_day, days=10):
  ret = []
  for idx in range(days):
    ret.append(gen_date_list_by_days(gen_today(idx), per_day))

  return ret

class Hadoop:
  conf = ConfigParser.ConfigParser({})

  def __init__(self, conf_path = "./hadoop.conf"):
    cprint("begin init Hadoop using [%s]" % conf_path, 'white', 'on_red')
    self.conf.read(conf_path)
    self.load_conf()
    print "jobname_base [%s]" % self.job_name 

  def load_conf(self):
    alert_mail = self.conf.get("alert","mail","hi.moonlight@gmail.com")
    alert_phone = self.conf.get("alert","phone","23333")

    self.job_name = ABSPATH.split("/")[-1].strip()
    self.job_name_base = self.job_name
    self.mapper_file_name = "map.py"
    self.reducer_file_name = "reduce.py"

    self.output_base = self.conf.get("hadoop","output_base")
    self.output_path = "%s/%s/" % (self.output_base, self.job_name_base)

    self.bin_dir = self.output_base+"tar_bin/"
    self.dfs_mapred_tar = self.bin_dir+self.job_name+".tar.gz"

    self.job_priority = self.conf.get("hadoop","job_priority","VERY_HIGH")
    self.memory_limit = self.conf.get("hadoop","memory_limit",2000)
    self.reduce_capacity = self.conf.get("hadoop","reduce_capacity",1000)
    self.reduce_num = self.conf.get("hadoop","reduce_num",50)
    self.map_num = self.conf.get("hadoop","map_num",10000)
    self.map_capacity = self.conf.get("hadoop","map_capacity",10000)
    self.hadoop_retry_times = self.conf.getint("hadoop","hadoop_retry_times")
    self.hadoop_retry_interval = self.conf.getint("hadoop","hadoop_retry_interval")

    self.interpreter_archive = self.conf.get("hadoop","python_archive")
    self.script_interpreter = self.conf.get("hadoop","python")
    self.tar_alias_name = os.popen("basename "+self.dfs_mapred_tar+"|sed s/\.tar\.gz//").read().strip()

    self.hadoop_home_path = self.conf.get("hadoop","hadoop_home_path")
    self.hadoop_bin_path = self.conf.get("hadoop","hadoop_bin_path")

    self.homepath = os.path.abspath("./")

    self.binpath = self.homepath + "/bin/"
    self.confpath = self.homepath + "/conf/"
    self.datapath = self.homepath + "/data/"
    self.logpath = self.homepath + "/log/"
    self.temppath = self.homepath + "/temp"

  def run(self, date, input_path, output_path,
  						get_result_to_local=True,
  						online=False,
  						need_alert=False,
  						getmerge=False):
      self.job_name = "%s_%s_%s" % (self.job_name_base, date, output_path.split("/")[-2])

      self.conf.reload()
      cprint('\n[hadoop job is preparing ...]\n', 'white', 'on_red')
      print ""
      prepare_local_dirs()
      pack_upload()
      cprint("[input hdfs path]" ,'white', 'on_red')
      print input_path.strip()
      cprint("[output hdfs path]",'white', 'on_red')
      print output_path.strip()
      cprint("[current job name]",'white', 'on_red')
      print self.conf.job_name.strip()
      
      command = self.conf.hadoop_bin_path+" streaming " 
      command += "  -D mapred.job.map.capacity=10000" +\
              "  -D mapred.job.reduce.capacity=10000"+\
              "  -D mapred.min.split.size=1024000000"+ \
              "  -D mapred.job.priority="+ self.conf.job_priority + \
              "  -D mapred.reduce.tasks="+self.conf.reduce_num + \
              "  -D mapred.job.name=mooncake_\""+self.conf.job_name+"\"" + \
              "  -D mapreduce.reduce.memory.mb=3500 " +\
              "  -D mapreduce.map.memory.mb=3500 " +\
              "  -D mapreduce.reduce.java.opts=-Xmx3500M " +\
              "  -D mapreduce.map.java.opts=-Xmx3500M " +\
              "  -D mapred.child.java.opts=-Xmx3500m " +\
              "  -D mapred.combine.input.format.local.only=false"+\
              "  -input " + input_path.strip() +\
              "  -output "+ output_path.strip() +\
              "  -cacheArchive "+self.dfs_mapred_tar+"#"+self.tar_alias_name

      command += "  -cacheArchive "+ self.python_archive +\
              "  -mapper \" "+" "+ self.python_bin_path + " " + self.tar_alias_name+"/bin/"+ self.mapper_file_name +" "+self.job_name +"\""+\
              "  -reducer \" "+" "+ self.python_bin_path + self.tar_alias_name+"/bin/"+ self.reducer_file_name +" "+self.job_name +"\""
  
      pretty_cmd = command.replace("  ", "\n\t")
  
      cprint("[hadoop run command]\n%s\n%s\n%s" % ("=="*50, pretty_cmd, "=="*50),"yellow","on_blue")
      
      ret = -1
      try:
          ret = self.run_hadoop_retry(command, output_path)
      except KeyboardInterrupt:
          cprint("KeyboardInterrupt","red","on_blue")
          ret = -1
  
      if ret == 0:
          cprint( "[hadoop job : %s done]" % self.job_name,"white","on_green")
  
          if get_result_to_local:
              os.system("mkdir -p ./output/%s" % date)
              if get_merge:
                os.system("%s/bin/hadoop fs -getmerge  %s ./output/%s/%s" % (self.hadoop_home_path,output_path,date,output_path.split("/")[-2]))
              else:
                os.system("%s/bin/hadoop fs -get  %s ./output/%s/%s" % (self.hadoop_home_path,output_path,date,output_path.split("/")[-2]))
              local_path = "./output/%s/%s" % (date,output_path.split("/")[-2])
              cprint("get hdfs result to local[%s]" % local_path,"white","on_green")
      else:
        if need_alert:
          alert("hadoop job faild [%s]" % self.job_name,channel="#hadoop-job")
        cprint("hadoop job faild",'red', attrs=['bold'], file=sys.stderr)
  
      return ret
  
  def run_hadoop_retry(self, command, opath = ''):
      cprint( "[run hadoop job]","white","on_red")
      for i in range(self.conf.hadoop_retry_times) :
          if opath != '':
              ret = os.system( self.conf.hadoop_home_path + "/bin/hadoop fs -ls " + opath )
              if ret == 0:
                  os.system( self.conf.hadoop_home_path + '/bin/hadoop fs -rmr ' + opath)
          ret = os.system(command)
          if ret != 0 :
              orgtime.sleep(self.conf.hadoop_retry_interval)
          else:
              return 0
      return -1
  
  def copy_to_hadoop_retry(self,command, opath = ''):
      for i in range(self.conf.hadoop_retry_times) :
          if opath != '':
              ret = os.system( self.conf.hadoop_home_path + "/bin/hadoop fs -ls " + opath )
              if ret == 0:
                  os.system( self.conf.hadoop_home_path + '/bin/hadoop fs -rm ' + opath + '/*')
              else:
                  os.system( self.conf.hadoop_home_path + '/bin/hadoop fs -mkdir ' + opath )
          ret = os.system(command)
          if ret != 0 :
              orgtime.sleep(self.conf.hadoop_retry_interval)
          else:
              return 0
      return -1
  
  def run_local_retry(command, opath = ''):
      for i in range(self.conf.hadoop_retry_times) :
          if opath != '' and opath != '/':
              make_new_out(opath)
          ret = os.system(command)
          if ret != 0 :
              orgtime.sleep(self.conf.hadoop_retry_interval)
          else:
              return 0
      return -1
  
  def has_hadoop_dir(opath):
      if opath != '':
          ret = os.system( self.conf.hadoop_home_path + "/bin/hadoop fs -ls " + opath )
          if ret == 0:
              return True
          else:
              return False
      return False
  
  def has_hadoop_file(path,filename):
      cprint( '[check hadoop file %s/%s]' % (path,filename),"white","on_red")
      ret = os.system( self.conf.hadoop_home_path + "/bin/hadoop fs -ls " + path + filename )
      if ret == 0:
          return True
      return False
  
  def make_new_out(path):
      if os.path.isdir(path):
          os.system('rm -rf ' + path)
          print 'make new out',path,'already exist'
      os.system('mkdir -p ' + path)
  
  def prepare_local_dirs():
      cprint( "[preparing local dirs]","white","on_red")
      cmd = "mkdir -p ./data ./self.conf ./log ./temp"
      print cmd
      os.system(cmd)
  
  def pack_upload():
      cprint( "[pack upload and put to hadoop]","white","on_red")
      cmd = "tar czf ./temp/"+self.conf.tar_alias_name+" self.conf bin data *.py "
      print cmd
      os.system(cmd)
      cmd = "%s/bin/hadoop fs -rm %s" % (self.conf.hadoop_home_path,self.conf.dfs_mapred_tar)
      print cmd
      os.system(cmd)
      cmd = "%s/bin/hadoop fs -put ./temp/%s %s " %(self.conf.hadoop_home_path,self.conf.tar_alias_name, self.conf.dfs_mapred_tar)
      print cmd
      os.system(cmd)
  def reload(self):
      self.binpath = self.homepath + "/bin/"
      self.self.confpath = self.homepath + "/self.conf/"
      self.datapath = self.homepath + "/data/"
      self.logpath = self.homepath + "/log/"
      self.temppath = self.homepath + "/temp"
      self.bin_dir = self.output_base+"tar_bin/"
      self.dfs_mapred_tar = self.bin_dir+self.job_name+".tar.gz"
      self.tar_alias_name = os.popen("basename "+self.dfs_mapred_tar+"|sed s/\.tar\.gz//").read().strip()

if __name__ == "__main__":
  s = Hadoop()
  print s.job_name
  s.run()
