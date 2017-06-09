#!/usr/bin/python
#encoding: utf-8
import os,sys
reload(sys)
sys.setdefaultencoding("utf-8")
base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base)
from slacker import Slacker
import ConfigParser
  
conf = ConfigParser.ConfigParser({})
conf.read("./conf/alert.conf")
channel = conf.get("slack","channel")
token = conf.get("slack","token")

slack = Slacker(token)


def alert(msg, mail = True, slack = True, channel="#mooncake"):
    print msg
    try:
      send_slack(msg, channel)
    except Exception, w:
      print w
      pass

def send_slack(msg, channel="#mooncake"):
  attempt = 0
  while attempt < 3:
    try:
      slack.chat.post_message(channel, msg)
      break
    except Exception,w:
      print "retry#%s send slack due to [%s]" %(attempt,w)
      attempt+=1

if __name__ == "__main__":
  pass
  #alert("im alert")
