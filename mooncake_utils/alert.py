#encoding: utf-8
import os,sys
reload(sys)
sys.setdefaultencoding("utf-8")
base = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base)
from slacker import Slacker
import ConfigParser
  
conf = ConfigParser.ConfigParser({})
conf.read("%s/conf/alert.conf" % base)
channel = conf.get("slack","channel")
token = conf.get("slack","token")

slack = Slacker(token)

def alert(msg, mail = True, slack = True, channel="#mooncake"):
    """This function does something.

      Args:
         msg (str):  报警内容
  
         mail (bool):  是否发送邮件
  
         slack (bool):  是否发送Slack
  
         channel (str):  Slack的接收频道
  
      Returns:
         bool.  The return code::
  
      Raises:
         NetworkError

    """

    print msg
    try:
      ret = send_slack(msg, channel)
    except Exception, w:
      print w
      pass

    return ret

def send_slack(msg, channel="#mooncake"):
  """Send Message to Slack

  :param msg: 报警内容. 
  :type msg: str. 
  :param channel: 接收报警的频道. 
  :type channel: str. 
  :returns: bool -- the return code. 
  :raises: AttributeError, NetworkError

  """ 
  attempt = 0
  while attempt < 3:
    try:
      slack.chat.post_message(channel, msg)
      return True
    except Exception,w:
      print "retry#%s send slack due to [%s]" %(attempt,w)
      attempt+=1
  return False 

if __name__ == "__main__":
  pass
