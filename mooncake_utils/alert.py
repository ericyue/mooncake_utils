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

"""
alert module
"""

def alert(msg, mail = True, slack = True, channel="#mooncake"):
    """
    用来发送报警,支持邮件和Slack

    :param msg: 报警内容
    :param mail: 是否开启邮件报警
    :param slack: 是否开启slack报警
    :param channel: slack报警的接收频道

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
