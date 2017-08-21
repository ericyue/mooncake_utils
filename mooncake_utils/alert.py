#encoding: utf-8
import os,sys
reload(sys)
sys.setdefaultencoding("utf-8")
base = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base)
from mooncake_utils.log import *
from os.path import basename
from slacker import Slacker
import yaml
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.MIMEText import MIMEText

default_conf_path = os.getenv('MOONCAKE_UTILS_ALERT_CONF', base)

class Alert:
  """
  alert module, 目前集成了Slack和邮箱两个途径报警
  
  使用前请先配置好alert.yaml,并且设置环境变量MOONCAKE_UTILS_ALERT_CONF指向其路径。 
  
  """

  logger = get_logger(debug=True, name='mu.alert', with_file=False)
  conf_path = default_conf_path
  slack = None

  enable_slack = True
  enable_mail = True

  default_channel = None
  
  def __init__(self, conf_path = None):
    if conf_path:
      self.conf_path = conf_path

    self.logger.info("init Alert with conf[{}]".format(self.conf_path))
    self.conf = yaml.load(open(self.conf_path, 'r'))
    
    self.enable_slack = self.conf['slack'].get('enable', True)
    self.enable_mail = self.conf['mail'].get('enable', True)
    self.slack = Slacker(self.conf['slack']['token'])
    self.default_channel = self.conf['slack']['default_channel']
    self.enable_log = False

  def send(self, msg, channel = None, enable_slack = True, enable_mail = True, logger = False):
    if not channel:
      channel = self.default_channel

    if self.enable_log or logger:
      self.logger.info(msg)

    if self.enable_slack and enable_slack:
      self.send_slack(msg, channel)
    
    if self.enable_mail and enable_mail:
      self.send_mail(msg, channel )

  def send_slack(self, msg, channel):
    attempt = 0
    while attempt < self.conf['slack']['retry']:
      try:
        self.slack.chat.post_message(channel, msg)
        return True
      except Exception, w:
        self.logger.exception("retry#%s send slack due to [%s]" %(attempt,w))
        attempt += 1
    return False 


  def send_mail(self, message, channel, flist=[], to_list = None):
    subject = "[%s] #%s# %s" % (self.conf['mail'].get('subject_prefix',''), channel, message)
    username = self.conf['mail']['username']
    password = self.conf['mail']['password']
  
    msg = MIMEMultipart()
    msg['From'] = username
    if to_list:
      msg['To'] = to_list
    else:
      msg['To'] = self.conf['mail']['receiver']
    msg['Subject'] = subject 
    msg.attach(MIMEText(message))
    for f in flist:
      with open(f, "rb") as fp:
        att = MIMEApplication(fp.read())
        att.add_header('Content-Disposition','attachment',filename=basename(f))
        msg.attach(att)

    attempt = 0
    while attempt < self.conf['slack']['retry']:
      try:
        mailServer = smtplib.SMTP_SSL(self.conf['mail']['smtp_server'],
                    self.conf['mail']['smtp_port'])
        mailServer.ehlo()
        mailServer.login(username, password)
        mailServer.sendmail(username, msg['To'].split(','), msg.as_string())
        mailServer.close()
        return True
      except Exception as e:
        attempt += 1
        self.logger.exception(e)	

    return False

if __name__ == "__main__":
  if len(sys.argv) <= 1: 
    exit()

  a = Alert()
  msg = sys.argv[1]
  try:
    channel = sys.argv[2]
  except:
    channel = "#yy_online"

  a.send(msg, channel=channel,logger=True)
