#!/usr/bin/python
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
       name (str):  The name to use.

    Kwargs:
       state (bool): Current state to be in.

    Returns:
       int.  The return code::

          0 -- Success!
          1 -- No good.
          2 -- Try again.

    Raises:
       AttributeError, KeyError

    A really great idea.  A way you might use me is

    >>> print public_fn_with_googley_docstring(name='foo', state=None)
    0

    BTW, this always returns 0.  **NEVER** use with :class:`MyPublicClass`.

    """

    print msg
    try:
      send_slack(msg, channel)
    except Exception, w:
      print w
      pass

def send_slack(msg, channel="#mooncake"):
  """This function does something.

  :param name: The name to use. 
  :type name: str. 
  :param state: Current state to be in. 
  :type state: bool. 
  :returns: int -- the return code. 
  :raises: AttributeError, KeyError

  """ 
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
