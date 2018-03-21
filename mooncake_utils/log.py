import logging
from mooncake_utils.file import mkdirp
import os,sys
from logging.handlers import TimedRotatingFileHandler,RotatingFileHandler

logbase = os.path.dirname(os.path.abspath(sys.argv[0])) + '/log/'

def get_logger(
          debug = True, 
          name = "mu.log",
          with_file = False,
          level = None, wrapper = False):
  """get_logger

  :param debug:
  :param name:
  :param with_file:
  """

  if name == None or name == "":
    return None
  formatter = logging.Formatter('%(threadName)s | %(asctime)s - %(levelname)s - <%(filename)s-%(funcName)s:%(lineno)d> : %(message)s')
  if debug:
      _level=logging.DEBUG
  else:
      _level=logging.INFO
  
  logger = logging.getLogger(name)
  if len(logger.handlers) > 0:
    return logger
  if not level:
    logger.setLevel(_level)
  else:
    logger.setLevel(level)
  
  console_handler = logging.StreamHandler()
  console_handler.setFormatter(formatter)
  logger.addHandler(console_handler)
  
  if with_file:
    mkdirp(logbase)
    log_file = logbase + "%s.log" % name
    file_handler = TimedRotatingFileHandler(
                        log_file,
                        "midnight", 1, 30,
                        None, True)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info("add file_hander to logger {}".format(log_file))

  logger.info("init logger success [{}]".format(name))
  if not wrapper:
    return logger
  return LogWrapper(logger)

class LogWrapper():
  def __init__(self, logger):
    self.logger = logger
    self.sep = " "

  def set_sep(self, n):
    self.sep = n
  
  def log(self, level, *args):
    self.logger.log(level, self.sep.join("{}".format(a) for a in args))

  def info(self, *args):
    self.logger.info(self.sep.join("{}".format(a) for a in args))
  
  def debug(self, *args):
    self.logger.debug(self.sep.join("{}".format(a) for a in args))
  
  def warning(self, *args):
    self.logger.warning(self.sep.join("{}".format(a) for a in args))
  
  def error(self, *args):
    self.logger.error(self.sep.join("{}".format(a) for a in args))
  
  def critical(self, *args):
    self.logger.critical(self.sep.join("{}".format(a) for a in args))
  
  def exception(self, *args):
    self.logger.exception(self.sep.join("{}".format(a) for a in args))

if __name__ == "__main__":
  logger = get_logger(debug = True, name = "mooncake_utils", level=3)
  print logger
  logger.info("mooncake's a good guy!")
  #try:
  #  raise Exception("ffff")
  #except Exception: 
  #  logger.exception("23333")

  #l = logger
  #l.info("dsf","aaa","123")
  #l.set_sep("#")
  #l.info("dsf","aaa","123")
  #l.debug("dsf","aaa","123")
  #l.log(3,"2333333333dsf","aaa","123")
  #l.exception('23333')

