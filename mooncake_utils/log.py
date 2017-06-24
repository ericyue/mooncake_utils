import logging
from mooncake_utils.file import mkdirp
import os,sys
from logging.handlers import TimedRotatingFileHandler,RotatingFileHandler
logbase = os.path.dirname(os.path.abspath(sys.argv[0])) + '/log/'

def get_logger(
          debug = True, 
          name = "mu.log",
          with_file = False):
  """get_logger

  :param debug:
  :param name:
  :param with_file:
  """

  if name == None or name == "":
    return None
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - <%(filename)s-%(funcName)s:%(lineno)d> : %(message)s')
  if debug:
      level=logging.DEBUG
  else:
      level=logging.INFO
  
  logger = logging.getLogger(name)
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
  return logger

if __name__ == "__main__":
  logger = get_logger(debug = True, name = "mooncake_utils")
  logger.info("mooncake's a good guy!")
