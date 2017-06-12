import logging
import os,sys
from logging.handlers import TimedRotatingFileHandler,RotatingFileHandler
logbase = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/log/"

DEBUG = True

def get_logger(debug = True, name = "mooncake_utils"):
  if name == None or name == "":
    return None
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - <%(filename)s-%(funcName)s:%(lineno)d> : %(message)s')
  if debug:
      level=logging.DEBUG
  else:
      level=logging.INFO
  os.system("mkdir -p %s" % logbase)
  log_file = logbase+"%s.log" % name
  logger = logging.getLogger(name)
  logger.setLevel(level)
  file_handler = TimedRotatingFileHandler(log_file,"midnight", 1, 30, None, True)
  console_handler = logging.StreamHandler()
  file_handler.setFormatter(formatter)
  console_handler.setFormatter(formatter)
  logger.addHandler(file_handler)
  logger.addHandler(console_handler)
  logger.info("init logger success {}".format(name))
  return logger


if __name__ == "__main__":
  logger = get_logger(DEBUG, "mooncake_utils logger")
	logger.info("mooncake's a good guy!")
