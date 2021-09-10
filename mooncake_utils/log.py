#coding=utf-8
import logging
from mooncake_utils.file import mkdirp
import os,sys
from logging.handlers import TimedRotatingFileHandler,RotatingFileHandler
#from kafka.client import SimpleClient
#from kafka.producer import KafkaProducer
import logging

logbase = os.path.dirname(os.path.abspath(sys.argv[0])) + '/log/'

def get_logger(
          debug = False, 
          name = "mu.log",
          with_file = False,
          level = None, wrapper = False,
          formatter_str = '%(threadName)s | %(asctime)s - %(levelname)s - <%(filename)s-%(funcName)s:%(lineno)d> : %(message)s',
          log_save_path = None,
          with_console = True,
          with_kafka = False, kafka_topic = None, kafka_hosts = None, kafka_api_version = (0, 10)):

  """get_logger

  :param debug:
  :param name:
  :param with_file:
  """

  formatter = logging.Formatter(formatter_str)
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

  if with_console: 
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

  if log_save_path:
    logpath = log_save_path
  else:
    logpath = logbase
    
  if with_file:
    mkdirp(logpath)
    log_file = logpath + "/%s.log" % name
    file_handler = TimedRotatingFileHandler(
                        log_file,
                        "midnight", 1, 30,
                        None, True)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.debug("add file_hander to logger {}".format(log_file))

  if with_kafka:
    kfk = KafkaLoggingHandler(kafka_hosts, kafka_topic, kafka_api_version)
    kfk.setFormatter(formatter)
    logger.addHandler(kfk)

  logger.debug("init logger success [{}]".format(name))
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


#class KafkaLoggingHandler(logging.Handler):
#    producer = None
#    def __init__(self, hosts_list, topic, kafka_api_version):
#        logging.Handler.__init__(self)
#
#        self.kafka_topic_name = topic
#        self.producer = KafkaProducer(bootstrap_servers = hosts_list,
#                            api_version = kafka_api_version)
#
#    def emit(self, record):
#        if record.name == 'kafka':
#            return
#        try:
#            msg = self.format(record)
#            if isinstance(msg, unicode):
#                msg = msg.encode("utf-8")
#
#            self.producer.send(self.kafka_topic_name, msg)
#        except (KeyboardInterrupt, SystemExit):
#            raise
#        except:
#            self.handleError(record)
#
#    def close(self):
#        if self.producer is not None:
#            self.producer.close()
#        logging.Handler.close(self)

if __name__ == "__main__":
  logger = get_logger(name = "mooncake_utils")
  logger.debug("mooncake's a good guy!")
