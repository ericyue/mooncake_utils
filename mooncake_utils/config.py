import yaml
from mooncake_utils.log import get_logger

logger = get_logger(name = "yaml", with_file = False, level = None)

def load_yaml(path):
  try:  
    conf = yaml.load(open(path,'r'))  
  except Exception, w:
    logger.exception(w)
    conf = None

  return conf
