import logging
import datetime as dt
import os
import yaml
HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG = yaml.load(open(os.path.join(HERE,'relativepath', filename)))

def getLogger(name, dir=None, level=logging.INFO):
    """
    creates log files at the desired path with name 'name.2016-09-14.log'
    """
    logger = logging.getLogger(name)
    if dir is None:
        dir = CONFIG['LOGS'] # path mentioned in config
    if not logger.handlers:
        hdlr = logging.FileHandler('%s/%s.%s.log'%(dir,name,str(dt.date.today())))
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
    logger.setLevel(level)
    return logger

if __name__=='__main__':
    getLogger("test_error_log")
