"""This module has operations on creating rotatimg time logger and access config files"""
import os
import configparser
import logging
from logging.handlers import TimedRotatingFileHandler

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")

class LoggerPath:
    """This class has methods on creating a logger obj"""
    def logger_object(folder_name):
        """This method returns the logger object"""
        log_dir = os.path.join(
            parent_dir,
            config["local"]["log_path"],folder_name,
        )
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, folder_name+"logfile.log")
        handler = TimedRotatingFileHandler(log_file,when='h', interval=1, backupCount=3)
        logging.basicConfig(
            level=logging.INFO,
            datefmt="%d-%b-%y %H:%M:%S",
            format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
            handlers=[handler]
        )
        logger_obj = logging.getLogger("Rotating Log")
        details={'logger':logger_obj,'config':config,'parent_dir':parent_dir}
        return details
    
    def local_download_path(folder_name):
        """This method returns the download path for local from config"""
        local_path = os.path.join(parent_dir,config["local"]["data_path"], folder_name
        )
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        return local_path