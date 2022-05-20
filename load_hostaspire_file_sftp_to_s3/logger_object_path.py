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
    def logger_object():
        """This method returns the logger object"""
        log_dir = os.path.join(
            parent_dir,
            config["local"]["log_path"],
            "load_sftp_to_s3",
        )
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, "sftp_to_s3_logfile.log")
        logging.basicConfig(
            level=logging.INFO,
            filename=log_file,
            datefmt="%d-%b-%y %H:%M:%S",
            format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
            filemode="a",
        )
        logger_obj = logging.getLogger("Rotating Log")
        handler = TimedRotatingFileHandler(log_file,when='h', interval=1, backupCount=3)
        logger_obj.addHandler(handler)
        details={'logger':logger_obj,'config':config,'parent_dir':parent_dir}
        return details