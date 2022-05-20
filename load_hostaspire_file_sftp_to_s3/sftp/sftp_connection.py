"""This module has script for the performance on sftp folder"""
import pysftp

# from fixtures import mock_sftp

class SftpConnection:
    """This class that contains methods for get file from sftp and renaming after uploded to s3"""

    def __init__(self,logger,config):
        """This is the init method of the class of SftpCon"""
        self.config=config
        self.conn = pysftp.Connection(
            host=config["SFTP"]["host"],
            username=config["SFTP"]["username"],
            password=config["SFTP"]["password"],
        )
        # self.conn=mock_sftp
        self.logger = logger
        self.sftp_path = config["SFTP"]["sftp_path"]

    def list_files(self):
        """This method gets the list of files names for the given sftp path by filtering"""
        try:
            sftp_file_list = [
                file for file in self.conn.listdir(self.sftp_path)
            ]
            self.logger.info("Received the files from sftp server")
        except Exception as err:
            self.logger.error(f"Cannot able to get the files from sftp server{err}")
            print("Cannot able to get the files from sftp server",err)
            sftp_file_list=None
        return sftp_file_list

