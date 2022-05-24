"""This module has script for the performance on sftp folder"""
import pysftp
# from fixtures import mock_sftp

class SftpConnection:
    """This class that contains methods for get file from sftp and renaming after uploded to s3"""

    def __init__(self,logger,config):
        """This is the init method of the class of SftpCon"""
        self.config=config
        self.logger = logger

    def sftp_connection(self):
        """This method connects with sftp server with the given details"""
        
        try:
            conn = pysftp.Connection(
            host=self.config["SFTP"]["host"],
            username=self.config["SFTP"]["username"],
            password=self.config["SFTP"]["password"],
        )
            self.logger.info("Successfully connected to sftp server with the given credentials")
        except Exception as err:
            self.logger.info("Cannot able to connect to sftp server with the given credentials")
            conn=None
        return conn
    
    def list_files(self,sftp_path):
        """This method gets the list of files names for the given sftp path by filtering"""
        try:
            conn=self.sftp_connection()
            # conn=mock_sftp
            
            sftp_file_list = [
                file for file in conn.listdir(sftp_path)
            ]
            self.logger.info("Received the files from sftp server")
        except Exception as err:
            self.logger.error(f"Cannot able to get the files from sftp server{err}")
            print("Cannot able to get the files from sftp server",err)
            sftp_file_list=None
        return sftp_file_list

